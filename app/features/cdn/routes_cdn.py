"""
CDN Routes - Content Delivery Network API Endpoints

This module provides FastAPI routes for managing content delivery and storage operations,
integrating with S3, MongoDB, and BunnyCDN services. It handles file operations, content
organization, and metadata management.

API Structure:
------------
1. Content Management:
   - List contents with pagination
   - File operations (move, delete, reorder)
   - Folder operations (create, move)
   - Content dump management

2. File Operations:
   - Thumbnail generation
   - Caption management
   - Sequence ordering
   - File record management

3. Storage Integration:
   - S3 content access
   - BunnyCDN operations
   - MongoDB synchronization
   - Cross-storage moves

4. Editor Notes:
   - Note creation and retrieval
   - Session annotations
   - Content documentation

Security Features:
---------------
1. Authentication:
   - Partner-based access control
   - Admin privileges
   - Client validation

2. Data Protection:
   - Path sanitization
   - Content validation
   - Access verification

3. Error Handling:
   - Storage failures
   - Database conflicts
   - Invalid content
   - Network issues

Directory Structure:
-----------------
sc/
├── {client_id}/
│   ├── STORIES/
│   │   └── F(date)_{client_id}/
│   ├── SPOTLIGHT/
│   │   └── F(date)_{client_id}/
│   ├── SAVED/
│   │   └── F(date)_{client_id}/
│   └── CONTENT_DUMP/
│       └── CONTENTDUMP_{client_id}/

Collections:
----------
- upload_collection: Regular story content
- spotlight_collection: Spotlight content
- saved_collection: Archived content
- content_dump_collection: Large archives
- edit_thumb_collection: Thumbnail metadata

Dependencies:
-----------
- FastAPI: Web framework
- Motor: Async MongoDB
- Boto3: AWS S3 SDK
- Pillow: Image processing
- Aiohttp: Async HTTP

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException, Body, Depends, Request, Response, Query
from typing import List, Dict, Any
import logging
from app.shared.database import upload_collection, spotlight_collection, async_client, client_info, saved_collection
from .s3_service import S3Service
import aiohttp
import asyncio
from app.features.uploadapp.upload_db import UploadDB
import re
import os
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
import certifi
from .thumbnail_service import ThumbnailService
from app.shared.auth import get_current_user_group, filter_by_partner
from .session_service import SessionService
import traceback
from bson import ObjectId
from fastapi.responses import StreamingResponse
from botocore.exceptions import ClientError
from pydantic import BaseModel
from PIL import Image
from io import BytesIO
import boto3

logger = logging.getLogger(__name__)

# Initialize database collections
edit_thumb_collection = async_client["UploadDB"]["EditThumb"]

router = APIRouter(prefix="/api/cdn")

# Initialize services
thumbnail_service = ThumbnailService()
s3_service = S3Service()

content_dump_router = APIRouter(prefix="/api/content-dump")
content_dump_collection = async_client["UploadDB"]["Content_Dump"]

class RemoveFileRequest(BaseModel):
    """
    Request model for removing file records from MongoDB.
    
    Attributes:
        file_path: Full path to the file in storage
        file_name: Name of the file to remove
    """
    file_path: str
    file_name: str

class AddFileRequest(BaseModel):
    """
    Request model for adding new file records to MongoDB.
    
    Attributes:
        file_path: Full path to the file in storage
        file_name: Name of the file to add
        file_type: Type of file (image/video)
        file_size: Size of file in bytes
        path: Optional custom path override
        seq_number: Display sequence number
        caption: Optional file caption
        video_length: Duration for video files
    """
    file_path: str
    file_name: str
    file_type: str = ""
    file_size: int = 0
    path: str = ""
    seq_number: int = 0
    caption: str = ""
    video_length: int = 0

@router.get("/client-info/{client_id}")
async def get_client_info(client_id: str):
    """
    Get client information from database.
    
    Path Parameters:
        client_id: Client identifier (e.g., 'ch11231999')
    
    Returns:
        Client information including:
        - Legal names
        - Client ID
        - Snap ID
        - Stage name
        
    Raises:
        400: Invalid client ID
        404: Client not found
        500: Database error
    """
    try:
        # If client_id is just "public", return a 400 error
        if client_id == "public":
            raise HTTPException(
                status_code=400,
                detail="Invalid client ID. Please provide a valid client ID."
            )
            
        # Clean up client_id - remove any public/ prefix and slashes
        clean_client_id = client_id.replace('public/', '').replace('public', '').strip('/')
        
        logger.info(f"Getting client info for: {clean_client_id}")
        client = await client_info.find_one({"client_id": clean_client_id})
        
        if not client:
            logger.error(f"Client not found: {clean_client_id}")
            raise HTTPException(status_code=404, detail="Client not found")
            
        # Convert ObjectId to string for JSON serialization
        client["_id"] = str(client["_id"])
        logger.info(f"Found client: {client.get('First_Legal_Name')} {client.get('Last_Legal_Name')}")
        
        return client
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting client info: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/list-contents")
async def list_contents(
    path: str = "", 
    page: int = 1, 
    limit: int = 50,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    List contents of a directory with pagination.
    
    Parameters:
        path: Directory path to list
        page: Page number (starts at 1)
        limit: Items per page (default 50)
        auth_data: User authentication data
    
    Returns:
        Dictionary containing:
        - List of files and folders
        - Pagination information
        - Access URLs for files
        
    Security:
        - Requires authentication
        - Partner-based access control
        - Admin bypass for restrictions
    
    Raises:
        403: Unauthorized access
        500: Storage/database error
    """
    try:
        logger.info(f"=== API endpoint called ===")
        logger.info(f"Path: {path}")
        logger.info(f"Page: {page}")
        logger.info(f"Limit: {limit}")
        logger.info(f"User groups: {auth_data}")

        # Extract client ID from path for access control
        client_id_match = re.search(r'public/([^/]+)/', f"/{path}/")
        client_id = None
        if client_id_match and path != "public":
            client_id = client_id_match.group(1)
            logger.info(f"Extracted client_id from path: {client_id}")
            
            # Skip access check for admin users
            if "ADMIN" not in auth_data["groups"]:
                # Get allowed clients for the user
                filter_query = await filter_by_partner(auth_data)
                
                # Check if user has access to this client
                if filter_query.get("client_id", {}).get("$in"):
                    has_access = client_id in filter_query["client_id"]["$in"]
                    if not has_access:
                        raise HTTPException(
                            status_code=403,
                            detail="You don't have permission to access this client's content"
                        )

        # Get contents from S3
        result = await s3_service.list_directory(path)
        
        if result['status'] == 'error':
            raise HTTPException(status_code=500, detail=result['message'])
            
        # Add presigned URLs for files
        for item in result['files']:
            if item['type'] == 'file':
                item['url'] = await s3_service.get_presigned_url(item['path'])

        # Sort contents: folders first, then files alphabetically
        result['folders'].sort(key=lambda x: x['name'].lower())
        result['files'].sort(key=lambda x: x['name'].lower())
        
        # Implement pagination
        start_idx = (page - 1) * limit
        end_idx = start_idx + limit
        
        paginated_folders = result['folders'][start_idx:end_idx]
        paginated_files = result['files'][start_idx:end_idx]
        
        total_items = len(result['folders']) + len(result['files'])
        total_pages = (total_items + limit - 1) // limit
        
        return {
            'status': 'success',
            'contents': paginated_folders + paginated_files,
            'pagination': {
                'current_page': page,
                'total_pages': total_pages,
                'total_items': total_items,
                'items_per_page': limit
            }
        }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing contents: {str(e)}")
        logger.exception("Exception details:")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/folder-operations")
async def handle_folder_operations(
    operation_data: Dict, 
    user_info: dict = Depends(get_current_user_group)
):
    """
    Handle folder operations (create, move, delete).
    
    Operations:
        - create_folder: Create new directory
        - move: Move files/folders
        - delete: Remove files/folders
    
    Parameters:
        operation_data: Dictionary containing:
            - operation: Type of operation
            - destination_path: Target path
            - source_path: Source path (for moves)
            - items: List of items to process
        user_info: User authentication data
    
    Storage Operations:
        1. S3 Storage:
            - File/folder operations
            - Path management
            - Content organization
        
        2. MongoDB:
            - Metadata updates
            - Session management
            - Thumbnail tracking
    
    Returns:
        Operation status and updated content listings
    
    Raises:
        400: Invalid operation/path
        403: Unauthorized
        500: Operation failure
    """
    try:
        # Convert STOR to STORIES in paths
        if "destination_path" in operation_data:
            operation_data["destination_path"] = operation_data["destination_path"].replace('/STOR/', '/STORIES/').replace('STOR/', 'STORIES/')
        if "source_path" in operation_data:
            operation_data["source_path"] = operation_data["source_path"].replace('/STOR/', '/STORIES/').replace('STOR/', 'STORIES/')
        if "items" in operation_data:
            operation_data["items"] = [item.replace('/STOR/', '/STORIES/').replace('STOR/', 'STORIES/') for item in operation_data["items"]]

        # Determine collection based on path
        if "/SAVED/" in operation_data.get("destination_path", ""):
            collection = saved_collection
            content_type = "SAVED"
        elif "/SPOTLIGHT/" in operation_data.get("destination_path", ""):
            collection = spotlight_collection
            content_type = "SPOTLIGHT"
        else:
            collection = upload_collection
            content_type = "STORIES"

        logger.info("=== START FOLDER OPERATION ===")
        logger.info(f"Operation type: {operation_data.get('operation')}")
        logger.info(f"Source path: {operation_data.get('source_path')}")
        logger.info(f"Destination path: {operation_data.get('destination_path')}")
        logger.info(f"Storage: S3")
        
        client_id = user_info.get('user_id')  # Get client_id from auth token
        logger.info(f"Authenticated client_id: {client_id}")
        
        # Use S3 service for all operations
        storage_service = s3_service
        
        if operation_data.get('operation') == "create_folder":
            try:
                logger.info("\n=== START CREATE FOLDER ===")
                folder_path = operation_data["destination_path"]
                
                result = await storage_service.create_directory(folder_path)
                logger.info(f"Folder creation result: {result}")
                
                # Initialize session in database using authenticated client_id
                logger.info(f"Calling init_session with client_id: {client_id}, folder_path: {folder_path}")
                session_result = await SessionService.init_session(client_id, folder_path)
                logger.info(f"Session initialization result: {session_result}")
                if not session_result:
                    logger.error("Failed to initialize session in database")

                return {
                    "status": "success" if result else "error",
                    "message": "Folder created successfully" if result else "Failed to create folder"
                }
                
            except Exception as e:
                logger.error(f"Error creating folder: {str(e)}")
                logger.exception("Full traceback:")
                raise HTTPException(status_code=500, detail=f"Failed to create folder: {str(e)}")
            
        elif operation_data.get('operation') == "move":
            print("\n📦 FOLDER OPERATION: MOVE")
            print("📂 Source:", operation_data.get("source_path"))
            print("📂 Destination:", operation_data.get("destination_path"))
            print("📂 Storage: S3")

            source_path = operation_data.get("source_path", "")
            dest_path = operation_data.get("destination_path", "")

            # Move files in S3
            result = await storage_service.move_files(
                source_path,
                dest_path,
                operation_data["items"]
            )
            
            if result.get('status') == 'success':
                # Update thumbnail paths in database first
                for item_path in operation_data["items"]:
                    if any(item_path.lower().endswith(ext) for ext in ['.mp4', '.webm', '.mov']):
                        old_path = item_path.strip('/')
                        new_path = f"{operation_data['destination_path'].strip('/')}/{os.path.basename(item_path)}"
                        
                        # Update the thumbnail path in MongoDB
                        await edit_thumb_collection.update_one(
                            {"video_path": old_path},
                            {"$set": {"video_path": new_path}}
                        )

                # Then update session records
                files_data = [{"name": os.path.basename(item)} for item in operation_data["items"]]
                db_result = await SessionService.move_files(
                    operation_data.get("source_path", ""),
                    operation_data.get("destination_path", ""),
                    files_data
                )
                
                if not db_result:
                    logger.error("Failed to update database records for moved files")
                    raise HTTPException(
                        status_code=500,
                        detail="Failed to update database records for moved files"
                    )

                # Get updated contents for both source and destination paths
                source_contents = await storage_service.list_directory(source_path)
                dest_contents = await storage_service.list_directory(dest_path)

                return {
                    "status": "success",
                    "message": "Files moved successfully",
                    "moved_items": operation_data["items"],
                    "updated_contents": {
                        "source": source_contents,
                        "destination": dest_contents
                    }
                }
            else:
                logger.error(f"Failed to move files in S3: {result.get('message', 'Unknown error')}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to move files in S3: {result.get('message', 'Unknown error')}"
                )
                
        elif operation_data.get('operation') == "delete":
            items = operation_data["items"]
            result = await storage_service.delete_files(items)
            return {
                "status": "success" if result else "error",
                "message": "Files deleted successfully" if result else "Failed to delete files"
            }
            
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported operation: {operation_data.get('operation')}"
            )
            
    except Exception as e:
        logger.error(f"Error in folder operation: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to perform folder operation: {str(e)}"
        )

async def update_database_paths(items: List[str], new_path: str):
    """Update file paths in database after moving files"""
    try:
        for item in items:
            filename = item.split("/")[-1]
            
            # Update upload_collection
            await upload_collection.update_many(
                {"sessions.files.file_name": filename},
                {
                    "$set": {
                        "sessions.$[session].files.$[file].file_path": new_path,
                        "sessions.$[session].files.$[file].full_path": f"{new_path}/{filename}"
                    }
                },
                array_filters=[
                    {"session.files": {"$exists": True}},
                    {"file.file_name": filename}
                ]
            )
            
            # Update spotlight_collection
            await spotlight_collection.update_many(
                {"sessions.files.file_name": filename},
                {
                    "$set": {
                        "sessions.$[session].files.$[file].file_path": new_path,
                        "sessions.$[session].files.$[file].full_path": f"{new_path}/{filename}"
                    }
                },
                array_filters=[
                    {"session.files": {"$exists": True}},
                    {"file.file_name": filename}
                ]
            )
            
            logger.info(f"Updated database paths for file: {filename} to new path: {new_path}")
            
    except Exception as e:
        logger.error(f"Error updating database paths: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update database paths: {str(e)}"
        ) 

@router.post("/reorder-files")
async def reorder_files(data: Dict = Body(...)):
    """Update sequence numbers for files in a session"""
    try:
        logger.info("\n=== START REORDER FILES ===")
        logger.info(f"Raw data received: {data}")
        
        path = data.get("path", "").strip('/')
        file_order = data.get("fileOrder", [])
        
        logger.info(f"Path: {path}")
        logger.info(f"File order: {file_order}")
        
        if not path or not file_order:
            raise HTTPException(status_code=400, detail="Missing path or file order")
            
        # Extract session ID from path
        session_match = re.search(r'F\([\d-]+\)_[^/]+', path)
        if not session_match:
            raise HTTPException(status_code=400, detail="Invalid path format")
            
        session_id = session_match.group(0)
        
        # Update sequence numbers in database
        for file in file_order:
            try:
                # Update sequence number for each file
                await upload_collection.update_one(
                    {
                        "sessions.session_id": session_id,
                        "sessions.files.file_name": file['name']
                    },
                    {
                        "$set": {
                            "sessions.$[session].files.$[file].seq_number": file['seq_number']
                        }
                    },
                    array_filters=[
                        {"session.session_id": session_id},
                        {"file.file_name": file['name']}
                    ]
                )
                logger.info(f"Updated sequence number for {file['name']} to {file['seq_number']}")
            except Exception as e:
                logger.error(f"Error updating sequence for file {file['name']}: {str(e)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to update sequence for file {file['name']}: {str(e)}"
                )

        return {
            "status": "success",
            "message": f"Successfully reordered {len(file_order)} files",
            "newOrder": file_order
        }
        
    except Exception as e:
        logger.error(f"Error reordering files: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reorder files: {str(e)}"
        )

@router.post("/generate-thumbnail")
async def generate_thumbnail(
    client_ID: str = Query(..., description="Client ID"),
    session_id: str = Query(..., description="Session ID"),
    file_name: str = Query(..., description="File name"),
    auth_data: dict = Depends(get_current_user_group)
) -> Dict[str, Any]:
    """Generate a thumbnail for a file using its CDN link."""
    try:
        # Validate auth matches client_ID
        if not auth_data or client_ID not in auth_data:
            logger.error(f"Auth mismatch: {auth_data} does not contain {client_ID}")
            raise HTTPException(status_code=403, detail="Not authorized for this client")

        logger.info(f"Received thumbnail generation request for {client_ID}/{session_id}/{file_name}")
        result = await cdn_mongo.generate_and_store_thumbnail(client_ID, session_id, file_name)
        
        if not result:
            logger.error("No result returned from generate_and_store_thumbnail")
            raise HTTPException(status_code=500, detail="Internal server error")
            
        if result.get("status") == "failed":
            logger.error(f"Thumbnail generation failed: {result.get('message')}")
            raise HTTPException(status_code=400, detail=result.get("message", "Unknown error"))
            
        logger.info(f"Thumbnail generation successful: {result}")
        return result
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.exception(f"Unexpected error generating thumbnail: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/test-thumb-insert")
async def test_thumb_insert(data: Dict = Body(...)):
    """Test endpoint to insert a thumbnail entry into the database"""
    try:
        logger.info("\n=== START TEST THUMB INSERT ===")
        logger.info(f"Raw data received: {data}")
        
        # Test database connection
        try:
            await async_client.admin.command('ping')
            logger.info("MongoDB connection successful")
        except Exception as db_error:
            logger.error(f"MongoDB connection error: {str(db_error)}")
            raise HTTPException(status_code=500, detail=f"Database connection error: {str(db_error)}")
        
        # Create the thumb document
        try:
            created_at = datetime.strptime(data.get("created_at"), "%Y-%m-%dT%H:%M:%SZ")
        except ValueError as e:
            logger.error(f"Error parsing datetime: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Invalid datetime format. Expected format: YYYY-MM-DDTHH:MM:SSZ")
            
        thumb_doc = {
            "video_path": data.get("video_path"),
            "thumb_path": data.get("thumb_path"),
            "created_at": created_at,
            "video_name": data.get("video_name"),
            "thumb_name": data.get("thumb_name")
        }
        logger.info(f"Created thumb document: {thumb_doc}")
        
        # Log collection info
        logger.info(f"Collection: {edit_thumb_collection}")
        logger.info(f"Collection name: {edit_thumb_collection.name}")
        logger.info(f"Database name: {edit_thumb_collection.database.name}")
        
        # Insert into EditThumb collection
        logger.info("Attempting to insert document...")
        result = await edit_thumb_collection.insert_one(thumb_doc)
        logger.info(f"Insert result: {result}")
        
        if not result.inserted_id:
            logger.error("No inserted_id in result")
            raise Exception("Failed to insert thumbnail document")
            
        logger.info(f"Added thumbnail entry to database with ID: {result.inserted_id}")
        
        return {
            "status": "success",
            "message": "Thumbnail entry added successfully",
            "thumb_id": str(result.inserted_id)
        }
        
    except Exception as e:
        logger.error(f"Error in test thumb insert: {str(e)}")
        logger.exception("Full traceback:")
        raise HTTPException(status_code=500, detail=str(e)) 

@router.get("/test")
async def test_endpoint():
    """Test endpoint to verify routing"""
    return {"status": "success", "message": "Test endpoint working"}

@router.post("/update-thumbnails")
async def update_thumbnails(data: Dict = Body(...)):
    """Update is_thumbnail flag for selected files"""
    try:
        logger.info("\n=== START UPDATE THUMBNAILS ===")
        logger.info(f"Raw data received: {data}")
        
        session_id = data.get("session_id")
        selected_files = data.get("selected_files", [])
        
        if not session_id or not selected_files:
            raise HTTPException(status_code=400, detail="Missing session ID or files")
            
        # First, reset all thumbnails for this session
        await upload_collection.update_one(
            {"sessions.session_id": session_id},
            {
                "$set": {
                    "sessions.$[].files.$[].is_thumbnail": False
                }
            }
        )
        
        # Then set the new thumbnails
        for file in selected_files:
            file_name = file.get("name") if isinstance(file, dict) else file
            await upload_collection.update_one(
                {"sessions.session_id": session_id},
                {
                    "$set": {
                        "sessions.$[].files.$[file].is_thumbnail": True
                    }
                },
                array_filters=[
                    {"file.file_name": file_name}
                ]
            )
            
        logger.info(f"Updated thumbnails for session {session_id}")
        return {
            "status": "success",
            "message": "Thumbnails updated successfully"
        }
        
    except Exception as e:
        logger.error(f"Error updating thumbnails: {str(e)}")
        logger.exception("Full traceback:")
        raise HTTPException(status_code=500, detail=str(e)) 

@router.post("/renumber-files")
async def renumber_files(data: Dict = Body(...)):
    """Renumber files in a folder with pattern 0001-MMDD"""
    try:
        logger.info("\n=== START RENUMBER FILES ===")
        logger.info(f"Raw data received: {data}")
        
        path = data.get("path", "").strip('/')
        logger.info(f"Path: {path}")
        
        if not path:
            logger.error("Missing path in request")
            raise HTTPException(status_code=400, detail="Missing path")
            
        # Extract client ID from path
        client_match = re.search(r'public/([^/]+)/', f"/{path}/")
        if not client_match:
            logger.error(f"Could not extract client ID from path: {path}")
            raise HTTPException(status_code=400, detail="Invalid path format - could not extract client ID")
        
        client_id = client_match.group(1)
        logger.info(f"Extracted client ID: {client_id}")
        
        # Check if this is a content dump folder
        is_content_dump = "CONTENT_DUMP" in path
        logger.info(f"Is content dump: {is_content_dump}")
        
        # Get current files in the folder
        try:
            result = await s3_service.list_directory(path)
            if result['status'] == 'error':
                raise Exception(result['message'])
                
            files = result['files']
            if not files:
                logger.info("No files found in directory")
                return {"status": "success", "message": "No files to rename"}
                
            # Sort files by sequence number if available
            files.sort(key=lambda x: int(x.get('name').split('-')[0]) if x.get('name', '').split('-')[0].isdigit() else 0)
            logger.info(f"Sorted files: {[f['name'] for f in files]}")
        except Exception as e:
            logger.error(f"Error listing directory: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to list directory: {str(e)}")
        
        # Get current date for MMDD format
        current_date = datetime.now().strftime("%m%d")
        
        # Process files in batches of 25
        BATCH_SIZE = 25
        renamed_count = 0
        total_files = len(files)
        logger.info(f"Starting to process {total_files} files in batches of {BATCH_SIZE}")
        
        for batch_start in range(0, total_files, BATCH_SIZE):
            try:
                batch_end = min(batch_start + BATCH_SIZE, total_files)
                batch = files[batch_start:batch_end]
                logger.info(f"Processing batch {batch_start//BATCH_SIZE + 1}, files {batch_start+1} to {batch_end}")
                
                # Process each file in the batch
                for index, file in enumerate(batch, start=batch_start + 1):
                    try:
                        old_name = file['name']
                        extension = os.path.splitext(old_name)[1]
                        new_name = f"{index:04d}-{current_date}{extension}"
                        
                        logger.info(f"Processing file: {old_name} -> {new_name}")
                        
                        # Skip if name is already correct
                        if old_name == new_name:
                            logger.info(f"Skipping {old_name} - already correctly named")
                            continue
                        
                        # Rename in S3
                        old_key = f"{path}/{old_name}"
                        new_key = f"{path}/{new_name}"
                        
                        result = await s3_service.move_file(old_key, new_key)
                        if result['status'] == 'error':
                            raise Exception(result['message'])
                            
                        renamed_count += 1
                        logger.info(f"Successfully renamed file {old_name} to {new_name}")
                        
                        if is_content_dump:
                            # Update MongoDB for content dump
                            try:
                                # First, remove the old file entry
                                await upload_collection.update_one(
                                    {
                                        "client_ID": client_id,
                                        "sessions.folder_id": "CONTENTDUMP_" + client_id
                                    },
                                    {
                                        "$pull": {
                                            "sessions.$[session].files": {
                                                "file_name": old_name
                                            }
                                        }
                                    },
                                    array_filters=[
                                        {"session.folder_id": "CONTENTDUMP_" + client_id}
                                    ]
                                )

                                # Then add the new file entry
                                await upload_collection.update_one(
                                    {
                                        "client_ID": client_id,
                                        "sessions.folder_id": "CONTENTDUMP_" + client_id
                                    },
                                    {
                                        "$push": {
                                            "sessions.$[session].files": {
                                                "file_name": new_name,
                                                "file_type": file.get("type", ""),
                                                "file_size": file.get("size", 0),
                                                "path": new_key,
                                                "seq_number": index,
                                                "upload_date": datetime.now(timezone.utc),
                                                "caption": "",
                                                "video_length": 0
                                            }
                                        }
                                    },
                                    array_filters=[
                                        {"session.folder_id": "CONTENTDUMP_" + client_id}
                                    ]
                                )

                            except Exception as mongo_error:
                                logger.error(f"MongoDB update error for {old_name}: {str(mongo_error)}")
                                continue
                            
                    except Exception as file_error:
                        logger.error(f"Error processing file {old_name}: {str(file_error)}")
                        continue
                
                # Add a small delay between batches
                if batch_end < total_files:
                    logger.info("Adding delay between batches")
                    await asyncio.sleep(2)
                    
            except Exception as batch_error:
                logger.error(f"Error processing batch: {str(batch_error)}")
                continue

        # Get updated contents after renaming
        try:
            result = await s3_service.list_directory(path)
            if result['status'] == 'error':
                raise Exception(result['message'])
                
            return {
                "status": "success",
                "message": f"Successfully renamed {renamed_count} files",
                "contents": result['files']
            }
        except Exception as final_error:
            logger.error(f"Error getting final contents: {str(final_error)}")
            raise HTTPException(status_code=500, detail=f"Error getting final contents: {str(final_error)}")
        
    except Exception as e:
        logger.error(f"Error renumbering files: {str(e)}")
        logger.exception("Full traceback:")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/update-caption")
async def update_caption(data: Dict = Body(...)):
    try:
        session_id = data.get("session_id")
        file_name = data.get("file_name")
        caption = data.get("caption", "")
        path = data.get("path", "")
        
        logger.info("Caption update request data:")
        logger.info(f"session_id: {session_id}")
        logger.info(f"file_name: {file_name}")
        logger.info(f"caption: {caption}")
        logger.info(f"path: {path}")
        
        collection = (
            spotlight_collection if "/SPOTLIGHT/" in path
            else saved_collection if "/SAVED/" in path 
            else upload_collection
        )

        # First find the document to verify our query
        doc = await collection.find_one({"sessions.session_id": session_id})
        logger.info(f"Found document: {doc is not None}")
        if doc:
            logger.info("Sessions in document:")
            for session in doc.get('sessions', []):
                logger.info(f"Session ID: {session.get('session_id')}")

        # Update the caption in the sessions array
        result = await collection.update_one(
            {"sessions.session_id": session_id},
            {"$set": {"sessions.$[].files.$[file].caption": caption}},
            array_filters=[{"file.file_name": file_name}]
        )
        
        logger.info(f"Update result: {result.modified_count} documents modified")
        return {"status": "success"}
        
    except Exception as e:
        logger.error(f"Caption update failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/move-files")
async def move_files(request: Request):
    """
    Move files between directories with metadata synchronization.
    
    Process Flow:
        1. File Movement:
            - S3 storage operations
            - Path validation
            - Content transfer
        
        2. Database Updates:
            - Session records
            - File metadata
            - Thumbnail paths
        
        3. Validation:
            - Path formats
            - Access rights
            - Content types
    
    Parameters:
        request: JSON containing:
            - source: Source path
            - destination: Target path
            - files: List of files to move
    
    Returns:
        Move operation status and results
    
    Raises:
        400: Invalid paths
        403: Unauthorized
        500: Move failure
    """
    print("\n")
    print("🚀 =====================================")
    print("🚀 MOVE FILES ENDPOINT CALLED")
    print("🚀 =====================================")
    print("\n")
    
    try:
        data = await request.json()
        source_path = data.get("source")
        dest_path = data.get("destination")
        files = data.get("files", [])
        
        print("📂 SOURCE:", source_path)
        print("📂 DESTINATION:", dest_path)
        print("📄 FILES TO MOVE:", [f['name'] for f in files])
        print("\n📄 FILE METADATA:")
        for f in files:
            print(f"  - {f['name']}:")
            print(f"    Size: {f.get('size', 'N/A')}")
            print(f"    Type: {f.get('type', 'N/A')}")
            print(f"    Last Modified: {f.get('lastModified', 'N/A')}")
            print("")

        # Move files in S3
        for file in files:
            source_key = f"{source_path}/{file['name']}"
            dest_key = f"{dest_path}/{file['name']}"
            
            result = await s3_service.move_file(source_key, dest_key)
            if result['status'] == 'error':
                raise HTTPException(status_code=500, detail=result['message'])

        # Update database records
        print("\n🔄 CALLING SESSION SERVICE move_files()...")
        result = await SessionService.move_files(source_path, dest_path, files)
        
        print(f"✅ MOVE RESULT: {result}")
        print("=====================================\n")

        return {"status": "success", "message": "Files moved successfully"}

    except Exception as e:
        print(f"\n❌ ERROR IN MOVE FILES ROUTE: {str(e)}")
        print(f"💥 Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

@content_dump_router.get("/{user_id}")
async def get_content_dump(
    user_id: str, 
    filename: str = None,  # Optional filename parameter
    groups: List[str] = Depends(get_current_user_group)
):
    try:
        print(f"\n📦 Getting Content Dump for user: {user_id}")
        print(f"👤 User groups: {groups}")
        print(f"🔍 Filename filter: {filename}")
        
        # Base query to find the document
        query = {
            "client_ID": user_id,
            "sessions.session_id": f"CONTENTDUMP_{user_id}"
        }
        
        # If filename provided, add it to query
        if filename:
            query["sessions.files.file_name"] = filename
            print(f"📄 Looking for specific file: {filename}")
        
        # Use projection to get only the matching session if filename is provided
        projection = None
        if filename:
            projection = {
                "sessions": {
                    "$elemMatch": {
                        "session_id": f"CONTENTDUMP_{user_id}",
                        "files.file_name": filename
                    }
                }
            }
        
        content_dump = await upload_collection.find_one(query, projection)
        print(f"📄 Query Result: {content_dump is not None}")

        if not content_dump:
            print(f"❌ No content dump found for user: {user_id}")
            return {
                "status": "error", 
                "message": "No content dump found"
            }

        # Convert ObjectId to string for JSON serialization
        content_dump["_id"] = str(content_dump["_id"])
        
        # If filename provided, filter the files array to only include matching file
        if filename and content_dump.get("sessions"):
            for session in content_dump["sessions"]:
                if session.get("files"):
                    session["files"] = [f for f in session["files"] if f.get("file_name") == filename]
        
        return {
            "status": "success",
            "data": content_dump
        }

    except Exception as e:
        print(f"❌ Error getting content dump: {str(e)}")
        print(f"💥 Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e)) 

@content_dump_router.post("/{user_id}")
async def create_content_dump(
    user_id: str,
    content_data: Dict,
    groups: List[str] = Depends(get_current_user_group)
):
    """Create new content dump for a user"""
    try:
        print(f"\n📦 Creating Content Dump for user: {user_id}")
        
        if "ADMIN" not in groups and user_id != groups.get('user_id'):
            raise HTTPException(status_code=403, detail="Not authorized")

        # Create content dump document
        new_content_dump = {
            "client_ID": user_id,
            "snap_ID": content_data.get("snap_ID", ""),
            "last_updated": datetime.now(timezone.utc),
            "sessions": [{
                "session_id": f"CONTENTDUMP_{user_id}",
                "content_type": "content_dump",
                "upload_date": datetime.now(timezone.utc),
                "folder_id": f"CONTENTDUMP_{user_id}",
                "folder_path": f"sc/{user_id}/CONTENT_DUMP/",
                "client_ID": user_id,
                "scan_date": datetime.now().strftime("%m-%d-%Y"),
                "total_files_count": 0,
                "total_files_size": 0,
                "total_files_size_human": "0.00 MB",
                "total_images": 0,
                "total_videos": 0,
                "editor_note": "",
                "total_session_views": 0,
                "avrg_session_view_time": 0,
                "all_video_length": 0,
                "timezone": content_data.get("timezone", "America/New_York"),
                "files": []
            }]
        }

        result = await upload_collection.insert_one(new_content_dump)
        
        print(f"✅ Created content dump with ID: {result.inserted_id}")
        return {
            "status": "success",
            "message": "Content dump created",
            "id": str(result.inserted_id)
        }

    except Exception as e:
        print(f"❌ Error creating content dump: {str(e)}")
        print(f"💥 Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

@content_dump_router.put("/{user_id}")
async def update_content_dump(
    user_id: str,
    content_data: Dict,
    groups: List[str] = Depends(get_current_user_group)
):
    """Update existing content dump for a user"""
    try:
        print(f"\n📦 Updating Content Dump for user: {user_id}")
        
        if "ADMIN" not in groups and user_id != groups.get('user_id'):
            raise HTTPException(status_code=403, detail="Not authorized")

        # Update the content dump
        update_data = {
            "$set": {
                "last_updated": datetime.now(timezone.utc),
                "snap_ID": content_data.get("snap_ID", ""),
                "sessions.$.timezone": content_data.get("timezone", "America/New_York"),
                "sessions.$.editor_note": content_data.get("editor_note", ""),
                "sessions.$.files": content_data.get("files", [])
            }
        }

        result = await upload_collection.update_one(
            {
                "client_ID": user_id,
                "sessions.session_id": f"CONTENTDUMP_{user_id}"
            },
            update_data
        )

        if result.modified_count == 0:
            print("❌ No content dump found to update")
            return {
                "status": "error",
                "message": "No content dump found"
            }

        print(f"✅ Updated content dump for user: {user_id}")
        return {
            "status": "success",
            "message": "Content dump updated"
        }

    except Exception as e:
        print(f"❌ Error updating content dump: {str(e)}")
        print(f"💥 Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e)) 

@router.post("/editor-notes/add")
async def add_editor_note(note_data: dict):
    """
    Add an editor note for a client's content.
    
    Note Structure:
        - Client association
        - Session/folder context
        - Timestamp tracking
        - Content references
    
    Parameters:
        note_data: Dictionary containing:
            - client_ID: Client identifier
            - folder_id: Session folder ID
            - note: Note content
            - file_name: Referenced file
            - cdn_url: Content URL
            - created_at: Timestamp
            - pinned: Priority flag
    
    Database Operations:
        1. Document Creation:
            - New client records
            - Session initialization
            - Note arrays
        
        2. Note Management:
            - Content organization
            - Priority handling
            - Reference tracking
    
    Returns:
        Note creation status
    
    Raises:
        400: Missing data
        500: Database error
    """
    try:
        client_id = note_data.get("client_ID")
        folder_id = note_data.get("folder_id")
        
        if not client_id or not folder_id:
            raise HTTPException(status_code=400, detail="Missing required fields")
            
        file_data = {
            "note": note_data.get("note"),
            "file_name": note_data.get("file_name"),
            "cdn_url": note_data.get("cdn_url"),
            "created_at": note_data.get("created_at"),
            "pinned": note_data.get("pinned", False)  # Add pinned field with default False
        }
        
        # Check if document exists
        existing_doc = await async_client["NotifDB"]["EditNotes"].find_one(
            {"client_ID": client_id}
        )
        
        if not existing_doc:
            await async_client["NotifDB"]["EditNotes"].insert_one({
                "client_ID": client_id,
                "sessions": {
                    folder_id: []
                }
            })
        elif not existing_doc.get('sessions', {}).get(folder_id):
            await async_client["NotifDB"]["EditNotes"].update_one(
                {"client_ID": client_id},
                {"$set": {f"sessions.{folder_id}": []}}
            )

        # Push the note to the array for this session
        await async_client["NotifDB"]["EditNotes"].update_one(
            {"client_ID": client_id},
            {
                "$push": {
                    f"sessions.{folder_id}": file_data
                }
            }
        )

        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error adding editor note: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/editor-notes/{client_id}")
async def get_editor_notes(client_id: str):
    """Get all editor notes for a client"""
    try:
        pipeline = [
            {
                "$match": {
                    "client_ID": client_id
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "sessions": 1
                }
            }
        ]
        
        notes = await async_client["NotifDB"]["EditNotes"].aggregate(pipeline).to_list(None)
        if not notes:
            return {"notes": []}
            
        # Format notes from all sessions and sort pinned notes first
        formatted_notes = []
        for doc in notes:
            for folder_id, notes_array in doc.get('sessions', {}).items():
                for note in notes_array:
                    formatted_notes.append({
                        "folder_id": folder_id,
                        "note": note.get('note'),
                        "created_at": note.get('created_at'),
                        "file_name": note.get('file_name'),
                        "cdn_url": note.get('cdn_url'),
                        "pinned": note.get('pinned', False)
                    })
        
        # Sort notes - pinned first, then by creation date
        formatted_notes.sort(key=lambda x: (-x['pinned'], x['created_at']), reverse=True)
                    
        return {"notes": formatted_notes}
        
    except Exception as e:
        logger.error(f"Error getting editor notes: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 

@router.get("/s3-content")
async def get_s3_content(key: str, response: Response):
    """
    Stream content directly from S3 bucket.
    
    Process Flow:
        1. Content Validation:
            - Key format check
            - Access verification
            - Type detection
        
        2. Content Delivery:
            - Stream initialization
            - Header management
            - Type-specific handling
        
        3. Error Handling:
            - Missing content
            - Access failures
            - Stream errors
    
    Parameters:
        key: S3 object key
        response: FastAPI response object
    
    Content Types:
        - Images: JPEG, PNG, GIF
        - Videos: MP4, WebM, MOV
        - Default: octet-stream
    
    Returns:
        Streaming response with content
    
    Raises:
        400: Invalid key
        404: Content not found
        500: S3 error
    """
    try:
        logger.info(f"Getting S3 content for key: {key}")
        
        # Validate that key is not empty and has expected format
        if not key or not key.startswith(s3_service.path_prefix):
            logger.error(f"Invalid S3 key: {key}")
            raise HTTPException(status_code=400, detail="Invalid S3 key")
        
        try:
            # Get object metadata first to determine content type
            head_response = s3_service.s3_client.head_object(
                Bucket=s3_service.bucket_name,
                Key=key
            )
            
            # Get the content type from metadata or guess from filename
            content_type = head_response.get('ContentType', 'application/octet-stream')
            if content_type == 'binary/octet-stream' or content_type == 'application/octet-stream':
                # Try to guess better content type from file extension
                file_extension = os.path.splitext(key)[1].lower()
                if file_extension in ['.jpg', '.jpeg']:
                    content_type = 'image/jpeg'
                elif file_extension == '.png':
                    content_type = 'image/png'
                elif file_extension == '.gif':
                    content_type = 'image/gif'
                elif file_extension == '.mp4':
                    content_type = 'video/mp4'
                elif file_extension == '.webm':
                    content_type = 'video/webm'
                elif file_extension == '.mov':
                    content_type = 'video/quicktime'
            
            logger.info(f"Content type for {key}: {content_type}")
            
            # Get the actual object
            get_response = s3_service.s3_client.get_object(
                Bucket=s3_service.bucket_name,
                Key=key
            )
            
            # Set content type and other headers
            response.headers['Content-Type'] = content_type
            if 'ContentLength' in get_response:
                response.headers['Content-Length'] = str(get_response['ContentLength'])
            
            # Stream the content from S3
            return StreamingResponse(
                get_response['Body'],
                media_type=content_type,
                headers=response.headers
            )
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            if error_code == 'NoSuchKey':
                logger.error(f"Object not found: {key}")
                raise HTTPException(status_code=404, detail="Object not found")
            else:
                logger.error(f"S3 error ({error_code}): {str(e)}")
                raise HTTPException(status_code=500, detail=f"S3 error: {str(e)}")
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting S3 content: {str(e)}")
        logger.exception("S3 get_content exception:")
        raise HTTPException(status_code=500, detail=f"Error getting S3 content: {str(e)}") 

@router.post("/remove-file-record", response_model=dict)
async def remove_file_record(request: RemoveFileRequest):
    """Remove a file record from MongoDB after a move operation"""
    try:
        logger.info("\n=== REMOVING FILE RECORD ===")
        
        # Convert STOR to STORIES in the file path
        file_path = request.file_path.replace('/STOR/', '/STORIES/').replace('STOR/', 'STORIES/')
        logger.info(f"Original file path: {request.file_path}")
        logger.info(f"Normalized file path: {file_path}")
        logger.info(f"File name: {request.file_name}")
        
        # Extract client ID from path
        client_match = re.search(r'/([a-z]{2}\d+)/', file_path, re.IGNORECASE)
        if not client_match:
            logger.error(f"Invalid path format - could not extract client ID from path: {file_path}")
            raise HTTPException(status_code=400, detail="Invalid path format - could not extract client ID")
        
        client_id = client_match.group(1)
        logger.info(f"Extracted client ID: {client_id}")
        
        # Determine collection based on path
        collection = None
        path_upper = file_path.upper()
        if "STORIES" in path_upper:
            collection = upload_collection
            logger.info("Using upload_collection")
        elif "SPOTLIGHT" in path_upper:
            collection = spotlight_collection
            logger.info("Using spotlight_collection")
        elif "SAVED" in path_upper:
            collection = saved_collection
            logger.info("Using saved_collection")
        else:
            logger.error(f"Invalid path - could not determine collection from path: {file_path}")
            raise HTTPException(status_code=400, detail="Invalid path - could not determine collection")
            
        logger.info(f"Using collection: {collection.name}")
        
        # Extract date from path (MMDDYY format)
        date_match = re.search(r'F\(([\d-]+)\)', file_path)
        if not date_match:
            logger.error(f"Invalid path format - could not extract date from path: {file_path}")
            raise HTTPException(status_code=400, detail="Invalid path format - could not extract date")
            
        folder_date = date_match.group(1)
        session_id = f"F({folder_date})_{client_id}"
        logger.info(f"Session ID: {session_id}")
        
        # First verify the document exists
        doc = await collection.find_one({
            "client_ID": client_id,
            "sessions.session_id": session_id
        })
        if not doc:
            logger.error(f"No document found for client_ID: {client_id}, session_id: {session_id}")
            raise HTTPException(status_code=404, detail="Document not found")
        logger.info("Found matching document")
        
        # Remove the file from the session's files array
        result = await collection.update_one(
            {
                "client_ID": client_id,
                "sessions.session_id": session_id
            },
            {
                "$pull": {
                    "sessions.$.files": {
                        "file_name": request.file_name
                    }
                }
            }
        )
        
        logger.info(f"Update result - matched_count: {result.matched_count}, modified_count: {result.modified_count}")
        
        if result.modified_count == 0:
            logger.warning(f"No file record found to remove for {request.file_name} in session {session_id}")
            return {
                "status": "warning",
                "message": "No file record found to remove"
            }
            
        logger.info(f"Successfully removed file record for {request.file_name}")
        
        # Update session totals
        await SessionService.update_session_totals(collection, client_id, session_id)
        logger.info("Updated session totals")
        
        return {
            "status": "success",
            "message": "File record removed successfully"
        }
        
    except Exception as e:
        logger.error(f"Error removing file record: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/add-file-record", response_model=dict)
async def add_file_record(request: AddFileRequest):
    """Add a file record to MongoDB after a drop operation"""
    try:
        logger.info("\n=== ADDING FILE RECORD ===")
        
        # Convert STOR to STORIES in the file path
        file_path = request.file_path.replace('/STOR/', '/STORIES/').replace('STOR/', 'STORIES/')
        logger.info(f"Original file path: {request.file_path}")
        logger.info(f"Normalized file path: {file_path}")
        logger.info(f"File name: {request.file_name}")
        
        # Extract client ID from path
        client_match = re.search(r'/([a-z]{2}\d+)/', file_path, re.IGNORECASE)
        if not client_match:
            logger.error(f"Invalid path format - could not extract client ID from path: {file_path}")
            raise HTTPException(status_code=400, detail="Invalid path format - could not extract client ID")
        
        client_id = client_match.group(1)
        logger.info(f"Extracted client ID: {client_id}")
        
        # Determine collection based on path
        collection = None
        path_upper = file_path.upper()
        if "CONTENT_DUMP" in path_upper:
            collection = upload_collection
            logger.info("Using upload_collection for content dump")
            session_id = f"CONTENTDUMP_{client_id}"
            folder_id = f"CONTENTDUMP_{client_id}"
        else:
            if "STORIES" in path_upper:
                collection = upload_collection
                logger.info("Using upload_collection")
            elif "SPOTLIGHT" in path_upper:
                collection = spotlight_collection
                logger.info("Using spotlight_collection")
            elif "SAVED" in path_upper:
                collection = saved_collection
                logger.info("Using saved_collection")
            else:
                logger.error(f"Invalid path - could not determine collection from path: {file_path}")
                raise HTTPException(status_code=400, detail="Invalid path - could not determine collection")
            
            # Extract date from path (MMDDYY format) for non-content-dump files
            date_match = re.search(r'F\(([\d-]+)\)', file_path)
            if not date_match:
                logger.error(f"Invalid path format - could not extract date from path: {file_path}")
                raise HTTPException(status_code=400, detail="Invalid path format - could not extract date")
                
            folder_date = date_match.group(1)
            session_id = f"F({folder_date})_{client_id}"
            folder_id = session_id
            
        logger.info(f"Using collection: {collection.name}")
        logger.info(f"Session ID: {session_id}")
        
        # First verify the document exists
        doc = await collection.find_one({
            "client_ID": client_id,
            "sessions.session_id": session_id
        })
        if not doc:
            logger.error(f"No document found for client_ID: {client_id}, session_id: {session_id}")
            raise HTTPException(status_code=404, detail="Document not found")
        logger.info("Found matching document")
        
        # Add the file to the session's files array
        file_data = {
            "file_name": request.file_name,
            "file_type": request.file_type,
            "file_size": request.file_size,
            "path": request.path or f"{file_path}/{request.file_name}",
            "seq_number": request.seq_number,
            "upload_date": datetime.now(timezone.utc),
            "caption": request.caption,
            "video_length": request.video_length
        }
        
        result = await collection.update_one(
            {
                "client_ID": client_id,
                "sessions.session_id": session_id
            },
            {
                "$push": {
                    "sessions.$.files": file_data
                }
            }
        )
        
        logger.info(f"Update result - matched_count: {result.matched_count}, modified_count: {result.modified_count}")
        
        if result.modified_count == 0:
            logger.warning(f"Failed to add file record for {request.file_name} in session {session_id}")
            return {
                "status": "error",
                "message": "Failed to add file record"
            }
            
        logger.info(f"Successfully added file record for {request.file_name}")
        
        # Update session totals
        await SessionService.update_session_totals(collection, client_id, session_id)
        logger.info("Updated session totals")
        
        return {
            "status": "success",
            "message": "File record added successfully"
        }
        
    except Exception as e:
        logger.error(f"Error adding file record: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/transfer-file-record")
async def transfer_file_record(data: Dict = Body(...)):
    """Transfer a file record from source session to destination session after successful file operation"""
    try:
        logger.info("\n=== TRANSFERRING FILE RECORD ===")
        
        source_path = data.get("source_path", "").replace('/STOR/', '/STORIES/').replace('STOR/', 'STORIES/')
        dest_path = data.get("destination_path", "").replace('/STOR/', '/STORIES/').replace('STOR/', 'STORIES/')
        file_name = data.get("file_name")
        
        if not source_path or not dest_path or not file_name:
            raise HTTPException(status_code=400, detail="Missing required fields")
            
        logger.info(f"Source path: {source_path}")
        logger.info(f"Destination path: {dest_path}")
        logger.info(f"File name: {file_name}")
        
        # Extract client ID and session ID from source path
        source_client_match = re.search(r'/([a-z]{2}\d+)/', source_path, re.IGNORECASE)
        source_date_match = re.search(r'F\(([\d-]+)\)', source_path)
        
        if not source_client_match:
            raise HTTPException(status_code=400, detail="Invalid source path format")
            
        source_client_id = source_client_match.group(1)
        source_session_id = f"F({source_date_match.group(1)})_{source_client_id}" if source_date_match else None
        
        # Determine source collection
        source_collection = None
        if "CONTENT_DUMP" in source_path.upper():
            source_collection = upload_collection
            source_session_id = f"CONTENTDUMP_{source_client_id}"
        elif "STORIES" in source_path.upper():
            source_collection = upload_collection
        elif "SPOTLIGHT" in source_path.upper():
            source_collection = spotlight_collection
        elif "SAVED" in source_path.upper():
            source_collection = saved_collection
            
        if not source_collection:
            raise HTTPException(status_code=400, detail="Could not determine source collection")
            
        # Get file data from source session
        source_doc = await source_collection.find_one(
            {
                "client_ID": source_client_id,
                "sessions.session_id": source_session_id,
                "sessions.files.file_name": file_name
            },
            {
                "sessions": {
                    "$elemMatch": {
                        "session_id": source_session_id,
                        "files": {
                            "$elemMatch": {
                                "file_name": file_name
                            }
                        }
                    }
                }
            }
        )
        
        if not source_doc or not source_doc.get("sessions"):
            raise HTTPException(status_code=404, detail="Source file record not found")
            
        source_file = source_doc["sessions"][0]["files"][0]
        
        # Extract client ID and session ID from destination path
        dest_client_match = re.search(r'/([a-z]{2}\d+)/', dest_path, re.IGNORECASE)
        dest_date_match = re.search(r'F\(([\d-]+)\)', dest_path)
        
        if not dest_client_match:
            raise HTTPException(status_code=400, detail="Invalid destination path format")
            
        dest_client_id = dest_client_match.group(1)
        dest_session_id = f"F({dest_date_match.group(1)})_{dest_client_id}" if dest_date_match else None
        
        # Determine destination collection
        dest_collection = None
        if "CONTENT_DUMP" in dest_path.upper():
            dest_collection = upload_collection
            dest_session_id = f"CONTENTDUMP_{dest_client_id}"
        elif "STORIES" in dest_path.upper():
            dest_collection = upload_collection
        elif "SPOTLIGHT" in dest_path.upper():
            dest_collection = spotlight_collection
        elif "SAVED" in dest_path.upper():
            dest_collection = saved_collection
            
        if not dest_collection:
            raise HTTPException(status_code=400, detail="Could not determine destination collection")
            
        # Update file data for destination
        file_data = {
            **source_file,
            "path": f"{dest_path.strip('/')}/{file_name}",
            "upload_date": datetime.now(timezone.utc)
        }
        
        # Add file to destination session
        result = await dest_collection.update_one(
            {
                "client_ID": dest_client_id,
                "sessions.session_id": dest_session_id
            },
            {
                "$push": {
                    "sessions.$.files": file_data
                }
            }
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=500, detail="Failed to add file to destination session")
            
        logger.info(f"Successfully added file record to destination session")
        
        # Update session totals for destination
        await SessionService.update_session_totals(dest_collection, dest_client_id, dest_session_id)
        logger.info("Updated destination session totals")
        
        return {
            "status": "success",
            "message": "File record transferred successfully"
        }
        
    except Exception as e:
        logger.error(f"Error transferring file record: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e)) 