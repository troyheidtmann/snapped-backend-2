"""
CDN MongoDB Service - Content Management System Integration

This module provides a comprehensive service layer for managing content delivery and storage integration
between BunnyCDN and MongoDB collections. It handles various content types including Stories, Spotlights,
Saved content, and Content Dumps.

System Architecture:
------------------
1. Storage Layer:
   - Primary CDN: BunnyCDN for content storage and delivery
   - Database: MongoDB for metadata and content organization
   - S3/CloudFront: For thumbnail storage and delivery

2. Collections Structure:
   - Uploads: Regular Snapchat story content
   - Spotlights: Spotlight-specific content
   - Saved: Archived/saved content
   - Content_Dump: Large content archives
   - Edit Notes: Content annotations and editor remarks

3. Directory Structure:
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

4. Data Models:
   - Session Document:
     * session_id: Unique identifier (e.g., F(01-29-2025)_ch11231999)
     * folder_id: Directory identifier
     * scan_date: Content discovery date
     * upload_date: CDN storage date
     * total_files: Content statistics
     * editor_notes: Content annotations

   - File Entry:
     * file_name: Original filename
     * file_type: Content type (image/video)
     * CDN_link: Content delivery URL
     * seq_number: Display order
     * caption: Content description
     * thumbnail: Preview image URL
     * video_length: Duration for videos
     * is_indexed: Search indexing status

5. Integration Points:
   - FastAPI Endpoints: RESTful API interface
   - MongoDB Async: Database operations
   - S3/CloudFront: Thumbnail management
   - Authentication: Partner-based access control
   - Client Info: User management system

6. Security Measures:
   - Partner Filtering: Content access control
   - Admin Privileges: Operation restrictions
   - Data Validation: Input sanitization
   - Error Isolation: Failure containment

7. Error Handling:
   - Storage Failures: CDN connectivity issues
   - Database Conflicts: Concurrent modifications
   - Invalid Content: Format validation
   - Missing Metadata: Default values

Dependencies:
------------
- FastAPI: Web framework and routing
- Motor: Async MongoDB driver
- Boto3: AWS S3 integration
- FFmpeg: Video processing
- PIL: Image manipulation
- Python-dotenv: Configuration management

Author: Snapped Development Team
"""

from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId
from datetime import datetime, timedelta
from app.shared.auth import get_current_user_group, filter_by_partner
from app.shared.database import (
    upload_collection,
    saved_collection,
    spotlight_collection,
    content_dump_collection,
    client_info,
    edit_notes
)
import logging
import base64
import ffmpeg
import tempfile
import aiohttp
import os
from PIL import Image, ImageDraw
from io import BytesIO
import re
import asyncio
from pydantic import BaseModel
from app.features.cdn.s3_service import S3Service
import boto3

# Add S3 bucket configuration
S3_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME', 'snapped-thumbnails')
CLOUDFRONT_DOMAIN = "d2jpg2hi585lgz.cloudfront.net"

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["cdn-mongo"]
)

class CDNMongoService:
    """
    CDN MongoDB Service - Core Content Management Service
    
    This service class provides the core functionality for managing content across different storage systems
    and collections. It handles content organization, metadata management, and integration between BunnyCDN
    and MongoDB.

    Key Responsibilities:
    -------------------
    1. Content Management:
       - File organization and categorization
       - Session management and tracking
       - Thumbnail generation and caching
       - Content type handling (Stories, Spotlights, etc.)

    2. Storage Integration:
       - BunnyCDN content storage
       - MongoDB metadata management
       - S3/CloudFront thumbnail delivery
       - Multi-collection synchronization

    3. Access Control:
       - Partner-based filtering
       - Admin privilege management
       - Client access validation
       - Content visibility rules

    4. Data Operations:
       - Content discovery and indexing
       - Metadata synchronization
       - File sequence management
       - Session organization

    Collections Structure:
    --------------------
    - Uploads: Regular story content
      * Session format: F(date)_clientid
      * Content type: STORIES
      
    - Spotlights: Spotlight content
      * Session format: F(date)_clientid
      * Content type: SPOTLIGHT
      
    - Saved: Archived content
      * Session format: F(date)_clientid
      * Content type: SAVED
      
    - Content_Dump: Large archives
      * Session format: CONTENTDUMP_clientid
      * Content type: ALL

    Error Handling:
    -------------
    - Storage failures: Automatic retry mechanisms
    - Database conflicts: Transaction management
    - Invalid content: Format validation
    - Missing metadata: Default value handling
    - Network issues: Connection recovery

    Usage Example:
    -------------
    ```python
    service = CDNMongoService()
    
    # List available content
    folders = await service.get_folder_tree(client_id="ch11231999")
    
    # Generate thumbnails
    result = await service.generate_and_store_thumbnail(
        client_id="ch11231999",
        session_id="F(01-29-2025)_ch11231999",
        file_name="video.mp4"
    )
    
    # Update file metadata
    await service.update_file_caption(
        client_id="ch11231999",
        session_id="F(01-29-2025)_ch11231999",
        file_name="image.jpg",
        caption="New caption"
    )
    ```
    """
    
    def __init__(self):
        # Keep just the raw collections without assumptions about content types
        self.collections = {
            "Uploads": upload_collection,
            "Saved": saved_collection,
            "Spotlights": spotlight_collection,
            "Content_Dump": content_dump_collection
        }
        self.edit_notes_collection = edit_notes
        self.s3_service = S3Service()

    async def get_parent_path(self, folder_path: str) -> str:
        """
        Get the parent path based on the current path level.
        Simple navigation that matches our MongoDB structure.
        """
        # Clean up path
        parts = [p for p in folder_path.strip('/').split('/') if p]
        
        # If we're at root or have no parts, stay at root
        if not parts:
            return '/'

        # If we're in any folder, go back to root
        # This matches our MongoDB structure where everything is flat under collections
        return '/'

    async def get_users(self, auth_data: dict = None) -> List[Dict[str, Any]]:
        """
        Get list of all users and their available content types.
        Args:
            auth_data: Authentication data for access control
        Returns:
            List of user objects with their client IDs and available content types
        """
        try:
            users = {}
            
            # If not admin, apply client filtering
            allowed_clients = None
            if auth_data and "ADMIN" not in auth_data["groups"]:
                filter_query = await filter_by_partner(auth_data)
                if filter_query.get("client_ID", {}).get("$in"):
                    allowed_clients = set(filter_query["client_ID"]["$in"])

            # Query each collection to find all unique users and their content types
            for collection_name, collection in self.collections.items():
                query = {}
                if allowed_clients:
                    query["client_ID"] = {"$in": list(allowed_clients)}
                
                async for doc in collection.find(query, {"client_ID": 1, "snap_ID": 1, "last_updated": 1}):
                    client_id = doc.get("client_ID")
                    if not client_id:
                        continue
                        
                    if client_id not in users:
                        users[client_id] = {
                            "client_id": client_id,
                            "snap_id": doc.get("snap_ID"),
                            "last_updated": doc.get("last_updated"),
                            "content_types": set()
                        }
                    
                    users[client_id]["content_types"].add(collection_name)

            # Convert sets to lists for JSON serialization and sort by client_id
            user_list = []
            for user in users.values():
                user["content_types"] = sorted(list(user["content_types"]))
                user_list.append(user)
            
            return sorted(user_list, key=lambda x: x["client_id"])

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def get_folder_tree(self, client_id: Optional[str] = None, auth_data: dict = None) -> List[Dict[str, Any]]:
        """
        Get the folder tree structure from MongoDB collections.
        If client_id is provided, only return folders for that client.
        """
        folders = []

        # Add root level collection folders
        for collection_name, collection in self.collections.items():
            # Base folder for each content type
            base_folder = {
                "name": collection_name,
                "type": "folder",
                "path": f"sc/{collection_name}/",
                "contents": []
            }

            # Query to find all documents (clients) in this collection
            query = {"client_ID": client_id}
            
            # Apply auth filtering if not admin
            if auth_data and "ADMIN" not in auth_data["groups"]:
                filter_query = await filter_by_partner(auth_data)
                if filter_query.get("client_ID", {}).get("$in"):
                    query["client_ID"] = {"$in": filter_query["client_ID"]["$in"]}
            
            async for doc in collection.find(query):
                client_folder = {
                    "name": doc["client_ID"],
                    "type": "folder",
                    "path": f"sc/{doc['client_ID']}/{collection_name}/",
                    "snap_id": doc.get("snap_ID"),
                    "last_updated": doc.get("last_updated"),
                    "contents": []
                }

                # Add session folders for this client
                for session in doc.get("sessions", []):
                    session_id = session.get("session_id")
                    folder_id = session.get("folder_id")
                    scan_date = session.get("scan_date")
                    
                    # Create session folder
                    session_folder = {
                        "name": session_id,
                        "type": "folder",
                        "path": session.get("folder_path", ""),
                        "folder_id": folder_id,
                        "scan_date": scan_date,
                        "upload_date": session.get("upload_date"),
                        "total_files": session.get("total_files_count", 0),
                        "total_size": session.get("total_files_size_human", "0 MB"),
                        "total_images": session.get("total_images", 0),
                        "total_videos": session.get("total_videos", 0),
                        "editor_note": session.get("editor_note", ""),
                        "total_session_views": session.get("total_session_views", 0),
                        "avrg_session_view_time": session.get("avrg_session_view_time", 0),
                        "all_video_length": session.get("all_video_length", 0),
                        "contents": []
                    }

                    # Add files to session folder
                    for file in session.get("files", []):
                        file_entry = {
                            "name": file["file_name"],
                            "type": file["file_type"],
                            "size": file.get("file_size_human", "0 MB"),
                            "CDN_link": file.get("CDN_link", ""),
                            "caption": file.get("caption", ""),
                            "seq_number": file.get("seq_number", 0),
                            "is_thumbnail": file.get("is_thumbnail", False),
                            "upload_time": file.get("upload_time", ""),
                            "video_length": file.get("video_length", 0),
                            "is_indexed": file.get("is_indexed", False)
                        }
                        session_folder["contents"].append(file_entry)

                    client_folder["contents"].append(session_folder)

                base_folder["contents"].append(client_folder)
            
            folders.append(base_folder)

        return folders

    async def get_gallery_files(self, folder_path: str, auth_data: dict = None) -> List[Dict[str, Any]]:
        """
        Get all files from a specific session folder.
        Args:
            folder_path: Path to the session folder (can handle various formats)
            auth_data: Authentication data for access control
        Returns:
            List of file objects with their metadata and CDN URLs
        """
        try:
            # Clean up path - remove any leading/trailing slashes and empty parts
            parts = [p for p in folder_path.strip('/').split('/') if p]
            
            # Extract key components
            client_id = None
            content_type = None
            session_id = None

            # Find client_id (format: xx99999999)
            for part in parts:
                if re.match(r'^[a-z]{2}\d+$', part, re.IGNORECASE):
                    client_id = part
                    break

            # Find content type
            content_types = {"STORIES", "SPOTLIGHT", "SAVED", "CONTENT_DUMP"}
            for part in parts:
                if part.upper() in content_types:
                    content_type = part.upper()
                    break

            # Find session_id (format: F(date)_clientid or CONTENTDUMP_clientid)
            for part in parts:
                if part.startswith('F(') or part.startswith('CONTENTDUMP_'):
                    session_id = part
                    break

            if not all([client_id, content_type, session_id]):
                raise HTTPException(
                    status_code=400, 
                    detail=f"Could not extract required components from path. Need client_id, content_type, and session_id."
                )

            # Map content type to collection
            collection_mapping = {
                "STORIES": "Uploads",
                "SAVED": "Saved",
                "SPOTLIGHT": "Spotlights",
                "CONTENT_DUMP": "Content_Dump",
                "ALL": "Uploads"  # Map ALL to Uploads collection
            }

            collection_name = collection_mapping.get(content_type)
            if not collection_name:
                raise HTTPException(status_code=400, detail=f"Invalid content type: {content_type}")

            collection = self.collections[collection_name]

            # Build the query
            query = {
                "client_ID": client_id,
                "sessions.session_id": session_id
            }

            # Apply auth filtering if not admin
            if auth_data and "ADMIN" not in auth_data["groups"]:
                filter_query = await filter_by_partner(auth_data)
                if filter_query.get("client_ID", {}).get("$in"):
                    query["client_ID"] = {"$in": filter_query["client_ID"]["$in"]}

            # Find the document and extract the specific session
            doc = await collection.find_one(query)
            if not doc:
                raise HTTPException(status_code=404, detail=f"Session not found for {session_id}")

            # Find the specific session in the sessions array
            session = next(
                (s for s in doc.get("sessions", []) if s.get("session_id") == session_id),
                None
            )
            if not session:
                raise HTTPException(status_code=404, detail=f"Session {session_id} not found in document")

            # Format the files with all available metadata
            gallery_files = []

            for file in session.get("files", []):
                # Create the gallery file entry
                gallery_file = {
                    "name": file["file_name"],
                    "type": file["file_type"],
                    "size": file.get("file_size_human", "0 MB"),
                    "CDN_link": file.get("CDN_link", ""),
                    "caption": file.get("caption", ""),
                    "seq_number": file.get("seq_number", 0),
                    "is_thumbnail": file.get("is_thumbnail", False),
                    "upload_time": file.get("upload_time", ""),
                    "video_length": file.get("video_length", 0),
                    "is_indexed": file.get("is_indexed", False),
                    "thumbnail": file.get("thumbnail", ""),
                    "video_summary": file.get("video_summary", ""),
                    "session_info": {
                        "folder_id": session.get("folder_id"),
                        "scan_date": session.get("scan_date"),
                        "upload_date": session.get("upload_date"),
                        "total_files": session.get("total_files_count", 0),
                        "total_size": session.get("total_files_size_human", "0 MB"),
                        "editor_note": session.get("editor_note", "")
                    }
                }
                
                gallery_files.append(gallery_file)

                # Generate thumbnail if needed
                if not file.get("thumbnail") and file.get("CDN_link"):
                    asyncio.create_task(self.generate_and_store_thumbnail(
                        client_id=client_id,
                        session_id=session_id,
                        file_name=file["file_name"]
                    ))

            return sorted(gallery_files, key=lambda x: x["seq_number"])

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error in get_gallery_files: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def _verify_file_exists(self, collection, client_id: str, session_id: str, file_name: str) -> Dict[str, Any]:
        """
        Verify if a file exists in the database and return its details.
        Args:
            collection: MongoDB collection to search in
            client_id: Client ID
            session_id: Session ID
            file_name: Name of the file to find
        Returns:
            Dict containing status and either file info or error message
        """
        try:
            # Log the search parameters
            logger.info(f"Searching for file with parameters:")
            logger.info(f"  client_ID: {client_id}")
            logger.info(f"  session_id: {session_id}")
            logger.info(f"  file_name: {file_name}")
            logger.info(f"  collection: {collection.name}")

            # First try to find the document with a more flexible query
            pipeline = [
                {
                    "$match": {
                        "client_ID": client_id
                    }
                },
                {
                    "$unwind": "$sessions"
                },
                {
                    "$match": {
                        "sessions.session_id": session_id
                    }
                },
                {
                    "$unwind": "$sessions.files"
                },
                {
                    "$match": {
                        "sessions.files.file_name": file_name
                    }
                },
                {
                    "$project": {
                        "file_info": "$sessions.files"
                    }
                }
            ]
            
            logger.info(f"Executing pipeline: {pipeline}")
            cursor = collection.aggregate(pipeline)
            result = await cursor.to_list(length=1)
            logger.info(f"Pipeline result: {result}")

            if not result:
                # Try to find the document without file matching
                doc = await collection.find_one({"client_ID": client_id})
                if doc:
                    logger.info(f"Found document for client_ID {client_id}")
                    logger.info("Available sessions:")
                    for session in doc.get("sessions", []):
                        logger.info(f"  Session ID: {session.get('session_id')}")
                        if session.get("session_id") == session_id:
                            logger.info("  Files in matching session:")
                            for file in session.get("files", []):
                                logger.info(f"    {file.get('file_name')}")
                else:
                    logger.info(f"No document found for client_ID {client_id}")
                
                logger.error(f"No document found for {client_id}/{session_id}/{file_name}")
                return {"status": "failed", "message": "File not found in database"}
            
            file_info = result[0]["file_info"]
            logger.info(f"Found file info: {file_info}")
            return {"status": "success", "file_info": file_info}
            
        except Exception as e:
            logger.error(f"Error in _verify_file_exists: {str(e)}")
            logger.exception("Full traceback:")
            return {"status": "failed", "message": f"Error verifying file: {str(e)}"}

    async def generate_and_store_thumbnail(self, client_id: str, session_id: str, file_name: str) -> Dict[str, Any]:
        try:
            logger.info(f"Starting thumbnail generation for {client_id}/{session_id}/{file_name}")
            
            # Get the collection based on session ID
            collection = await self._get_collection_for_session(session_id)
            if collection is None:
                logger.error(f"Could not determine collection for session {session_id}")
                return {"status": "failed", "message": "Invalid session ID"}
            
            logger.info(f"Using collection: {collection.name}")

            # Verify file exists and get its info
            result = await self._verify_file_exists(collection, client_id, session_id, file_name)
            if result["status"] == "failed":
                return result
            
            file_info = result["file_info"]
            if "CDN_link" not in file_info or not file_info["CDN_link"]:
                logger.error(f"No CDN link found for file: {client_id}/{session_id}/{file_name}")
                return {"status": "failed", "message": "CDN link not found"}

            cdn_url = file_info["CDN_link"]
            logger.info(f"Found CDN URL: {cdn_url}")

            # Create a temporary directory for processing
            with tempfile.TemporaryDirectory() as temp_dir:
                # Download file
                temp_input = os.path.join(temp_dir, file_name)
                async with aiohttp.ClientSession() as session:
                    async with session.get(cdn_url) as response:
                        if response.status != 200:
                            logger.error(f"Failed to download from CDN URL {cdn_url}, status: {response.status}")
                            await self._mark_thumbnail_failed(collection, client_id, session_id, file_name)
                            return {"status": "failed", "message": f"Failed to download file from CDN: {response.status}"}
                        with open(temp_input, 'wb') as f:
                            async for chunk in response.content.iter_chunked(8192):
                                f.write(chunk)

                # Determine if this is a video file
                is_video = any(file_name.lower().endswith(ext) for ext in ['.mp4', '.webm', '.mov', '.avi'])
                temp_output = os.path.join(temp_dir, 'thumbnail.jpg')

                try:
                    if is_video:
                        # Use FFmpeg for video thumbnail
                        logger.info("Generating video thumbnail with FFmpeg")
                        stream = (
                            ffmpeg
                            .input(temp_input)
                            .filter('select', 'eq(n,0)')  # Select first frame
                            # Scale to fill 480x480 maintaining aspect ratio
                            .filter('scale', w='if(gte(iw,ih),480,-1)', h='if(gte(iw,ih),-1,480)')
                            # Crop to square from center
                            .filter('crop', w='min(iw,ih)', h='min(iw,ih)', x='(iw-min(iw,ih))/2', y='(ih-min(iw,ih))/2')
                            # Scale to final size
                            .filter('scale', w=480, h=480)
                            .output(temp_output, vframes=1)
                            .overwrite_output()
                        )
                        ffmpeg.run(stream, capture_stdout=True, capture_stderr=True)
                    else:
                        # Use PIL for image thumbnail
                        logger.info("Generating image thumbnail with PIL")
                        with Image.open(temp_input) as img:
                            img = img.convert('RGB')
                            # Calculate dimensions for square crop
                            width, height = img.size
                            size = min(width, height)
                            left = (width - size) // 2
                            top = (height - size) // 2
                            # Crop to square from center
                            img = img.crop((left, top, left + size, top + size))
                            # Scale to final size
                            img = img.resize((480, 480), Image.Resampling.LANCZOS)
                            img.save(temp_output, 'JPEG', quality=85)

                    # Upload to S3
                    s3_client = boto3.client('s3')
                    thumbnail_key = f"thumbnails/{client_id}_{file_name}"
                    
                    with open(temp_output, 'rb') as f:
                        s3_client.upload_fileobj(
                            f,
                            S3_BUCKET_NAME,
                            thumbnail_key,
                            ExtraArgs={'ContentType': 'image/jpeg'}
                        )

                    # Generate the thumbnail URL with CloudFront domain
                    thumbnail_url = f"https://{CLOUDFRONT_DOMAIN}/{thumbnail_key}"

                    # Update MongoDB with thumbnail URL
                    result = await collection.update_one(
                        {
                            "client_ID": client_id,
                            "sessions.session_id": session_id,
                            "sessions.files.file_name": file_name
                        },
                        {
                            "$set": {
                                "sessions.$[session].files.$[file].thumbnail": thumbnail_url,
                                "sessions.$[session].files.$[file].thumbnail_failed": False
                            }
                        },
                        array_filters=[
                            {"session.session_id": session_id},
                            {"file.file_name": file_name}
                        ]
                    )

                    if result.modified_count == 0:
                        logger.warning(f"No documents updated for {client_id}/{session_id}/{file_name}")
                        return {"status": "failed", "message": "Failed to update document"}

                    return {
                        "status": "success",
                        "thumbnail_url": thumbnail_url
                    }

                except Exception as e:
                    logger.error(f"Error processing file: {str(e)}")
                    await self._mark_thumbnail_failed(collection, client_id, session_id, file_name)
                    return {"status": "failed", "message": str(e)}

        except Exception as e:
            logger.error(f"Error generating thumbnail: {str(e)}")
            await self._mark_thumbnail_failed(collection, client_id, session_id, file_name)
            return {"status": "failed", "message": str(e)}

    async def get_client_info(self, client_id: str) -> Dict[str, Any]:
        """
        Get client's info from ClientInfo collection.
        Args:
            client_id: The client's ID (e.g., 'kd12012004')
        Returns:
            Dictionary containing client's information
        """
        try:
            logger.info(f"Getting client info for client_id: {client_id}")
            # Query using both lowercase and uppercase client_id
            client_doc = await client_info.find_one({
                "$or": [
                    {"client_id": client_id},
                    {"client_ID": client_id}
                ]
            })
            
            logger.info(f"Found client doc: {client_doc}")
            
            if not client_doc:
                logger.warning(f"No client found with client_id/client_ID: {client_id}")
                return {
                    "First_Legal_Name": "",
                    "Last_Legal_Name": "",
                    "client_ID": client_id,
                    "snap_id": "",
                    "Stage_Name": ""
                }

            # Return exact field names as shown in MongoDB
            result = {
                "First_Legal_Name": client_doc.get("First_Legal_Name", ""),
                "Last_Legal_Name": client_doc.get("Last_Legal_Name", ""),
                "client_ID": client_doc.get("client_ID", client_doc.get("client_id", client_id)),
                "snap_id": client_doc.get("snap_id", ""),
                "Stage_Name": client_doc.get("Stage_Name", "")
            }
            logger.info(f"Returning client info: {result}")
            return result

        except Exception as e:
            logger.error(f"Error getting client info: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def _mark_thumbnail_failed(self, collection, client_id: str, session_id: str, file_name: str, retry_after: int = 300):
        """Mark a file as having failed thumbnail generation with retry timing."""
        try:
            await collection.update_one(
                {
                    "client_ID": client_id,
                    "sessions.session_id": session_id,
                    "sessions.files.file_name": file_name
                },
                {
                    "$set": {
                        "sessions.$[session].files.$[file].thumbnail_failed": True,
                        "sessions.$[session].files.$[file].thumbnail_retry_after": retry_after
                    }
                },
                array_filters=[
                    {"session.session_id": session_id},
                    {"file.file_name": file_name}
                ]
            )
            logger.info(f"Marked thumbnail as failed for {file_name} with retry after {retry_after}s")
        except Exception as e:
            logger.error(f"Failed to mark thumbnail as failed for {file_name}: {str(e)}")

    async def list_collections(self) -> List[str]:
        """Get list of all available collections"""
        return list(self.collections.keys())

    async def list_clients_in_collection(self, collection_name: str, auth_data: dict = None) -> List[Dict[str, Any]]:
        """Get all clients in a specific collection with their basic info"""
        try:
            if collection_name not in self.collections:
                raise HTTPException(status_code=404, detail=f"Collection {collection_name} not found")
            
            collection = self.collections[collection_name]
            query = {}
            
            # Apply auth filtering if not admin
            if auth_data and "ADMIN" not in auth_data["groups"]:
                filter_query = await filter_by_partner(auth_data)
                # Convert client_id to client_ID to match MongoDB field
                if filter_query.get("client_ID", {}).get("$in"):
                    query["client_ID"] = {"$in": filter_query["client_ID"]["$in"]}

            clients = []
            async for doc in collection.find(query, {"client_ID": 1, "snap_ID": 1, "last_updated": 1}):
                if doc.get("client_ID"):
                    clients.append({
                        "client_ID": doc["client_ID"],
                        "snap_id": doc.get("snap_ID"),
                        "last_updated": doc.get("last_updated")
                    })
            
            return sorted(clients, key=lambda x: x["client_ID"])
            
        except Exception as e:
            logger.error(f"Error listing clients in collection: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def list_client_sessions(self, collection_name: str, client_id: str, auth_data: dict = None) -> List[Dict[str, Any]]:
        """Get all sessions for a client in a specific collection"""
        try:
            if collection_name not in self.collections:
                raise HTTPException(status_code=404, detail=f"Collection {collection_name} not found")
            
            collection = self.collections[collection_name]
            query = {"client_ID": client_id}
            
            # Apply auth filtering if not admin
            if auth_data and "ADMIN" not in auth_data["groups"]:
                filter_query = await filter_by_partner(auth_data)
                if filter_query.get("client_ID", {}).get("$in"):
                    if client_id not in filter_query["client_ID"]["$in"]:
                        raise HTTPException(status_code=403, detail="Not authorized to access this client")

            doc = await collection.find_one(query)
            if not doc:
                return []

            sessions = []
            for session in doc.get("sessions", []):
                sessions.append({
                    "session_id": session.get("session_id"),
                    "folder_id": session.get("folder_id"),
                    "scan_date": session.get("scan_date"),
                    "upload_date": session.get("upload_date"),
                    "total_files": session.get("total_files_count", 0),
                    "total_size": session.get("total_files_size_human", "0 MB"),
                    "total_images": session.get("total_images", 0),
                    "total_videos": session.get("total_videos", 0),
                    "editor_note": session.get("editor_note", "")
                })
            
            return sorted(sessions, key=lambda x: x.get("scan_date", ""), reverse=True)
            
        except Exception as e:
            logger.error(f"Error listing client sessions: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def get_editor_notes(self, client_id: str) -> List[Dict[str, Any]]:
        """
        Get all editor notes for a client
        Args:
            client_id: The client's ID
        Returns:
            List of editor notes with their metadata
        """
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
            
            notes = await self.edit_notes_collection.aggregate(pipeline).to_list(None)
            if not notes:
                return []
                
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
            return formatted_notes

        except Exception as e:
            logger.error(f"Error getting editor notes: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def create_new_session(self, client_id: str, content_type: str) -> Dict[str, Any]:
        """
        Create a new empty session for a client in the specified content type collection.
        Args:
            client_id: Client ID (e.g., 'ch11231999')
            content_type: Content type (e.g., 'STORIES', 'SPOTLIGHT', etc.)
        Returns:
            Newly created session info
        """
        try:
            # Map content type to collection
            collection_map = {
                "STORIES": "Uploads",
                "SPOTLIGHT": "Spotlights",
                "SAVED": "Saved",
                "CONTENT_DUMP": "Content_Dump"
            }
            collection_name = collection_map.get(content_type)
            if not collection_name:
                raise HTTPException(status_code=400, detail=f"Invalid content type: {content_type}")

            collection = self.collections[collection_name]

            # Get existing sessions to find the latest date
            doc = await collection.find_one({"client_ID": client_id})
            latest_date = None
            
            if doc and doc.get("sessions"):
                # Extract dates from session IDs and find the latest
                for session in doc["sessions"]:
                    session_id = session.get("session_id", "")
                    date_match = re.search(r'F\((\d{2}-\d{2}-\d{4})\)', session_id)
                    if date_match:
                        try:
                            session_date = datetime.strptime(date_match.group(1), "%m-%d-%Y")
                            if not latest_date or session_date > latest_date:
                                latest_date = session_date
                        except ValueError:
                            continue

            # If no latest date found, use today
            if not latest_date:
                latest_date = datetime.now()

            # Set the new session date to the day after the latest
            new_session_date = latest_date + timedelta(days=1)
            session_date_str = new_session_date.strftime("%m-%d-%Y")
            
            session_id = f"F({session_date_str})_{client_id}"
            folder_id = f"F({session_date_str})_{client_id}"

            # Create new session object
            new_session = {
                "session_id": session_id,
                "content_type": content_type,
                "upload_date": datetime.now().isoformat(),
                "folder_id": folder_id,
                "folder_path": f"sc/{client_id}/{content_type}/{session_id}/",
                "client_ID": client_id,
                "scan_date": session_date_str,
                "files": [],
                "total_files_count": 0,
                "total_files_size": 0,
                "total_files_size_human": "0 MB",
                "total_images": 0,
                "total_videos": 0,
                "editor_note": ""
            }

            # Update the document with the new session
            result = await collection.update_one(
                {"client_ID": client_id},
                {
                    "$push": {"sessions": new_session},
                    "$set": {"last_updated": datetime.now().isoformat()}
                }
            )

            if result.modified_count == 0:
                # If no document was modified, create a new one
                doc = {
                    "client_ID": client_id,
                    "sessions": [new_session],
                    "last_updated": datetime.now().isoformat()
                }
                await collection.insert_one(doc)

            return {
                "status": "success",
                "session": new_session
            }

        except Exception as e:
            logger.error(f"Error creating new session: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def check_file_thumbnail(self, client_id: str, session_id: str, file_name: str) -> Dict[str, Any]:
        """
        Check if a specific file is marked as a thumbnail
        Args:
            client_id: Client ID (e.g., 'ch11231999')
            session_id: Session ID (e.g., 'F(01-29-2025)_ch11231999')
            file_name: Name of the file to check
        Returns:
            Dictionary containing thumbnail status
        """
        try:
            # Determine collection based on session_id
            collection_name = None
            if session_id.startswith('CONTENTDUMP_'):
                collection_name = "Content_Dump"
            elif session_id.startswith('F('):
                if 'SPOTLIGHT' in session_id:
                    collection_name = "Spotlights"
                elif 'SAVED' in session_id:
                    collection_name = "Saved"
                else:
                    collection_name = "Uploads"

            if not collection_name:
                raise HTTPException(status_code=400, detail="Invalid session ID format")

            collection = self.collections[collection_name]

            # Find the document and file
            doc = await collection.find_one({
                "client_ID": client_id,
                "sessions.session_id": session_id,
                "sessions.files.file_name": file_name
            })

            if not doc:
                raise HTTPException(status_code=404, detail="File not found")

            # Find the specific session and file
            session = next((s for s in doc["sessions"] if s["session_id"] == session_id), None)
            if not session:
                raise HTTPException(status_code=404, detail="Session not found")

            file = next((f for f in session["files"] if f["file_name"] == file_name), None)
            if not file:
                raise HTTPException(status_code=404, detail="File not found in session")

            return {
                "status": "success",
                "is_thumbnail": file.get("is_thumbnail", False),
                "file_info": {
                    "file_name": file["file_name"],
                    "file_type": file.get("file_type"),
                    "CDN_link": file.get("CDN_link"),
                    "seq_number": file.get("seq_number"),
                    "upload_time": file.get("upload_time")
                }
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error checking file thumbnail status: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def toggle_file_thumbnail(self, client_id: str, session_id: str, file_name: str) -> Dict[str, Any]:
        """
        Toggle the thumbnail status of a specific file
        Args:
            client_id: Client ID (e.g., 'ch11231999')
            session_id: Session ID (e.g., 'F(01-29-2025)_ch11231999')
            file_name: Name of the file to toggle
        Returns:
            Dictionary containing the new thumbnail status
        """
        try:
            # Determine collection based on session_id
            collection_name = None
            if session_id.startswith('CONTENTDUMP_'):
                collection_name = "Content_Dump"
            elif session_id.startswith('F('):
                if 'SPOTLIGHT' in session_id:
                    collection_name = "Spotlights"
                elif 'SAVED' in session_id:
                    collection_name = "Saved"
                else:
                    collection_name = "Uploads"

            if not collection_name:
                raise HTTPException(status_code=400, detail="Invalid session ID format")

            collection = self.collections[collection_name]

            # Find the document and file
            doc = await collection.find_one({
                "client_ID": client_id,
                "sessions.session_id": session_id,
                "sessions.files.file_name": file_name
            })

            if not doc:
                raise HTTPException(status_code=404, detail="File not found")

            # Find the specific session and file
            session = next((s for s in doc["sessions"] if s["session_id"] == session_id), None)
            if not session:
                raise HTTPException(status_code=404, detail="Session not found")

            file = next((f for f in session["files"] if f["file_name"] == file_name), None)
            if not file:
                raise HTTPException(status_code=404, detail="File not found in session")

            # Get current thumbnail status and toggle it
            current_status = file.get("is_thumbnail", False)
            new_status = not current_status

            # Update the file's thumbnail status
            result = await collection.update_one(
                {
                    "client_ID": client_id,
                    "sessions.session_id": session_id,
                    "sessions.files.file_name": file_name
                },
                {
                    "$set": {
                        "sessions.$[session].files.$[file].is_thumbnail": new_status
                    }
                },
                array_filters=[
                    {"session.session_id": session_id},
                    {"file.file_name": file_name}
                ]
            )

            if result.modified_count == 0:
                raise HTTPException(status_code=500, detail="Failed to update thumbnail status")

            return {
                "status": "success",
                "previous_status": current_status,
                "new_status": new_status,
                "file_info": {
                    "file_name": file["file_name"],
                    "file_type": file.get("file_type"),
                    "CDN_link": file.get("CDN_link"),
                    "seq_number": file.get("seq_number"),
                    "upload_time": file.get("upload_time")
                }
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error toggling file thumbnail status: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def move_file(self, client_id: str, source_session_id: str, target_session_id: str, file_name: str) -> Dict[str, Any]:
        """
        Move a file from one session to another, supporting cross-collection moves
        Args:
            client_id: Client ID (e.g., 'ch11231999')
            source_session_id: Source session ID (e.g., 'CONTENTDUMP_ds01051985')
            target_session_id: Target session ID (e.g., 'F(01-29-2025)_ch11231999')
            file_name: Name of the file to move
        Returns:
            Dictionary containing the status and moved file info
        """
        try:
            def get_collection_name(session_id: str, content_type: str = None) -> str:
                # If content type is explicitly provided as Content_Dump, use it
                if content_type == "Content_Dump":
                    return "Content_Dump"
                
                # If session ID starts with CONTENTDUMP_, use Content_Dump collection
                if session_id.startswith('CONTENTDUMP_'):
                    return "Content_Dump"
                
                # If session ID starts with F(), check its format
                if session_id.startswith('F('):
                    if 'SPOTLIGHT' in session_id:
                        return "Spotlights"
                    elif 'SAVED' in session_id:
                        return "Saved"
                    else:
                        return "Uploads"
                
                # Default to Uploads if no other matches
                return "Uploads"

            # Get source and target collections
            source_collection_name = get_collection_name(source_session_id)
            target_collection_name = get_collection_name(target_session_id)
            
            # Log collection resolution
            logger.info(f"Source session ID: {source_session_id} -> Collection: {source_collection_name}")
            logger.info(f"Target session ID: {target_session_id} -> Collection: {target_collection_name}")
            
            source_collection = self.collections[source_collection_name]
            target_collection = self.collections[target_collection_name]

            # Find the source document and file
            source_doc = await source_collection.find_one({
                "client_ID": client_id,
                "sessions.session_id": source_session_id
            })

            if not source_doc:
                # Try searching in Content_Dump if not found
                if source_collection_name != "Content_Dump":
                    logger.info("Source not found in primary collection, trying Content_Dump")
                    source_collection = self.collections["Content_Dump"]
                    source_doc = await source_collection.find_one({
                        "client_ID": client_id,
                        "sessions.session_id": source_session_id
                    })
                if not source_doc:
                    raise HTTPException(status_code=404, detail="Source document not found")

            # Find the specific session and file
            source_session = next((s for s in source_doc["sessions"] if s["session_id"] == source_session_id), None)
            if not source_session:
                raise HTTPException(status_code=404, detail="Source session not found")

            file_to_move = next((f for f in source_session["files"] if f["file_name"] == file_name), None)
            if not file_to_move:
                raise HTTPException(status_code=404, detail="File not found in source session")

            # Find the target document and session
            target_doc = await target_collection.find_one({
                "client_ID": client_id,
                "sessions.session_id": target_session_id
            })

            if not target_doc:
                raise HTTPException(status_code=404, detail="Target document not found")

            target_session = next((s for s in target_doc["sessions"] if s["session_id"] == target_session_id), None)
            if not target_session:
                raise HTTPException(status_code=404, detail="Target session not found")

            # Update MongoDB-specific metadata while preserving CDN-related info
            file_to_move_updated = file_to_move.copy()
            # Reset sequence number for new session
            file_to_move_updated["seq_number"] = len(target_session.get("files", [])) + 1
            
            # Update folder-related metadata and CDN link for cross-collection moves
            if source_collection != target_collection:
                file_to_move_updated["folder_path"] = target_session.get("folder_path")
                file_to_move_updated["folder_id"] = target_session.get("folder_id")
                
                # Update CDN link based on target collection
                if target_collection == "Content_Dump":
                    file_to_move_updated["CDN_link"] = f"sc/{client_id}/CONTENT_DUMP/{file_name}"
                elif target_collection == "Spotlights":
                    file_to_move_updated["CDN_link"] = f"sc/{client_id}/SPOTLIGHT/{file_name}"
                elif target_collection == "Saved":
                    file_to_move_updated["CDN_link"] = f"sc/{client_id}/SAVED/{file_name}"
                else:  # Uploads
                    file_to_move_updated["CDN_link"] = f"sc/{client_id}/STORIES/{file_name}"
                
                # Update upload time
                file_to_move_updated["upload_time"] = datetime.utcnow().isoformat()

            # Start a transaction
            async with await target_collection.database.client.start_session() as session:
                async with session.start_transaction():
                    try:
                        # Remove file from source session
                        await source_collection.update_one(
                            {
                                "client_ID": client_id,
                                "sessions.session_id": source_session_id
                            },
                            {
                                "$pull": {
                                    "sessions.$[session].files": {"file_name": file_name}
                                }
                            },
                            array_filters=[{"session.session_id": source_session_id}],
                            session=session
                        )

                        # Add file to target session with updated metadata
                        await target_collection.update_one(
                            {
                                "client_ID": client_id,
                                "sessions.session_id": target_session_id
                            },
                            {
                                "$push": {
                                    "sessions.$[session].files": file_to_move_updated
                                }
                            },
                            array_filters=[{"session.session_id": target_session_id}],
                            session=session
                        )

                        # Update file counts and sizes
                        file_size = file_to_move.get("file_size", 0)
                        is_video = file_to_move.get("file_type") == "video"
                        is_image = file_to_move.get("file_type") == "image"

                        # Update source session counts
                        await source_collection.update_one(
                            {
                                "client_ID": client_id,
                                "sessions.session_id": source_session_id
                            },
                            {
                                "$inc": {
                                    "sessions.$[session].total_files_count": -1,
                                    "sessions.$[session].total_files_size": -file_size,
                                    "sessions.$[session].total_videos": -1 if is_video else 0,
                                    "sessions.$[session].total_images": -1 if is_image else 0
                                }
                            },
                            array_filters=[{"session.session_id": source_session_id}],
                            session=session
                        )

                        # Update target session counts
                        await target_collection.update_one(
                            {
                                "client_ID": client_id,
                                "sessions.session_id": target_session_id
                            },
                            {
                                "$inc": {
                                    "sessions.$[session].total_files_count": 1,
                                    "sessions.$[session].total_files_size": file_size,
                                    "sessions.$[session].total_videos": 1 if is_video else 0,
                                    "sessions.$[session].total_images": 1 if is_image else 0
                                }
                            },
                            array_filters=[{"session.session_id": target_session_id}],
                            session=session
                        )

                    except Exception as e:
                        logger.error(f"Error during file move transaction: {str(e)}")
                        raise HTTPException(status_code=500, detail=f"Transaction failed: {str(e)}")

            return {
                "status": "success",
                "message": "File moved successfully",
                "moved_file": {
                    "file_name": file_to_move_updated["file_name"],
                    "file_type": file_to_move_updated.get("file_type"),
                    "CDN_link": file_to_move_updated.get("CDN_link"),
                    "from_session": source_session_id,
                    "to_session": target_session_id,
                    "from_collection": source_collection,
                    "to_collection": target_collection,
                    "new_seq_number": file_to_move_updated.get("seq_number")
                }
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error moving file: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def update_file_caption(self, client_id: str, session_id: str, file_name: str, caption: str) -> Dict[str, Any]:
        """
        Update the caption for a specific file
        Args:
            client_id: Client ID (e.g., 'ch11231999')
            session_id: Session ID (e.g., 'F(01-29-2025)_ch11231999')
            file_name: Name of the file to update
            caption: New caption text
        Returns:
            Dictionary containing the update status and file info
        """
        try:
            # Determine collection based on session_id
            collection_name = None
            if session_id.startswith('CONTENTDUMP_'):
                collection_name = "Content_Dump"
            elif session_id.startswith('F('):
                if 'SPOTLIGHT' in session_id:
                    collection_name = "Spotlights"
                elif 'SAVED' in session_id:
                    collection_name = "Saved"
                else:
                    collection_name = "Uploads"

            if not collection_name:
                raise HTTPException(status_code=400, detail="Invalid session ID format")

            collection = self.collections[collection_name]

            # Update the file's caption
            result = await collection.update_one(
                {
                    "client_ID": client_id,
                    "sessions.session_id": session_id,
                    "sessions.files.file_name": file_name
                },
                {
                    "$set": {
                        "sessions.$[session].files.$[file].caption": caption
                    }
                },
                array_filters=[
                    {"session.session_id": session_id},
                    {"file.file_name": file_name}
                ]
            )

            if result.modified_count == 0:
                # Check if the file exists
                doc = await collection.find_one({
                    "client_ID": client_id,
                    "sessions.session_id": session_id,
                    "sessions.files.file_name": file_name
                })
                if not doc:
                    raise HTTPException(status_code=404, detail="File not found")
                else:
                    # File exists but caption might be the same
                    return {
                        "status": "unchanged",
                        "message": "Caption was already set to this value",
                        "file_info": {
                            "file_name": file_name,
                            "caption": caption
                        }
                    }

            return {
                "status": "success",
                "message": "Caption updated successfully",
                "file_info": {
                    "file_name": file_name,
                    "caption": caption
                }
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating file caption: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def update_file_sequence_numbers(self, client_id: str, session_id: str, file_updates: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Update sequence numbers for multiple files in a session
        Args:
            client_id: Client ID (e.g., 'ch11231999')
            session_id: Session ID (e.g., 'F(01-29-2025)_ch11231999')
            file_updates: List of dictionaries containing file_name and new_seq_number
                Example: [{"file_name": "0001-0203.jpg", "seq_number": 5}, ...]
        Returns:
            Dictionary containing the update status and updated files info
        """
        try:
            # Determine collection based on session_id
            collection_name = None
            if session_id.startswith('CONTENTDUMP_'):
                collection_name = "Content_Dump"
            elif session_id.startswith('F('):
                if 'SPOTLIGHT' in session_id:
                    collection_name = "Spotlights"
                elif 'SAVED' in session_id:
                    collection_name = "Saved"
                else:
                    collection_name = "Uploads"

            if not collection_name:
                raise HTTPException(status_code=400, detail="Invalid session ID format")

            collection = self.collections[collection_name]

            # Verify the session and files exist
            doc = await collection.find_one({
                "client_ID": client_id,
                "sessions.session_id": session_id
            })

            if not doc:
                raise HTTPException(status_code=404, detail="Session not found")

            session = next((s for s in doc["sessions"] if s["session_id"] == session_id), None)
            if not session:
                raise HTTPException(status_code=404, detail="Session not found in document")

            # Get list of existing files for validation
            existing_files = {f["file_name"]: f for f in session["files"]}
            
            # Validate all files exist before making any updates
            for update in file_updates:
                if update["file_name"] not in existing_files:
                    raise HTTPException(
                        status_code=404, 
                        detail=f"File not found: {update['file_name']}"
                    )

            # Update each file's sequence number
            updated_files = []
            for update in file_updates:
                result = await collection.update_one(
                    {
                        "client_ID": client_id,
                        "sessions.session_id": session_id,
                        "sessions.files.file_name": update["file_name"]
                    },
                    {
                        "$set": {
                            "sessions.$[session].files.$[file].seq_number": update["seq_number"]
                        }
                    },
                    array_filters=[
                        {"session.session_id": session_id},
                        {"file.file_name": update["file_name"]}
                    ]
                )

                if result.modified_count > 0:
                    updated_files.append({
                        "file_name": update["file_name"],
                        "old_seq_number": existing_files[update["file_name"]].get("seq_number"),
                        "new_seq_number": update["seq_number"]
                    })

            return {
                "status": "success",
                "message": f"Updated sequence numbers for {len(updated_files)} files",
                "updated_files": updated_files
            }

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating file sequence numbers: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def _get_collection_for_session(self, session_id: str):
        """
        Determine the appropriate collection based on the session ID format and content type.
        
        Args:
            session_id (str): The session ID to check
            
        Returns:
            AsyncIOMotorCollection or None: The appropriate MongoDB collection, or None if format is invalid
        """
        try:
            logger.info(f"Getting collection for session: {session_id}")
            
            # First try to find the document in Content_Dump collection
            collection = self.collections["Content_Dump"]
            doc = await collection.find_one({
                "sessions": {
                    "$elemMatch": {
                        "session_id": session_id,
                        "content_type": "ALL"
                    }
                }
            })
            
            if doc:
                logger.info("Found session in Content_Dump collection")
                return collection
            
            # If not found in Content_Dump, check other collections based on session ID
            if session_id.startswith('F('):
                # Extract the session ID without the client ID part
                base_session_id = session_id.split('_')[0] if '_' in session_id else session_id
                logger.info(f"Base session ID: {base_session_id}")
                
                # Check for specific prefixes in the session ID
                if 'SPOTLIGHT' in session_id:
                    logger.info("Using Spotlights collection")
                    return self.collections["Spotlights"]
                elif 'SAVED' in session_id:
                    logger.info("Using Saved collection")
                    return self.collections["Saved"]
                else:
                    # Default to Uploads collection for F(date) format
                    logger.info("Using Uploads collection")
                    return self.collections["Uploads"]
                
            logger.error(f"Invalid session ID format: {session_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error determining collection for session {session_id}: {str(e)}")
            return None

def get_cdn_mongo_service() -> CDNMongoService:
    """
    Dependency to get CDNMongoService instance
    """
    return CDNMongoService()

class FileSequenceUpdate(BaseModel):
    file_name: str
    seq_number: int

class BatchSequenceUpdateRequest(BaseModel):
    client_id: str
    session_id: str
    file_updates: List[FileSequenceUpdate]

@router.post("/update-sequence-numbers")
async def update_sequence_numbers(
    request: BatchSequenceUpdateRequest,
    cdn_mongo_service: CDNMongoService = Depends(get_cdn_mongo_service),
    auth_data: dict = Depends(get_current_user_group)
) -> Dict[str, Any]:
    """
    Update sequence numbers for multiple files in a batch
    """
    # Check if user has access to this client
    if auth_data and "ADMIN" not in auth_data["groups"]:
        filter_query = await filter_by_partner(auth_data)
        if filter_query.get("client_ID", {}).get("$in"):
            allowed_clients = filter_query["client_ID"]["$in"]
            if request.client_id not in allowed_clients:
                raise HTTPException(status_code=403, detail="Not authorized to access this client")

    # Convert Pydantic models to dictionaries for the service method
    file_updates = [
        {"file_name": update.file_name, "seq_number": update.seq_number}
        for update in request.file_updates
    ]
    
    return await cdn_mongo_service.update_file_sequence_numbers(
        request.client_id,
        request.session_id,
        file_updates
    )

@router.get("/get-users")
async def get_users(
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Get list of all users and their available content types.
    Returns:
        List of users with their client IDs and available content types
    Example response:
        {
            "status": "success",
            "users": [
                {
                    "client_ID": "hl01192006",
                    "snap_id": "466428",
                    "last_updated": "2025-04-01T06:33:31.860000",
                    "content_types": ["STORIES", "SPOTLIGHT"]
                },
                ...
            ]
        }
    """
    try:
        cdn_service = CDNMongoService()
        users = await cdn_service.get_users(auth_data)
        return {
            "status": "success",
            "users": users
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/list-folders")
async def list_folders(
    client_id: Optional[str] = None,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    List all folders in the CDN structure.
    Optionally filter by client_id.
    Requires authentication.
    """
    try:
        cdn_service = CDNMongoService()
        folders = await cdn_service.get_folder_tree(client_id, auth_data)
        return {
            "status": "success",
            "folders": folders
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/file-gallery")
async def file_gallery(
    folder_path: str,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Get all files from a specific session folder for gallery display.
    Args:
        folder_path: Path to the session folder (e.g., 'sc/hl01192006/STORIES/F(04-01-2025)_hl01192006/')
    Returns:
        List of files with their metadata and CDN URLs, sorted by sequence number
    """
    try:
        cdn_service = CDNMongoService()
        files = await cdn_service.get_gallery_files(folder_path, auth_data)
        return {
            "status": "success",
            "total_files": len(files),
            "files": files
        }
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/get-client-info/{client_ID}")
async def get_client_info(
    client_ID: str,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Get client's legal name by client ID.
    Args:
        client_ID: The client's ID (e.g., 'hl01192006')
    Returns:
        Client's legal name
    Example response:
        {
            "status": "success",
            "client_info": {
                "First_Legal_Name": "Kyla",
                "Last_Legal_Name": "Dodds",
                "client_ID": "kd12012004"
            }
        }
    """
    try:
        # Check if user has access to this client
        if auth_data and "ADMIN" not in auth_data["groups"]:
            filter_query = await filter_by_partner(auth_data)
            if filter_query.get("client_ID", {}).get("$in"):
                allowed_clients = filter_query["client_ID"]["$in"]
                if client_ID not in allowed_clients:
                    raise HTTPException(status_code=403, detail="Not authorized to access this client")

        cdn_service = CDNMongoService()
        client_info = await cdn_service.get_client_info(client_ID)
        return {
            "status": "success",
            "client_info": client_info
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/generate-thumbnail")
async def generate_thumbnail(
    client_ID: str,
    session_id: str,
    file_name: str,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Generate a thumbnail for a file and store it in the file object within the session
    Args:
        client_ID: Client ID
        session_id: Session ID
        file_name: Name of the file to generate thumbnail for
    Returns:
        Status of the operation
    """
    try:
        cdn_service = CDNMongoService()
        result = await cdn_service.generate_and_store_thumbnail(client_ID, session_id, file_name)
        return result
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/collections")
async def list_collections(
    cdn_mongo_service: CDNMongoService = Depends(get_cdn_mongo_service),
    auth_data: dict = Depends(get_current_user_group)
) -> Dict[str, Any]:
    """
    Get list of all available collections
    Returns:
        Dictionary containing list of collection names
    Example response:
        {
            "status": "success",
            "collections": ["Uploads", "Saved", "Spotlights", "Content_Dump"]
        }
    """
    try:
        collections = await cdn_mongo_service.list_collections()
        return {
            "status": "success",
            "collections": collections
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/collections/{collection_name}")
async def get_collection_contents(
    collection_name: str,
    path: Optional[str] = None,
    auth_data: dict = Depends(get_current_user_group)
):
    """Look at what's in a collection"""
    try:
        service = CDNMongoService()
        collection = service.collections[collection_name]
        
        # Handle auth
        query = {}
        if auth_data and "ADMIN" not in auth_data["groups"]:
            filter_query = await filter_by_partner(auth_data)
            if filter_query.get("client_ID", {}).get("$in"):
                query["client_ID"] = {"$in": filter_query["client_ID"]["$in"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))