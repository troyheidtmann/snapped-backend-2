"""
Upload Application Routes Module

This module handles file upload functionality including session management,
file processing, and content organization for various types of uploads.

Features:
- Session initialization
- File upload handling
- Content management
- Spotlight integration
- Content dump processing
- Notes management

Data Model:
- Upload sessions
- File metadata
- Content organization
- Client tracking
- Notes structure

Security:
- File validation
- Access control
- Error handling
- Data sanitization

Dependencies:
- FastAPI for routing
- MongoDB for storage
- logging for tracking
- Pydantic for validation

Author: Snapped Development Team
"""

from fastapi import HTTPException
from . import router
from typing import Dict, Optional
import logging
from pydantic import BaseModel
from datetime import datetime
from app.shared.database import upload_collection, client_info, spotlight_collection, client, notes_collection
from .upload_service import UploadService
from urllib.parse import unquote

logger = logging.getLogger(__name__)
upload_service = UploadService()

# Add Content_Dump collection reference
content_dump_collection = client['UploadDB']['Content_Dump']

class SessionData(BaseModel):
    """
    Session initialization data model.
    
    Attributes:
        client_ID (str): Client identifier
        snap_ID (Optional[str]): Snapchat ID, defaults to empty
        timezone (Optional[str]): Client timezone, defaults to UTC
        date (str): Session date
        content_type (str): Type of content being uploaded
        folder_id (str): Target folder identifier
        folder_path (str): Path to storage folder
        total_files (int): Total number of files in session
    """
    client_ID: str
    snap_ID: Optional[str] = ""
    timezone: Optional[str] = "UTC"
    date: str
    content_type: str
    folder_id: str
    folder_path: str
    total_files: int

logger.info("Registering upload routes...")
@router.post("/init-session")
async def init_session(session_data: SessionData):
    """
    Initialize a new upload session.
    
    Args:
        session_data (SessionData): Session initialization parameters
        
    Returns:
        dict: Session status and identifiers
        
    Raises:
        HTTPException: For initialization errors
        
    Notes:
        - Creates new session
        - Validates client
        - Sets up tracking
    """
    logger.info(f"Route hit: POST /init-session")
    logger.info(f"Request data: {session_data}")
    try:
        result = await upload_service.init_upload_session(session_data.dict())
        logger.info("Upload session initialized successfully")
        return {
            "status": "success",
            "session_id": result["session_id"] if result else None,
            "client_ID": session_data.client_ID
        }
    except Exception as e:
        logger.error(f"Error in init_session: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/add-file/{session_id}")
async def add_file(session_id: str, file_data: Dict):
    """Add file to session"""
    try:
        await upload_service.add_file_to_session(session_id, file_data)
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error adding file: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/content-files/{client_id}/{session_folder}")
async def get_session_files(client_id: str, session_folder: str):
    """Get all files for a specific session"""
    try:
        pipeline = [
            {
                "$match": {
                    "client_ID": client_id,
                    "sessions.session_id": session_folder
                }
            },
            {
                "$unwind": "$sessions"
            },
            {
                "$match": {
                    "sessions.session_id": session_folder
                }
            },
            {
                "$unwind": "$sessions.files"
            },
            {
                "$project": {
                    "_id": 0,
                    "session_id": "$sessions.session_id",
                    "folder_path": "$sessions.folder_path",
                    "folder_id": "$sessions.folder_id",
                    "upload_date": "$sessions.upload_date",
                    "file_name": "$sessions.files.file_name",
                    "file_type": "$sessions.files.file_type",
                    "CDN_link": "$sessions.files.CDN_link",
                    "file_size": "$sessions.files.file_size_human",
                    "video_length": "$sessions.files.video_length",
                    "caption": "$sessions.files.caption"
                }
            }
        ]

        results = await upload_collection.aggregate(pipeline).to_list(None)
        
        if not results:
            return {"status": "success", "data": [], "message": "No files found"}

        return {
            "status": "success",
            "data": results,
            "message": f"Found {len(results)} files"
        }

    except Exception as e:
        logger.error(f"Error getting session files: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/api/uploadapp/content-files/get/{client_id}/{session_folder}/{file_name?}")
async def get_content_files(client_id: str, session_folder: str, file_name: str = None):
    """Get file content from a session"""
    try:
        pipeline = [
            {
                "$match": {
                    "client_ID": client_id,
                    "sessions.session_id": session_folder
                }
            },
            {
                "$unwind": "$sessions"
            },
            {
                "$match": {
                    "sessions.session_id": session_folder
                }
            }
        ]

        # If file_name is provided, get specific file, otherwise get all files
        if file_name:
            pipeline.extend([
                {"$unwind": "$sessions.files"},
                {
                    "$match": {
                        "sessions.files.file_name": file_name
                    }
                },
                {
                    "$project": {
                        "CDN_link": "$sessions.files.CDN_link"
                    }
                }
            ])
        else:
            pipeline.append({
                "$project": {
                    "files": "$sessions.files"
                }
            })

        result = await upload_collection.aggregate(pipeline).to_list(None)
        
        if not result:
            return {"status": "error", "message": "Content not found"}

        if file_name:
            return {
                "status": "success",
                "CDN_link": result[0]['CDN_link'],
                "message": "File found"
            }
        else:
            return {
                "status": "success",
                "data": result[0]['files'],
                "message": f"Found {len(result[0]['files'])} files"
            }

    except Exception as e:
        logger.error(f"Error getting content: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/spotlight/session/{session_id}")
async def add_spotlight_session(session_id: str, data: Dict):
    """Handle spotlight session data"""
    try:
        logger.info(f"Received spotlight session request - session_id: {session_id}")
        logger.info(f"Request data: {data}")
        
        client_ID = data["client_ID"]
        files = data["files"]
        
        logger.info(f"Processing spotlight data for client: {client_ID} with {len(files)} files")
        
        # Get snap_id from client_info collection
        client_doc = await client_info.find_one({"client_id": client_ID})
        snap_id = client_doc.get("snap_id", "") if client_doc else ""
        
        # Format files with CDN URLs and captions
        formatted_files = [{
            **file,
            "caption": file.get("caption", ""),
            "cdn_url": f"https://c.snapped.cc/public/{client_ID}/SPOT/{file['file_name']}"
        } for file in files]

        # Check if session exists for this date
        existing_doc = await spotlight_collection.find_one({
            "client_ID": client_ID,
            "sessions.session_id": session_id
        })

        if existing_doc:
            # Update existing session
            session = next((s for s in existing_doc["sessions"] if s["session_id"] == session_id), None)
            if session:
                # Update session totals
                new_total_files = session["total_files_count"] + data["total_files_count"]
                new_total_size = session["total_files_size"] + data["total_files_size"]
                new_total_images = session["total_images"] + data["total_images"]
                new_total_videos = session["total_videos"] + data["total_videos"]
                new_video_length = session["all_video_length"] + data["all_video_length"]

                # Update session with new totals and append files
                result = await spotlight_collection.update_one(
                    {
                        "client_ID": client_ID,
                        "sessions.session_id": session_id
                    },
                    {
                        "$set": {
                            "sessions.$.total_files_count": new_total_files,
                            "sessions.$.total_files_size": new_total_size,
                            "sessions.$.total_files_size_human": f"{new_total_size / 1024:.1f} KB",
                            "sessions.$.total_images": new_total_images,
                            "sessions.$.total_videos": new_total_videos,
                            "sessions.$.all_video_length": new_video_length,
                            "last_updated": datetime.utcnow()
                        },
                        "$push": {
                            "sessions.$.files": {
                                "$each": formatted_files
                            }
                        }
                    }
                )
            else:
                raise HTTPException(status_code=500, detail="Session found in document but not accessible")
        else:
            # Create new session document
            session_doc = {
                "session_id": session_id,
                "content_type": data["content_type"],
                "upload_date": datetime.utcnow(),
                "folder_id": data["folder_id"],
                "folder_path": data["folder_path"],
                "total_files_count": data["total_files_count"],
                "total_files_size": data["total_files_size"],
                "total_files_size_human": data["total_files_size_human"],
                "total_images": data["total_images"],
                "total_videos": data["total_videos"],
                "editor_note": data["editor_note"],
                "total_session_views": data["total_session_views"],
                "avrg_session_view_time": data["avrg_session_view_time"],
                "all_video_length": data["all_video_length"],
                "timezone": data["timezone"],
                "files": formatted_files
            }
            
            # Insert new document or add session to existing client document
            result = await spotlight_collection.update_one(
                {"client_ID": client_ID},
                {
                    "$set": {
                        "client_ID": client_ID,
                        "snap_ID": data["snap_ID"] or snap_id,
                        "last_updated": datetime.utcnow()
                    },
                    "$push": {
                        "sessions": session_doc
                    }
                },
                upsert=True
            )
        
        logger.info(f"Update result - matched: {result.matched_count}, modified: {result.modified_count}, upserted_id: {result.upserted_id}")
            
        return {
            "status": "success",
            "message": f"Added {len(files)} files to spotlight session"
        }
        
    except Exception as e:
        logger.error(f"Error adding spotlight session: {str(e)}")
        logger.exception("Full error:")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/content_dump/session/{session_id}")
async def add_content_dump_session(session_id: str, data: Dict):
    """Handle content dump session data"""
    try:
        logger.info(f"Received content dump session request - session_id: {session_id}")
        logger.info(f"Request data: {data}")
        
        client_ID = data["client_ID"]
        files = data["files"]
        
        logger.info(f"Processing content dump data for client: {client_ID} with {len(files)} files")
        
        # Get snap_id from client_info collection
        client_doc = await client_info.find_one({"client_id": client_ID})
        snap_id = client_doc.get("snap_id", "") if client_doc else ""
        
        # Create session document
        session_doc = {
            
            "session_id": session_id,
            "content_type": data["content_type"],
            "upload_date": datetime.utcnow(),
            "folder_id": data["folder_id"],
            "folder_path": data["folder_path"],
            "total_files_count": data["total_files_count"],
            "total_files_size": data["total_files_size"],
            "total_files_size_human": data["total_files_size_human"],
            "total_images": data["total_images"],
            "total_videos": data["total_videos"],
            "editor_note": data["editor_note"],
            "total_session_views": data["total_session_views"],
            "avrg_session_view_time": data["avrg_session_view_time"],
            "all_video_length": data["all_video_length"],
            "timezone": data["timezone"],
            "files": data["files"]
        }
        
        # Update or insert document
        await content_dump_collection.update_one(
            {"client_ID": client_ID},  # Find by client_ID
            {
                "$set": {
                    "client_ID": client_ID,
                    "snap_ID": data["snap_ID"] or snap_id,
                    "last_updated": datetime.utcnow()
                },
                "$push": {
                    "sessions": session_doc
                }
            },
            upsert=True
        )
        
        logger.info(f"Successfully processed content dump session for client: {client_ID}")
            
        return {
            "status": "success",
            "message": f"Added content dump session with {len(files)} files"
        }
        
    except Exception as e:
        logger.error(f"Error adding content dump session: {str(e)}")
        logger.exception("Full error:")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/api/uploadapp/routes")
async def list_routes():
    """Debug endpoint to list all registered routes"""
    routes = []
    for route in router.routes:
        routes.append({
            "path": route.path,
            "methods": route.methods,
            "name": route.name
        })
    return {"routes": routes}
    
@router.post("/uploadapp/content-notes/add")
async def add_content_note(note_data: dict):
    try:
        client_id = note_data.get('client_ID')
        session_folder = note_data.get('session_folder')
        
        if not all([client_id, session_folder]):
            raise HTTPException(status_code=400, detail="Missing required fields")

        # Create note object
        note_object = {
            "file_name": note_data['file_data'].get('file_name'),
            "note": note_data['file_data'].get('note'),
            "created_at": note_data['file_data'].get('created_at'),
            "cdn_url": note_data['file_data'].get('cdn_url')
        }

        # Update or create document
        result = await notes_collection.update_one(
            {"client_ID": client_id},
            {
                "$push": {
                    f"sessions.{session_folder}": note_object
                }
            },
            upsert=True
        )

        return {
            "status": "success",
            "message": "Note saved successfully",
            "note": note_object
        }

    except Exception as e:
        logger.error(f"Error adding note: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/video-summary/{client_id}/{session_id}/{file_name}")
async def get_video_summary(client_id: str, session_id: str, file_name: str):
    """Get video summary for a specific file"""
    try:
        # URL decode the parameters
        session_id = unquote(session_id)
        file_name = unquote(file_name)

        # Simple find_one query
        doc = await upload_collection.find_one(
            {"client_ID": client_id},
            {"sessions": {"$elemMatch": {"session_id": session_id}}}
        )

        if not doc or not doc.get("sessions"):
            return {"status": "success", "video_summary": None}

        session = doc["sessions"][0]
        file_data = next(
            (f for f in session["files"] if f["file_name"] == file_name),
            None
        )

        return {
            "status": "success",
            "video_summary": file_data.get("video_summary") if file_data else None
        }

    except Exception as e:
        logger.error(f"Error getting video summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
    