"""
Upload Service Module

This module provides the business logic for handling file uploads,
managing sessions, and processing files.

Features:
- Session management
- File processing
- Client integration
- Status tracking
- Error handling

Data Model:
- Upload sessions
- File metadata
- Client data
- Processing status
- Session tracking

Dependencies:
- MongoDB for storage
- datetime for timestamps
- logging for tracking
- typing for type hints

Author: Snapped Development Team
"""

from typing import Dict
import logging
from datetime import datetime
from app.shared.database import upload_collection, client_info

logger = logging.getLogger(__name__)

class UploadService:
    """
    Upload service handler.
    
    Manages the business logic for file uploads and session management.
    
    Attributes:
        collection: MongoDB collection for uploads
        client_info: MongoDB collection for client information
    """
    
    def __init__(self):
        """Initialize service with database connections."""
        self.collection = upload_collection
        self.client_info = client_info

    async def init_upload_session(self, session_data: Dict):
        """
        Initialize a new upload session.
        
        Args:
            session_data (Dict): Session initialization parameters
            
        Returns:
            dict: Session identifier and client info
            
        Raises:
            Exception: For initialization errors
            
        Notes:
            - Checks existing sessions
            - Fetches client info
            - Creates session
            - Sets metadata
        """
        try:
            session_id = f"F({session_data['date']})_{session_data['client_ID']}"
            
            # First check if session already exists
            existing_session = await self.collection.find_one({
                "client_ID": session_data["client_ID"],
                "sessions.session_id": session_id
            })
            
            if existing_session:
                return {
                    "session_id": session_id,
                    "client_ID": session_data["client_ID"]
                }

            # Fetch snap_id from ClientInfo
            client_doc = await self.client_info.find_one({"client_id": session_data["client_ID"]})
            snap_id = client_doc.get("snap_id", "") if client_doc else ""

            # Create session document
            session = {
                "session_id": session_id,
                "content_type": session_data["content_type"],
                "upload_date": datetime.utcnow(),
                "folder_id": session_data["folder_id"],
                "folder_path": session_data["folder_path"],
                "total_files_count": session_data["total_files"],
                "total_files_size": 0,
                "total_files_size_human": "0 B",
                "total_images": 0,
                "total_videos": 0,
                "editor_note": "",
                "total_session_views": 0,
                "avrg_session_view_time": 0,
                "all_video_length": 0,
                "snap_ID": snap_id,
                "timezone": session_data.get("timezone", "UTC"),
                "files": []
            }

            # Create/update client document with new session
            await self.collection.update_one(
                {
                    "client_ID": session_data["client_ID"],
                    "sessions.session_id": {"$ne": session_id}
                },
                {
                    "$setOnInsert": {
                        "client_ID": session_data["client_ID"],
                        "snap_ID": snap_id,
                        "timezone": session_data.get("timezone", "UTC")
                    },
                    "$push": {
                        "sessions": session
                    }
                },
                upsert=True
            )
            
            return {
                "session_id": session["session_id"],
                "client_ID": session_data["client_ID"]
            }
            
        except Exception as e:
            logger.error(f"Error in init_upload_session: {str(e)}")
            raise

    async def add_file_to_session(self, session_id: str, file_data: Dict):
        """
        Add a file to an existing session.
        
        Args:
            session_id (str): Session identifier
            file_data (Dict): File metadata and content
            
        Raises:
            Exception: For file processing errors
            
        Notes:
            - Updates session
            - Processes metadata
            - Updates counters
            - Handles errors
        """
        try:
            file_entry = {
                "seq_number": file_data["seq_number"],
                "file_name": file_data["file_name"],
                "date_time": datetime.utcnow(),
                "file_path": file_data["file_path"],
                "CDN_link": file_data["cdn_url"],
                "file_type": file_data["file_type"],
                "file_size": file_data["file_size"],
                "file_size_human": file_data["file_size_human"],
                "video_length": file_data.get("video_length", 0),
                "is_thumbnail": file_data.get("is_thumbnail", False),
                "caption": file_data.get("caption", "")
            }

            await self.collection.update_one(
                {"sessions.session_id": session_id},
                {
                    "$push": {"sessions.$.files": file_entry},
                    "$inc": {
                        "sessions.$.total_files_size": file_data["file_size"],
                        "sessions.$.total_images": 1 if file_data["file_type"].startswith("image") else 0,
                        "sessions.$.total_videos": 1 if file_data["file_type"].startswith("video") else 0,
                        "sessions.$.all_video_length": file_data.get("video_length", 0)
                    }
                }
            )
        except Exception as e:
            logger.error(f"Error adding file: {str(e)}")
            raise
