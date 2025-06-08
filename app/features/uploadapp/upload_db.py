"""
Upload Database Management Module

This module provides database operations for the upload functionality,
handling session management and file tracking.

Features:
- Session management
- File tracking
- Client info integration
- Metadata storage
- Status tracking

Data Model:
- Upload sessions
- File metadata
- Client information
- Storage paths
- Status flags

Dependencies:
- MongoDB for storage
- datetime for timestamps
- logging for tracking
- typing for type hints

Author: Snapped Development Team
"""

from datetime import datetime
from typing import Dict, List
import logging
from app.shared.database import upload_collection, client_info, spotlight_collection

logger = logging.getLogger(__name__)

class UploadDB:
    """
    Upload database operations handler.
    
    Manages database operations for file uploads including session
    management and file tracking.
    
    Attributes:
        collection: MongoDB collection for uploads
        client_info: MongoDB collection for client information
    """
    
    def __init__(self):
        """Initialize database connections."""
        self.collection = upload_collection
        self.client_info = client_info

    async def get_client_snap_id(self, client_ID: str) -> str:
        """
        Fetch client's Snapchat ID from database.
        
        Args:
            client_ID (str): Client identifier
            
        Returns:
            str: Client's Snapchat ID or empty string
            
        Notes:
            - Queries client info collection
            - Returns empty if not found
            - No validation performed
        """
        client_doc = await self.client_info.find_one({"client_id": client_ID})
        return client_doc.get("snap_id", "") if client_doc else ""

    async def init_session(self, session_data: Dict):
        """
        Initialize upload session in database.
        
        Args:
            session_data (Dict): Session initialization data
            
        Returns:
            Database operation result or None for spotlight
            
        Raises:
            Exception: For database errors
            
        Notes:
            - Validates client
            - Checks for spotlight
            - Creates session
            - Updates metadata
        """
        try:
            logger.info("\n=== INIT SESSION START ===")
            logger.info(f"Session data to save: {session_data}")
            
            # Get snap_id from ClientInfo
            client_doc = await self.client_info.find_one({"client_id": session_data["client_ID"]})
            logger.info(f"Found client doc: {client_doc}")
            
            snap_id = client_doc.get("snap_id", "") if client_doc else ""
            logger.info(f"Using snap_id: {snap_id}")
            
            # Parse folder path to determine if it's a spotlight
            folder_parts = session_data["folder_path"].split("/")
            logger.info(f"Folder parts: {folder_parts}")
            
            if "SPOTLIGHT" in [part.upper() for part in folder_parts]:
                logger.info("Skipping spotlight folder")
                return None
            
            # Add the session
            result = await self.collection.update_one(
                {"client_ID": session_data["client_ID"]},
                {
                    "$push": {
                        "sessions": {
                            **session_data,
                            "snap_ID": snap_id
                        }
                    }
                },
                upsert=True
            )
            
            logger.info(f"Database update result: {result.raw_result}")
            return result
            
        except Exception as e:
            logger.error(f"Error in init_session: {str(e)}")
            logger.exception("Full traceback:")
            raise

    async def add_file(self, session_id: str, file_data: Dict):
        """
        Add file entry to session in database.
        
        Args:
            session_id (str): Session identifier
            file_data (Dict): File metadata
            
        Notes:
            - Updates session
            - Tracks file stats
            - Updates counters
            - Handles metadata
        """
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
                "$push": {
                    "sessions.$.files": file_entry
                },
                "$inc": {
                    "sessions.$.total_files_size": file_data["file_size"],
                    "sessions.$.total_images": 1 if file_data["file_type"].startswith("image") else 0,
                    "sessions.$.total_videos": 1 if file_data["file_type"].startswith("video") else 0,
                    "sessions.$.all_video_length": file_data.get("video_length", 0)
                }
            }
        )
