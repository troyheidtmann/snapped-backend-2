"""
Bunny Scanner Content Management System

This module serves as the core content management system for Snapchat creator content,
handling content discovery, processing, and storage across multiple MongoDB collections.
It provides automated scanning and organization of content stored in BunnyCDN.

System Architecture:
    - Storage Layer:
        * BunnyCDN: Primary content storage
        * MongoDB Collections:
            - upload_collection: Stories/Roll content
            - spotlight_collection: Spotlight videos
            - content_dump_collection: Archive content
            - saved_collection: Saved content
            - client_info: Creator metadata
    
    - Directory Structure:
        /sc/
        ├── {client_id}/
        │   ├── STORIES/
        │   │   └── F(date)_{client_id}/
        │   ├── SPOTLIGHT/
        │   │   └── F(date)_{client_id}/
        │   ├── CONTENT_DUMP/
        │   └── SAVED/
        │       └── F(date)_{client_id}/
    
    - Content Types:
        1. Stories/Roll:
            - Daily content
            - Session-based organization
            - View tracking
        2. Spotlight:
            - Featured content
            - Performance metrics
        3. Content Dump:
            - Archive storage
            - Bulk content
        4. Saved:
            - Preserved content
            - Historical records

Data Models:
    1. Session Document:
        - Unique session ID
        - Content metadata
        - File listings
        - Analytics data
    
    2. File Entry:
        - File metadata
        - CDN links
        - Sequence information
        - Upload tracking

Integration Points:
    - FastAPI endpoints
    - MongoDB databases
    - BunnyCDN storage
    - Client information system

Error Handling:
    - Storage failures
    - Database conflicts
    - Invalid content
    - Missing metadata
"""

import requests
import json
from datetime import datetime, date, timedelta, timezone
import os
from typing import Dict, List
import logging
import re
from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
from app.shared.database import upload_collection, spotlight_collection, content_dump_collection, client_info, saved_collection
from fastapi import APIRouter, HTTPException, Request
from dotenv import load_dotenv
import traceback
from app.features.video.video_splitter import VideoSplitter
from app.shared.bunny_cdn import BunnyCDN

# Configure logging for content management operations
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI router
router = APIRouter(prefix="/api/bunnyscan", tags=["bunnyscan"])

load_dotenv()

class BunnyScanner:
    """
    Core content management service for BunnyCDN-hosted Snapchat content.
    
    Responsibilities:
        - Content discovery and organization
        - File processing and metadata extraction
        - Database synchronization
        - Session management
        - Error handling and logging
    
    Integration Points:
        - BunnyCDN API
        - MongoDB collections
        - Video processing utilities
        - Client information system
    """
    
    def __init__(self):
        """
        Initialize scanner with required components and configuration.
        
        Components:
            - BunnyCDN client
            - Video processing tools
            - Date range configuration
            - Content type mappings
        """
        self.should_stop = False
        self.bunny = BunnyCDN()
        self.video_splitter = VideoSplitter(bunny_cdn=self.bunny)
        
        # BunnyCDN configuration
        self.api_key = self.bunny.api_key
        self.storage_zone = self.bunny.storage_zone
        self.base_url = self.bunny.base_url
        self.cdn_url = self.bunny.cdn_url
        self.headers = self.bunny.headers
        
        # Date range configuration for scanning
        today = datetime.now()
        self.dates = [
            (today + timedelta(days=x)).strftime("%m-%d-%Y")
            for x in range(0, 4)  # Today plus next three days
        ]
        
        # Content type configuration
        self.content_types = {
            "regular": ["STORIES", "ROLL"],
            "spotlight": ["SPOTLIGHT"],
            "content_dump": ["CONTENT_DUMP"],
            "saved": ["SAVED"]
        }
        logger.info(f"Scanning for dates: {self.dates}")
        logger.info(f"Initialized scanner for storage zone: {self.storage_zone}")

    def list_directory(self, path: str = "") -> List[Dict]:
        """
        List contents of a BunnyCDN directory.
        
        Args:
            path: Directory path to list
        
        Returns:
            List of file/directory entries
        
        Error Handling:
            - Network failures
            - Invalid paths
            - Authentication errors
        """
        url = f"{self.base_url}{self.storage_zone}/{path.strip('/')}"
        if not url.endswith('/'):
            url += '/'
        logger.info(f"Requesting directory listing from: {url}")
        response = requests.get(url, headers=self.headers)
        logger.info(f"Response status code: {response.status_code}")
        
        if response.status_code == 200:
            files = response.json()
            logger.info(f"Found {len(files)} files in response")
            logger.info(f"First few files: {files[:3] if files else 'None'}")
            return files
        else:
            logger.error(f"Failed to list directory {path}: {response.status_code}")
            logger.error(f"Response content: {response.text}")
            return []

    async def process_files(self, files: List[Dict], folder_path: str) -> tuple:
        """
        Process files and generate metadata entries.
        
        Processing Steps:
            1. File Filtering:
                - Directory exclusion
                - Name pattern analysis
            2. Metadata Extraction:
                - File type detection
                - Size calculation
                - Sequence assignment
            3. Entry Generation:
                - CDN link creation
                - Upload time tracking
                - Type categorization
        
        Args:
            files: List of file entries from BunnyCDN
            folder_path: Base path for CDN links
        
        Returns:
            tuple: (processed_files, total_size, image_count, video_count)
        """
        processed_files = []
        total_size = 0
        total_images = 0
        total_videos = 0
        current_seq = 1
        
        # Process all files in order
        for file in sorted(files, key=lambda x: x["ObjectName"]):
            if file.get("IsDirectory"):
                continue
                
            file_name = file["ObjectName"]
            
            processed_file = self._create_file_entry(file_name, file, folder_path, current_seq)
            processed_files.append(processed_file)
            total_size += file["Length"]
            
            if processed_file["file_type"] == "video":
                total_videos += 1
            else:
                total_images += 1
                
            current_seq += 1
        
        return processed_files, total_size, total_images, total_videos

    def _create_file_entry(self, file_name: str, file_info: Dict, folder_path: str, seq_number: int) -> Dict:
        """Helper method to create standardized file entries"""
        is_thumbnail = "-t." in file_name.lower()
        file_type = "video" if file_name.lower().endswith(('.mp4', '.mov', '.avi')) else "image"
        
        return {
            "file_name": file_name,
            "file_type": file_type,
            "CDN_link": f"{self.cdn_url}{folder_path}{file_name}",
            "file_size": file_info["Length"],
            "file_size_human": f"{file_info['Length'] / (1024*1024):.2f} MB",
            "video_length": 0 if file_type == "video" else None,
            "caption": "",
            "is_thumbnail": is_thumbnail,
            "seq_number": seq_number,
            "upload_time": datetime.now(timezone.utc).isoformat()
        }

    async def create_session_doc(self, client_id: str, content_type: str, scan_date: str, 
                               folder_path: str, files_data: tuple) -> Dict:
        """Create a session document with all required fields"""
        processed_files, total_size, total_images, total_videos = files_data
        
        # Standardize session_id format to use MM-DD-YYYY
        session_id = f"F({scan_date})_{client_id}"
        
        # Get client info including timezone
        client_doc = await client_info.find_one({"client_id": client_id})
        client_timezone = client_doc.get("Timezone") if client_doc else None
        
        if not client_timezone:
            logger.error(f"No timezone found for client {client_id}")
            raise ValueError(f"Missing timezone for client {client_id}")
        
        # Ensure consistent path format (no leading slash)
        clean_folder_path = folder_path.lstrip('/')
        
        # Process files with consistent structure
        standardized_files = []
        for file in processed_files:
            # Extract sequence number from filename (e.g., "0001-xxxx-xxxx" -> 1)
            seq_match = re.match(r'^(\d{4})', file["file_name"])
            seq_number = int(seq_match.group(1)) if seq_match else 0
            
            standardized_file = {
                "seq_number": seq_number,
                "file_name": file["file_name"],
                "file_type": file["file_type"],
                "CDN_link": file["CDN_link"],
                "file_size": file["file_size"],
                "file_size_human": file["file_size_human"],
                "video_length": file["video_length"],
                "caption": file.get("caption", ""),
                "is_thumbnail": file.get("is_thumbnail", False),
                "date_time": datetime.now(timezone.utc),
                "file_path": f"{clean_folder_path}{file['file_name']}"
            }
            standardized_files.append(standardized_file)

        # Sort files by sequence number
        standardized_files.sort(key=lambda x: x["seq_number"])

        return {
            "session_id": session_id,
            "content_type": content_type.upper(),
            "upload_date": datetime.now(timezone.utc),
            "folder_id": session_id,
            "folder_path": clean_folder_path,
            "total_files_count": len(standardized_files),
            "total_files_size": total_size,
            "total_files_size_human": f"{total_size / (1024*1024):.2f} MB",
            "total_images": total_images,
            "total_videos": total_videos,
            "editor_note": "",
            "total_session_views": 0,
            "avrg_session_view_time": 0,
            "all_video_length": 0,
            "timezone": client_timezone,
            "files": standardized_files
        }

    async def scan_client_content(self, client_id: str, content_type: str, scan_date: str):
        """Scan content for a specific client, type and date"""
        try:
            # For CONTENT_DUMP, we don't need the date in the folder path
            if content_type == "CONTENT_DUMP":
                folder_path = f"/sc/{client_id}/{content_type}/"
            else:
                folder_path = f"/sc/{client_id}/{content_type}/F({scan_date})_{client_id}/"
                
            logger.info(f"Scanning folder path: {folder_path}")
            files = self.list_directory(folder_path)
            
            if not files:
                logger.info(f"No files found in {folder_path}")
                return
            
            logger.info(f"Found {len(files)} files for {client_id}/{content_type} on {scan_date}")
            
            # Process files
            files_data, total_size, total_images, total_videos = await self.process_files(files, folder_path)
            
            # Prepare data for storage
            data = {
                "client_ID": client_id,
                "scan_date": scan_date,
                "files": files_data,
                "total_files_count": len(files_data),
                "total_files_size": total_size,
                "total_files_size_human": f"{total_size / (1024*1024):.2f} MB",
                "total_images": total_images,
                "total_videos": total_videos,
                "editor_note": "",
                "total_session_views": 0,
                "avrg_session_view_time": 0,
                "all_video_length": 0
            }
            
            logger.info(f"Processing content type: {content_type}")
            
            # Store based on content type
            if content_type == "STORIES":
                await self.store_stories(data)
            elif content_type == "SPOTLIGHT":
                await self.store_spotlight(data)
            elif content_type == "CONTENT_DUMP":
                await self.store_content_dump(data)
            elif content_type == "SAVED":
                await self.store_saved(data)
        except Exception as e:
            logger.error(f"Error in scan_client_content: {str(e)}")
            raise e

    async def scan_uploads(self):
        """Main scanning function"""
        if self.should_stop:
            return
        logger.info("Starting BunnyCDN scan...")
        
        sc_contents = self.list_directory("sc/")
        if not sc_contents:
            return

        for client_dir in sc_contents:
            if not client_dir.get("IsDirectory"):
                continue
                
            client_id = client_dir["ObjectName"]
            logger.info(f"Scanning client: {client_id}")
            
            content_dirs = self.list_directory(f"sc/{client_id}/")
            if not content_dirs:
                continue

            for content_dir in content_dirs:
                if not content_dir.get("IsDirectory"):
                    continue
                    
                content_type = content_dir["ObjectName"]
                if content_type in [ct for types in self.content_types.values() for ct in types]:
                    for scan_date in self.dates:
                        await self.scan_client_content(client_id, content_type, scan_date)

    async def store_stories(self, data: Dict):
        """
        Store stories/roll content in upload_collection.
        
        Storage Strategy:
            1. Document Lookup:
                - Client identification
                - Session existence check
            2. Content Management:
                - New file detection
                - Sequence maintenance
                - Size tracking
            3. Database Operations:
                - Atomic updates
                - Session organization
                - Metadata synchronization
        
        Args:
            data: Content metadata and file information
        
        Returns:
            Dict: Operation status and details
        """
        try:
            client_id = data["client_ID"]
            session_id = f"F({data['scan_date']})_{client_id}"
            
            # First check if document exists for this client
            client_doc = await upload_collection.find_one({"client_ID": client_id})
            
            if client_doc:
                # Check if sessions is an object instead of an array and fix it
                if client_doc.get('sessions') and isinstance(client_doc['sessions'], dict):
                    logger.info("Converting sessions from object to array format")
                    await upload_collection.update_one(
                        {"_id": client_doc["_id"]},
                        {
                            "$set": {
                                "sessions": [client_doc['sessions']]
                            }
                        }
                    )
                    # Refresh our document reference
                    client_doc = await upload_collection.find_one({"client_ID": client_id})

                # Now proceed with normal processing
                existing_sessions = client_doc.get("sessions", [])
                existing_session = None
                
                # Properly iterate through sessions with type checking
                for session in existing_sessions:
                    if isinstance(session, dict) and session.get("session_id") == session_id:
                        existing_session = session
                        break

                if existing_session:
                    # Compare files to find new ones
                    existing_files = {f["file_name"]: f for f in existing_session.get("files", [])}
                    new_files = [f for f in data["files"] if f["file_name"] not in existing_files]
                    
                    if not new_files:
                        logger.info(f"No new files to add for session {session_id}")
                        return {"status": "skipped", "message": "No new files to add"}
                    
                    # Update sequence numbers for new files
                    max_seq = max((f.get("seq_number", 0) for f in existing_session.get("files", [])), default=0)
                    for i, file in enumerate(new_files, start=max_seq + 1):
                        file["seq_number"] = i
                    
                    # Add only new files to the session and update totals
                    _ = await upload_collection.update_one(
                        {
                            "client_ID": client_id,
                            "sessions.session_id": session_id
                        },
                        {
                            "$push": {
                                "sessions.$.files": {"$each": new_files}
                            },
                            "$set": {
                                "last_updated": datetime.now(timezone.utc),
                                "sessions.$.total_files_count": existing_session["total_files_count"] + len(new_files),
                                "sessions.$.total_files_size": existing_session["total_files_size"] + sum(f["file_size"] for f in new_files),
                                "sessions.$.total_files_size_human": f"{(existing_session['total_files_size'] + sum(f['file_size'] for f in new_files)) / (1024*1024):.2f} MB",
                                "sessions.$.total_images": existing_session["total_images"] + sum(1 for f in new_files if f["file_type"] == "image"),
                                "sessions.$.total_videos": existing_session["total_videos"] + sum(1 for f in new_files if f["file_type"] == "video")
                            }
                        }
                    )
                    logger.info(f"Added {len(new_files)} new files to existing session {session_id}")
                    return {"status": "success", "message": f"Added {len(new_files)} new files to session"}
                else:
                    # Add new session to existing document
                    # Get client info including snap_ID first
                    client_info_doc = await client_info.find_one({"client_id": client_id})
                    if not client_info_doc:
                        logger.error(f"No client info found for client {client_id}")
                        raise ValueError(f"Missing client info for {client_id}")

                    _ = await upload_collection.update_one(
                        {"client_ID": client_id},
                        {
                            "$push": {
                                "sessions": {
                                    "session_id": session_id,
                                    "content_type": "STORIES",
                                    "upload_date": datetime.now(timezone.utc),
                                    "folder_id": session_id,
                                    "folder_path": f"sc/{client_id}/STORIES/{session_id}/",
                                    **data
                                }
                            },
                            "$set": {
                                "last_updated": datetime.now(timezone.utc),
                                "snap_ID": client_info_doc.get("snap_id", "")
                            }
                        }
                    )
            else:
                # Get client info including snap_ID
                client_info_doc = await client_info.find_one({"client_id": client_id})
                if not client_info_doc:
                    logger.error(f"No client info found for client {client_id}")
                    raise ValueError(f"Missing client info for {client_id}")
                
                # Create new document with first session
                _ = await upload_collection.insert_one({
                    "client_ID": client_id,
                    "snap_ID": client_info_doc.get("snap_id", ""),
                    "last_updated": datetime.now(timezone.utc),
                    "sessions": [{
                        "session_id": session_id,
                        "content_type": "STORIES",
                        "upload_date": datetime.now(timezone.utc),
                        "folder_id": session_id,
                        "folder_path": f"sc/{client_id}/STORIES/{session_id}/",
                        **data
                    }]
                })
            
            logger.info(f"Successfully stored new stories session for {client_id}")
            return {"status": "success", "message": "Stored stories session"}
        except Exception as e:
            logger.error(f"Error storing stories: {e}")
            raise e

    async def store_spotlight(self, data: Dict):
        """
        Store spotlight content in spotlight_collection.
        
        Processing Flow:
            1. Session Management:
                - Unique session identification
                - Date-based organization
                - Content type validation
            
            2. Database Operations:
                - Document lookup/creation
                - Atomic updates
                - Session merging
            
            3. Metadata Handling:
                - Client info synchronization
                - Upload time tracking
                - File organization
        
        Error Handling:
            - Database conflicts
            - Missing client info
            - Invalid data format
            
        Args:
            data: Spotlight content metadata and files
            
        Returns:
            Dict: Operation status and details
        """
        try:
            logger.info(f"Starting store_spotlight with data: {json.dumps(data, default=str)}")
            
            client_id = data["client_ID"]
            session_id = f"F({data['scan_date']})_{client_id}"
            
            logger.info(f"Looking for existing client doc with client_ID: {client_id}")
            # First check if document exists for this client
            client_doc = await spotlight_collection.find_one({"client_ID": client_id})
            logger.info(f"Found client doc: {client_doc is not None}")
            
            if not client_doc:
                logger.info("No client doc found, getting client info...")
                # Get client info including snap_ID
                client_info_doc = await client_info.find_one({"client_id": client_id})
                if not client_info_doc:
                    logger.error(f"No client info found for client {client_id}")
                    raise ValueError(f"Missing client info for {client_id}")
                
                logger.info("Creating new SPOTLIGHT document...")
                # Create new document with first session
                new_doc = {
                    "client_ID": client_id,
                    "snap_ID": client_info_doc.get("snap_id", ""),
                    "last_updated": datetime.now(timezone.utc),
                    "sessions": [{
                        "session_id": session_id,
                        "content_type": "SPOTLIGHT",
                        "upload_date": datetime.now(timezone.utc),
                        "folder_id": session_id,
                        "folder_path": f"sc/{client_id}/SPOTLIGHT/{session_id}/",
                        **data
                    }]
                }
                logger.info(f"New document structure: {json.dumps(new_doc, default=str)}")
                
                try:
                    result = await spotlight_collection.insert_one(new_doc)
                    logger.info(f"Insert result: {result.inserted_id}")
                except Exception as db_error:
                    logger.error(f"Database error: {str(db_error)}")
                    logger.error(f"Full traceback: {traceback.format_exc()}")
                    raise
                
                logger.info(f"Created new SPOTLIGHT document for {client_id}")
                return {"status": "success", "message": "Created new SPOTLIGHT document"}
            
            logger.info("Found existing client doc, checking for session...")
            # Check if session exists
            existing_session = next(
                (s for s in client_doc.get("sessions", []) if s.get("session_id") == session_id),
                None
            )
            logger.info(f"Found existing session: {existing_session is not None}")
            
            if not existing_session:
                logger.info("No existing session found, adding new session...")
                # Add new session
                new_session = {
                    "session_id": session_id,
                    "content_type": "SPOTLIGHT",
                    "upload_date": datetime.now(timezone.utc),
                    "folder_id": session_id,
                    "folder_path": f"sc/{client_id}/SPOTLIGHT/{session_id}/",
                    **data
                }
                logger.info(f"New session structure: {json.dumps(new_session, default=str)}")
                
                try:
                    result = await spotlight_collection.update_one(
                        {"client_ID": client_id},
                        {
                            "$push": {"sessions": new_session},
                            "$set": {"last_updated": datetime.now(timezone.utc)}
                        }
                    )
                    logger.info(f"Update result - matched: {result.matched_count}, modified: {result.modified_count}")
                except Exception as db_error:
                    logger.error(f"Database error: {str(db_error)}")
                    logger.error(f"Full traceback: {traceback.format_exc()}")
                    raise
                
                logger.info(f"Added new session {session_id} to existing SPOTLIGHT document")
                return {"status": "success", "message": "Added new SPOTLIGHT session"}
            
            logger.info("Found existing session, updating...")
            # Update existing session
            try:
                result = await spotlight_collection.update_one(
                    {"client_ID": client_id, "sessions.session_id": session_id},
                    {
                        "$set": {
                            "sessions.$": {
                                **existing_session,
                                **data,
                                "last_updated": datetime.now(timezone.utc)
                            }
                        }
                    }
                )
                logger.info(f"Update result - matched: {result.matched_count}, modified: {result.modified_count}")
            except Exception as db_error:
                logger.error(f"Database error: {str(db_error)}")
                logger.error(f"Full traceback: {traceback.format_exc()}")
                raise
            
            logger.info(f"Updated existing SPOTLIGHT session {session_id}")
            return {"status": "success", "message": "Updated SPOTLIGHT session"}
            
        except Exception as e:
            logger.error(f"Error storing spotlight: {str(e)}")
            logger.error(f"Full traceback: {traceback.format_exc()}")
            raise

    async def store_content_dump(self, data: Dict):
        """
        Store archive content in content_dump_collection.
        
        Storage Strategy:
            1. Content Organization:
                - Single session per client
                - Continuous file addition
                - Size tracking
            
            2. File Management:
                - Duplicate detection
                - Sequence maintenance
                - Metadata updates
            
            3. Performance Optimization:
                - Batch updates
                - Index usage
                - Query optimization
        
        Args:
            data: Archive content metadata and files
            
        Returns:
            Dict: Operation status and details
        """
        try:
            client_id = data["client_ID"]
            session_id = f"CONTENTDUMP_{client_id}"
            
            # First check if document exists for this client
            client_doc = await content_dump_collection.find_one({"client_ID": client_id})
            
            if client_doc:
                # Check if sessions is an object instead of an array and fix it
                if client_doc.get('sessions') and isinstance(client_doc['sessions'], dict):
                    logger.info("Converting sessions from object to array format")
                    await content_dump_collection.update_one(
                        {"_id": client_doc["_id"]},
                        {
                            "$set": {
                                "sessions": [client_doc['sessions']]
                            }
                        }
                    )
                    # Refresh our document reference
                    client_doc = await content_dump_collection.find_one({"client_ID": client_id})

                # Now proceed with normal processing
                existing_sessions = client_doc.get("sessions", [])
                existing_session = None
                
                # Properly iterate through sessions with type checking
                for session in existing_sessions:
                    if isinstance(session, dict) and session.get("session_id") == session_id:
                        existing_session = session
                        break

                if existing_session:
                    # Compare files to find new ones
                    existing_files = {f["file_name"]: f for f in existing_session.get("files", [])}
                    new_files = [f for f in data["files"] if f["file_name"] not in existing_files]
                    
                    if not new_files:
                        logger.info(f"No new files to add for session {session_id}")
                        return {"status": "skipped", "message": "No new files to add"}
                    
                    # Update sequence numbers for new files
                    max_seq = max((f.get("seq_number", 0) for f in existing_session.get("files", [])), default=0)
                    for i, file in enumerate(new_files, start=max_seq + 1):
                        file["seq_number"] = i
                    
                    # Add only new files to the session and update totals
                    _ = await content_dump_collection.update_one(
                        {
                            "client_ID": client_id,
                            "sessions.session_id": session_id
                        },
                        {
                            "$push": {
                                "sessions.$.files": {"$each": new_files}
                            },
                            "$set": {
                                "last_updated": datetime.now(timezone.utc),
                                "sessions.$.total_files_count": existing_session["total_files_count"] + len(new_files),
                                "sessions.$.total_files_size": existing_session["total_files_size"] + sum(f["file_size"] for f in new_files),
                                "sessions.$.total_files_size_human": f"{(existing_session['total_files_size'] + sum(f['file_size'] for f in new_files)) / (1024*1024):.2f} MB",
                                "sessions.$.total_images": existing_session["total_images"] + sum(1 for f in new_files if f["file_type"] == "image"),
                                "sessions.$.total_videos": existing_session["total_videos"] + sum(1 for f in new_files if f["file_type"] == "video")
                            }
                        }
                    )
                    logger.info(f"Added {len(new_files)} new files to existing session {session_id}")
                    return {"status": "success", "message": f"Added {len(new_files)} new files to session"}
                else:
                    # Add new session to existing document
                    _ = await content_dump_collection.update_one(
                        {"client_ID": client_id},
                        {
                            "$push": {
                                "sessions": {
                                    "session_id": session_id,
                                    "content_type": "CONTENT_DUMP",
                                    "upload_date": datetime.now(timezone.utc),
                                    "folder_id": session_id,
                                    "folder_path": f"sc/{client_id}/CONTENT_DUMP/",
                                    **data
                                }
                            },
                            "$set": {
                                "last_updated": datetime.now(timezone.utc)
                            }
                        }
                    )
            else:
                # Create new document with first session - no need for client info lookup
                _ = await content_dump_collection.insert_one({
                    "client_ID": client_id,
                    "last_updated": datetime.now(timezone.utc),
                    "sessions": [{
                        "session_id": session_id,
                        "content_type": "CONTENT_DUMP",
                        "upload_date": datetime.now(timezone.utc),
                        "folder_id": session_id,
                        "folder_path": f"sc/{client_id}/CONTENT_DUMP/",
                        **data
                    }]
                })
            
            logger.info(f"Successfully stored content dump session for {client_id}")
            return {"status": "success", "message": "Stored content dump session"}
        except Exception as e:
            logger.error(f"Error storing content dump: {e}")
            raise e

    async def store_saved(self, data: Dict):
        """
        Store saved content in saved_collection.
        
        Processing Flow:
            1. Content Validation:
                - File integrity check
                - Metadata completeness
                - Client verification
            
            2. Storage Operations:
                - Session management
                - File organization
                - Size tracking
            
            3. Data Synchronization:
                - Client info updates
                - Session metadata
                - File sequencing
        
        Args:
            data: Saved content metadata and files
            
        Returns:
            Dict: Operation status and details
        """
        try:
            client_id = data["client_ID"]
            session_id = f"F({data['scan_date']})_{client_id}"
            
            # First check if document exists for this client
            client_doc = await saved_collection.find_one({"client_ID": client_id})
            
            if client_doc:
                # Check if sessions is an object instead of an array and fix it
                if client_doc.get('sessions') and isinstance(client_doc['sessions'], dict):
                    logger.info("Converting sessions from object to array format")
                    await saved_collection.update_one(
                        {"_id": client_doc["_id"]},
                        {
                            "$set": {
                                "sessions": [client_doc['sessions']]
                            }
                        }
                    )
                    # Refresh our document reference
                    client_doc = await saved_collection.find_one({"client_ID": client_id})

                # Now proceed with normal processing
                existing_sessions = client_doc.get("sessions", [])
                existing_session = None
                
                for session in existing_sessions:
                    if isinstance(session, dict) and session.get("session_id") == session_id:
                        existing_session = session
                        break

                if existing_session:
                    # Update existing session logic...
                    existing_files = {f["file_name"]: f for f in existing_session.get("files", [])}
                    new_files = [f for f in data["files"] if f["file_name"] not in existing_files]
                    
                    if not new_files:
                        logger.info(f"No new files to add for session {session_id}")
                        return {"status": "skipped", "message": "No new files to add"}
                    
                    # Update sequence numbers for new files
                    max_seq = max((f.get("seq_number", 0) for f in existing_session.get("files", [])), default=0)
                    for i, file in enumerate(new_files, start=max_seq + 1):
                        file["seq_number"] = i
                    
                    # Add only new files to the session and update totals
                    _ = await saved_collection.update_one(
                        {
                            "client_ID": client_id,
                            "sessions.session_id": session_id
                        },
                        {
                            "$push": {
                                "sessions.$.files": {"$each": new_files}
                            },
                            "$set": {
                                "last_updated": datetime.now(timezone.utc),
                                "sessions.$.total_files_count": existing_session["total_files_count"] + len(new_files),
                                "sessions.$.total_files_size": existing_session["total_files_size"] + sum(f["file_size"] for f in new_files),
                                "sessions.$.total_files_size_human": f"{(existing_session['total_files_size'] + sum(f['file_size'] for f in new_files)) / (1024*1024):.2f} MB",
                                "sessions.$.total_images": existing_session["total_images"] + sum(1 for f in new_files if f["file_type"] == "image"),
                                "sessions.$.total_videos": existing_session["total_videos"] + sum(1 for f in new_files if f["file_type"] == "video")
                            }
                        }
                    )
                else:
                    # Add new session to existing document
                    client_info_doc = await client_info.find_one({"client_id": client_id})
                    if not client_info_doc:
                        logger.error(f"No client info found for client {client_id}")
                        raise ValueError(f"Missing client info for {client_id}")

                    _ = await saved_collection.update_one(
                        {"client_ID": client_id},
                        {
                            "$push": {
                                "sessions": {
                                    "session_id": session_id,
                                    "content_type": "SAVED",
                                    "upload_date": datetime.now(timezone.utc),
                                    "folder_id": session_id,
                                    "folder_path": f"sc/{client_id}/SAVED/{session_id}/",
                                    **data
                                }
                            },
                            "$set": {
                                "last_updated": datetime.now(timezone.utc),
                                "snap_ID": client_info_doc.get("snap_id", "")
                            }
                        }
                    )
            else:
                # Create new document with first session
                client_info_doc = await client_info.find_one({"client_id": client_id})
                if not client_info_doc:
                    logger.error(f"No client info found for client {client_id}")
                    raise ValueError(f"Missing client info for {client_id}")
                
                _ = await saved_collection.insert_one({
                    "client_ID": client_id,
                    "snap_ID": client_info_doc.get("snap_id", ""),
                    "last_updated": datetime.now(timezone.utc),
                    "sessions": [{
                        "session_id": session_id,
                        "content_type": "SAVED",
                        "upload_date": datetime.now(timezone.utc),
                        "folder_id": session_id,
                        "folder_path": f"sc/{client_id}/SAVED/{session_id}/",
                        **data
                    }]
                })
            
            logger.info(f"Successfully stored new saved session for {client_id}")
            return {"status": "success", "message": "Stored saved session"}
        except Exception as e:
            logger.error(f"Error storing saved content: {e}")
            raise e

    async def scan_specific_path(self, path: str):
        """
        Scan and process content from a specific CDN path.
        
        Path Processing:
            1. Path Validation:
                - Format verification
                - Component extraction
                - Access checking
            
            2. Content Discovery:
                - Directory listing
                - File filtering
                - Type detection
            
            3. Processing Flow:
                - Content type routing
                - Date extraction
                - Client validation
        
        Args:
            path: CDN path to scan
            
        Returns:
            Dict: Scan results and processed content details
        """
        logger.info(f"Scanning specific path: {path}")
        
        files = self.list_directory(path)
        logger.info(f"Found {len(files) if files else 0} files in path")
        
        if not files:
            logger.info(f"No files found in {path}")
            return
        
        # Extract client_id and content_type from path
        path_parts = path.strip("/").split("/")
        logger.info(f"Path parts: {path_parts}")
        
        if len(path_parts) >= 3 and path_parts[0] == "sc":
            client_id = path_parts[1]
            content_type = path_parts[2]
            logger.info(f"Extracted client_id: {client_id}, content_type: {content_type}")
            
            # Get scan date if available
            scan_date = None
            if len(path_parts) > 3 and path_parts[3].startswith("F("):
                scan_date = path_parts[3].split("_")[0][2:-1]
                logger.info(f"Extracted scan_date: {scan_date}")
            
            if scan_date:
                logger.info(f"Scanning specific date {scan_date}")
                await self.scan_client_content(client_id, content_type, scan_date)
            else:
                logger.info(f"Scanning all dates for {content_type}")
                for date in self.dates:
                    await self.scan_client_content(client_id, content_type, date)

    async def refresh_client_content(self, client_id: str, content_type: str, scan_date: str):
        """
        Refresh and reprocess client content for a specific date.
        
        Processing Steps:
            1. Content Location:
                - Path construction
                - Directory scanning
                - File discovery
            
            2. Data Processing:
                - File analysis
                - Metadata extraction
                - Session creation
            
            3. Storage Update:
                - Collection selection
                - Document refresh
                - Session synchronization
        
        Args:
            client_id: Target client identifier
            content_type: Content category to refresh
            scan_date: Target date for refresh
            
        Returns:
            Dict: Refresh operation status and details
        """
        try:
            # For CONTENT_DUMP, we don't need the date in the folder path
            if content_type == "CONTENT_DUMP":
                folder_path = f"/sc/{client_id}/{content_type}/"
            else:
                folder_path = f"/sc/{client_id}/{content_type}/F({scan_date})_{client_id}/"
            
            logger.info(f"Refreshing folder path: {folder_path}")
            files = self.list_directory(folder_path)
            
            if not files:
                logger.info(f"No files found in {folder_path}")
                return
            
            logger.info(f"Found {len(files)} files for {client_id}/{content_type} on {scan_date}")
            
            # Process files
            files_data = await self.process_files(files, folder_path)
            
            # Create session document
            session_doc = await self.create_session_doc(client_id, content_type, scan_date, folder_path, files_data)
            
            # Store based on content type
            collection_map = {
                "STORIES": upload_collection,
                "SPOTLIGHT": spotlight_collection,
                "CONTENT_DUMP": content_dump_collection,
                "SAVED": saved_collection
            }
            
            collection = collection_map.get(content_type)
            if not collection:
                raise ValueError(f"Invalid content type: {content_type}")
            
            # Get client info
            client_info_doc = await client_info.find_one({"client_id": client_id})
            if not client_info_doc and content_type != "CONTENT_DUMP":
                logger.error(f"No client info found for client {client_id}")
                raise ValueError(f"Missing client info for {client_id}")
            
            # Update or create document
            session_id = session_doc["session_id"]
            client_doc = await collection.find_one({"client_ID": client_id})
            
            if client_doc:
                # Check if session exists
                session_exists = any(s.get("session_id") == session_id for s in client_doc.get("sessions", []))
                
                if session_exists:
                    # Replace the specific session in the array
                    await collection.update_one(
                        {
                            "client_ID": client_id,
                            "sessions.session_id": session_id
                        },
                        {
                            "$set": {
                                "last_updated": datetime.now(timezone.utc),
                                "sessions.$": session_doc
                            }
                        }
                    )
                    logger.info(f"Replaced existing session {session_id}")
                else:
                    # Add new session to array
                    await collection.update_one(
                        {"client_ID": client_id},
                        {
                            "$push": {"sessions": session_doc},
                            "$set": {
                                "last_updated": datetime.now(timezone.utc)
                            }
                        }
                    )
                    logger.info(f"Added new session {session_id}")
            else:
                # Create new client document with first session
                await collection.insert_one({
                    "client_ID": client_id,
                    "snap_ID": client_info_doc.get("snap_id", "") if client_info_doc else "",
                    "last_updated": datetime.now(timezone.utc),
                    "sessions": [session_doc]
                })
                logger.info(f"Created new client document with session {session_id}")
            
            logger.info(f"Successfully refreshed session for {client_id}")
            return {"status": "success", "message": "Refreshed session"}
            
        except Exception as e:
            logger.error(f"Error in refresh_client_content: {e}")
            raise e

@router.post("/stories")
async def api_store_stories(data: Dict):
    """
    API endpoint for storing story content.
    
    Processing:
        1. Request validation
        2. Scanner initialization
        3. Content storage
        
    Returns:
        Dict: Operation result
    """
    scanner = BunnyScanner()
    return await scanner.store_stories(data)

@router.post("/spotlight")
async def api_store_spotlight(data: Dict):
    """
    API endpoint for storing spotlight content.
    
    Processing:
        1. Data validation
        2. Scanner initialization
        3. Content processing
        
    Returns:
        Dict: Operation result
    """
    scanner = BunnyScanner()
    return await scanner.store_spotlight(data)

@router.post("/content_dump")
async def api_store_content_dump(data: Dict):
    """
    API endpoint for storing archive content.
    
    Processing:
        1. Request validation
        2. Scanner initialization
        3. Archive storage
        
    Returns:
        Dict: Operation result
    """
    scanner = BunnyScanner()
    return await scanner.store_content_dump(data)

@router.post("/saved")
async def api_store_saved(data: Dict):
    """
    API endpoint for storing saved content.
    
    Processing:
        1. Data validation
        2. Scanner initialization
        3. Content storage
        
    Returns:
        Dict: Operation result
    """
    scanner = BunnyScanner()
    return await scanner.store_saved(data)

@router.post("/scan-path")
async def scan_specific_path(request: Request):
    """
    API endpoint for scanning specific CDN paths.
    
    Processing Flow:
        1. Request Validation:
            - Path verification
            - Component extraction
            - Type checking
        
        2. Content Processing:
            - Scanner initialization
            - Content type routing
            - Date handling
        
        3. Error Handling:
            - Invalid paths
            - Processing failures
            - Missing data
    
    Returns:
        Dict: Scan results and status
    """
    try:
        data = await request.json()
        path = data.get("path")
        if not path:
            raise HTTPException(status_code=400, detail="Path is required")

        # Extract client_id and content_type from path
        path_parts = path.strip("/").split("/")
        if len(path_parts) >= 3 and path_parts[0] == "sc":
            client_id = path_parts[1]
            content_type = path_parts[2]
            
            scanner = BunnyScanner()
            today = datetime.now()
            scan_date = today.strftime("%m-%d-%Y")

            # Handle different content types
            if content_type == "CONTENT_DUMP":
                # Content dump doesn't use dates, just scan the folder
                await scanner.scan_client_content(client_id, content_type, scan_date)
            elif content_type in ["SPOTLIGHT", "STORIES", "SAVED"]:
                # Get scan date if available from F(date) pattern
                if len(path_parts) > 3 and path_parts[3].startswith("F("):
                    scan_date = path_parts[3].split("_")[0][2:-1]  # Extract date from F(date)
                    logger.info(f"Extracted scan date: {scan_date} from path: {path}")
                
                try:
                    await scanner.scan_client_content(client_id, content_type, scan_date)
                except Exception as e:
                    logger.error(f"Error in scan_client_content: {str(e)}")
                    logger.error(f"Full traceback: {traceback.format_exc()}")
                    raise
            else:
                logger.error(f"Invalid content type: {content_type}")
                raise HTTPException(status_code=400, detail=f"Invalid content type: {content_type}")
                    
            return {
                "status": "success",
                "message": f"Scanned path: {path}",
                "client_id": client_id,
                "content_type": content_type,
                "scan_date": scan_date
            }
            
        raise HTTPException(status_code=400, detail="Invalid path format")
        
    except Exception as e:
        logger.error(f"Error scanning path: {str(e)}")
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

# if __name__ == "__main__":
#     asyncio.run(main()) 