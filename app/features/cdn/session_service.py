import logging
from datetime import datetime, timezone
from app.shared.database import (
    upload_collection, 
    spotlight_collection,
    saved_collection,
    content_dump_collection,
    client_info
)
from app.features.cdn.s3_service import S3Service
import re
from typing import List, Dict
import traceback

logger = logging.getLogger(__name__)

class SessionService:
    @staticmethod
    async def init_session(client_id: str, folder_path: str):
        """Initialize a new session when creating a folder"""
        try:
            logger.info(f"\n=== INITIALIZING SESSION ===")
            logger.info(f"Client ID: {client_id}")
            logger.info(f"Folder Path: {folder_path}")

            # Extract date and determine collection type from path
            date_match = re.search(r'F\(([\d-]+)\)', folder_path)
            if not date_match:
                logger.error(f"Invalid folder path format: {folder_path}")
                return None

            # Get date in correct format
            scan_date = date_match.group(1)
            session_id = f"F({scan_date})_{client_id}"
            logger.info(f"Session ID: {session_id}")

            # Determine collection type from path
            if "/SAVED/" in folder_path:
                collection = saved_collection
                content_type = "SAVED"
            elif "/SPOTLIGHT/" in folder_path:
                collection = spotlight_collection
                content_type = "SPOTLIGHT"
            else:
                collection = upload_collection
                content_type = "STORIES"

            logger.info(f"Using collection: {collection.name}")

            # Get client info for snap_id
            logger.info(f"Looking for client in client_info collection with client_id: {client_id}")
            client_doc = await client_info.find_one({"client_id": client_id})
            logger.info(f"Client doc found: {client_doc is not None}")
            if client_doc:
                logger.info(f"Client doc: {client_doc}")
            else:
                logger.error(f"No client info found for client {client_id}")
                # Let's see what's in the collection
                all_clients = await client_info.find({}).to_list(length=10)
                logger.info(f"Sample of clients in collection: {all_clients}")
                return None
            
            logger.info(f"Found client info: {client_doc.get('First_Legal_Name')} {client_doc.get('Last_Legal_Name')}")

            # Create session document matching exact structure
            session_data = {
                "session_id": session_id,
                "content_type": content_type,
                "upload_date": datetime.now(timezone.utc),
                "folder_id": session_id,
                "folder_path": folder_path,
                "client_ID": client_id,
                "scan_date": scan_date,
                "total_files_count": 0,
                "total_files_size": 0,
                "total_files_size_human": "0.00 MB",
                "total_images": 0,
                "total_videos": 0,
                "editor_note": "",
                "total_session_views": 0,
                "avrg_session_view_time": 0,
                "all_video_length": 0,
                "files": []
            }

            # Check if client document exists
            existing_doc = await collection.find_one({"client_ID": client_id})
            logger.info(f"Existing doc found in {collection.name}: {existing_doc is not None}")
            
            try:
                if existing_doc:
                    logger.info("Updating existing document")
                    result = await collection.update_one(
                        {"client_ID": client_id},
                        {
                            "$push": {"sessions": session_data},
                            "$set": {
                                "last_updated": datetime.now(timezone.utc),
                                "snap_id": client_doc.get("snap_id", "")
                            }
                        }
                    )
                    logger.info(f"Update result - matched: {result.matched_count}, modified: {result.modified_count}")
                else:
                    logger.info("Creating new document")
                    new_doc = {
                        "client_ID": client_id,
                        "snap_id": client_doc.get("snap_id", ""),
                        "last_updated": datetime.now(timezone.utc),
                        "sessions": [session_data]
                    }
                    logger.info(f"New document structure: {new_doc}")
                    result = await collection.insert_one(new_doc)
                    logger.info(f"Insert result - inserted ID: {result.inserted_id}")

            except Exception as db_error:
                logger.error(f"Database operation failed: {str(db_error)}")
                logger.exception("Database error traceback:")
                return None

            return session_data

        except Exception as e:
            logger.error(f"Error initializing session: {str(e)}")
            logger.exception("Full traceback:")
            return None

    @staticmethod
    async def move_files(source_path: str, dest_path: str, files: List[Dict]) -> Dict:
        print(f"📦 Moving {len(files)} files")
        print(f"   From: {source_path}")
        print(f"   To: {dest_path}")
        
        try:
            # Extract client ID from path - handle both public/ and /sc/ prefixes
            client_match = re.search(r'(?:public/|/sc/)([^/]+)/', source_path)
            if not client_match:
                logger.error(f"Invalid source path format: {source_path}")
                return False

            client_ID = client_match.group(1)
            
            # Get source and destination collections
            is_content_dump = "/CONTENT_DUMP/" in source_path
            
            # Perform the actual file move in S3
            s3_service = S3Service()
            result = await s3_service.move_files(source_path, dest_path, files)
            print("✅ Files moved successfully")
            return result

            # # Database operations commented out for now
            # source_collection = (
            #     content_dump_collection if is_content_dump
            #     else spotlight_collection if "/SPOTLIGHT/" in source_path
            #     else upload_collection
            # )
            
            # dest_collection = (
            #     spotlight_collection if "/SPOTLIGHT/" in dest_path
            #     else upload_collection
            # )

            # # Extract session IDs from numeric folder names
            # def extract_session_id(path, client_id):
            #     # Try the F(date)_clientid format first
            #     session_match = re.search(r'F\([\d-]+\)_[^/]+', path)
            #     if session_match:
            #         return session_match.group(0)
                
            #     # Try numeric folder format (e.g., 040625)
            #     folder_match = re.search(r'/(\d{6})/', path)
            #     if folder_match:
            #         folder_date = folder_match.group(1)
            #         return f"F({folder_date})_{client_id}"
                    
            #     return None

            # # Get source and destination session IDs
            # source_session = "CONTENTDUMP_" + client_ID if is_content_dump else extract_session_id(source_path, client_ID)
            # if not source_session and not is_content_dump:
            #     logger.error(f"Could not extract session ID from source path: {source_path}")
            #     return False

            # dest_session = extract_session_id(dest_path, client_ID)
            # if not dest_session:
            #     logger.error(f"Could not extract session ID from destination path: {dest_path}")
            #     return False

            # logger.debug(f"Source session: {source_session}")
            # logger.debug(f"Destination session: {dest_session}")

            # # Initialize destination session if needed for SPOTLIGHT
            # if "/SPOTLIGHT/" in dest_path:
            #     dest_session_match = re.search(r'F\([\d-]+\)_[^/]+', dest_path)
            #     if dest_session_match:
            #         dest_session = dest_session_match.group(0)
            #         # Check if session exists
            #         existing_session = await spotlight_collection.find_one({
            #             "client_ID": client_ID,
            #             "sessions.session_id": dest_session
            #         })
            #         if not existing_session:
            #             # Initialize new session
            #             session_data = await SessionService.init_session(client_ID, dest_path)
            #             if not session_data:
            #                 logger.error(f"Failed to initialize SPOTLIGHT session: {dest_session}")
            #                 return False
                        
            #             # Create document if it doesn't exist
            #             await spotlight_collection.update_one(
            #                 {"client_ID": client_ID},
            #                 {
            #                     "$setOnInsert": {
            #                         "client_ID": client_ID,
            #                         "last_updated": datetime.now(timezone.utc),
            #                         "sessions": []
            #                     }
            #                 },
            #                 upsert=True
            #             )
                        
            #             # Add the session
            #             await spotlight_collection.update_one(
            #                 {"client_ID": client_ID},
            #                 {
            #                     "$push": {"sessions": session_data}
            #                 }
            #             )

            # # Get source files data differently for content dump
            # if is_content_dump:
            #     source_session = "CONTENTDUMP_" + client_ID
                
            #     # Get existing content dump data
            #     content_dump_doc = await content_dump_collection.find_one(
            #         {"client_ID": client_ID, "sessions.session_id": source_session}
            #     )
                
            #     if not content_dump_doc:
            #         print(f"❌ No content dump found for client {client_ID}")
            #         return False
                
            #     # Create lookup dict of files to move by name
            #     files_to_move = {f["name"]: f for f in files}
                
            #     # Get existing file data from content dump
            #     existing_files = next(
            #         (s["files"] for s in content_dump_doc["sessions"] 
            #          if s["session_id"] == source_session),
            #         []
            #     )
                
            #     # Match files with existing data
            #     moved_files = []
            #     for file_name, file_info in files_to_move.items():
            #         # Find existing file data
            #         existing_file = next(
            #             (f for f in existing_files if f["file_name"] == file_name),
            #             None
            #         )
                    
            #         if existing_file:
            #             # Use existing data but update paths
            #             file_data = existing_file.copy()
            #             file_data["CDN_link"] = f"https://snapped2.b-cdn.net/{dest_path.strip('/')}/{file_name}"
            #             file_data["file_path"] = f"{dest_path.strip('/')}/{file_name}"
            #             moved_files.append(file_data)
                        
            #             # Remove from content dump after successful move
            #             await content_dump_collection.update_one(
            #                 {"client_ID": client_ID, "sessions.session_id": source_session},
            #                 {"$pull": {"sessions.$.files": {"file_name": file_name}}}
            #             )
            #         else:
            #             # Create new file data if not found
            #             is_video = file_name.lower().endswith(('.mp4', '.mov', '.avi'))
            #             file_data = {
            #                 "seq_number": 0,
            #                 "file_name": file_name,
            #                 "file_type": "video" if is_video else "image",
            #                 "CDN_link": f"https://snapped2.b-cdn.net/{dest_path.strip('/')}/{file_name}",
            #                 "file_size": file_info.get("size", 0),
            #                 "file_size_human": f"{file_info.get('size', 0) / (1024*1024):.2f} MB",
            #                 "video_length": 0 if is_video else None,
            #                 "caption": "",
            #                 "is_thumbnail": False,
            #                 "upload_time": file_info.get("lastModified"),
            #                 "file_path": f"{dest_path.strip('/')}/{file_name}"
            #             }
            #             moved_files.append(file_data)

            # # Remove files from source if not content dump
            # if not is_content_dump:
            #     await source_collection.update_one(
            #         {"client_ID": client_ID, "sessions.session_id": source_session},
            #         {"$pull": {"sessions.$.files": {"file_name": {"$in": [f["name"] for f in files]}}}}
            #     )

            # # Add files to destination
            # if moved_files:
            #     await dest_collection.update_one(
            #         {"client_ID": client_ID, "sessions.session_id": dest_session},
            #         {"$push": {"sessions.$.files": {"$each": moved_files}}}
            #     )

            #     # Update totals for both sessions
            #     await SessionService.update_session_totals(dest_collection, client_ID, dest_session)
                
            #     # Only update content dump totals if it's a content dump move
            #     if is_content_dump:
            #         await SessionService.update_session_totals(content_dump_collection, client_ID, source_session)

        except Exception as e:
            print(f"❌ Move failed: {str(e)}")
            raise e

    @staticmethod
    async def update_session_totals(collection, client_ID: str, session_id: str):
        """Update session totals after file operations"""
        try:
            # Get current session
            doc = await collection.find_one(
                {"client_ID": client_ID, "sessions.session_id": session_id},
                {"sessions.$": 1}
            )
            
            logger.info(f"Updating totals for session {session_id} in {collection.name}")
            logger.info(f"Found document: {doc is not None}")

            if not doc or not doc.get("sessions"):
                logger.error(f"No session found for {session_id}")
                return False

            session = doc["sessions"][0]
            files = session.get("files", [])
            
            logger.info(f"Current file count: {len(files)}")

            # Calculate new totals
            total_size = sum(f.get("file_size", 0) for f in files)
            total_images = sum(1 for f in files if f.get("file_type") == "image")
            total_videos = sum(1 for f in files if f.get("file_type") == "video")

            # Update session
            await collection.update_one(
                {"client_ID": client_ID, "sessions.session_id": session_id},
                {
                    "$set": {
                        "sessions.$.total_files_count": len(files),
                        "sessions.$.total_files_size": total_size,
                        "sessions.$.total_files_size_human": f"{total_size / (1024*1024):.2f} MB",
                        "sessions.$.total_images": total_images,
                        "sessions.$.total_videos": total_videos,
                        "sessions.$.last_updated": datetime.now(timezone.utc)
                    }
                }
            )

            return True

        except Exception as e:
            logger.error(f"Error updating session totals: {str(e)}")
            return False 