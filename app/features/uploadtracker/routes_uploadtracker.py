"""
Upload Tracker Module

This module provides tracking and management of upload activities,
including file processing, content monitoring, and metrics.

Features:
- Upload activity tracking
- Media details management
- Content notes handling
- Post activity monitoring
- Content flags management
- Story metrics tracking
- Content deletion

Data Model:
- Upload sessions
- Media metadata
- Content notes
- Post metrics
- Content flags
- Story metrics

Security:
- User group validation
- Partner filtering
- Access control
- Data validation

Dependencies:
- FastAPI for routing
- MongoDB for storage
- AWS S3 for file storage
- datetime for timestamps
- logging for tracking

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from datetime import datetime, timedelta
import logging
from app.shared.database import (
    upload_collection, 
    async_client, 
    DB_NAME, 
    notes_collection, 
    content_dump_collection, 
    queue_collection, 
    spotlight_collection,
    notif_db,
    saved_collection
)
from fastapi.responses import JSONResponse
from typing import List
from app.shared.auth import get_current_user_group, filter_by_partner
import re
import boto3
from botocore.exceptions import ClientError
import os

router = APIRouter(prefix="/api/uploadapp", tags=["uploadapp"])
logger = logging.getLogger(__name__)

def format_minutes(seconds):
    """
    Format seconds into MM:SS format.
    
    Args:
        seconds: Time in seconds (int, float, or str)
        
    Returns:
        str: Time in MM:SS format
        
    Notes:
        - Handles different input types
        - Returns '00:00' for invalid inputs
        - Logs conversion process
    """
    logger.info(f"format_minutes called with value: {seconds}, type: {type(seconds)}")
    if seconds is None:
        logger.info("Input was None, returning '00:00'")
        return "00:00"
    try:
        if isinstance(seconds, str):
            logger.info(f"Converting string '{seconds}' to float")
            seconds = float(seconds)
        
        # Convert to integer to ensure whole seconds
        seconds = int(seconds)
        
        # Calculate minutes and remaining seconds
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        
        # Format as MM:SS
        result = f"{minutes:02d}:{remaining_seconds:02d}"
        logger.info(f"Converted {seconds} seconds to {result}")
        return result
    except (ValueError, TypeError) as e:
        logger.error(f"Error formatting minutes: {e}, value was: {seconds}, type: {type(seconds)}")
        return "00:00"

@router.get("/upload-activity")
async def get_upload_activity(user_groups: List[str] = Depends(get_current_user_group)):
    """
    Get upload activity for all clients.
    
    Args:
        user_groups: List of user group identifiers
        
    Returns:
        dict: Upload activity data by client
        
    Notes:
        - Filters by user groups
        - Gets client names
        - Processes upload data
        - Formats video lengths
    """
    try:
        # Calculate date range
        today = datetime.now()
        start_date = today - timedelta(days=30)
        end_date = today + timedelta(days=7)
        
        # Get filter query based on user's groups
        filter_query = await filter_by_partner(user_groups)
        logger.info(f"Filter query: {filter_query}")
        
        # Get client names from ClientInfo collection first
        client_info = async_client[DB_NAME]['ClientInfo']
        client_names = {}
        accessible_clients = set()
        
        # Add group filter to client info query
        client_query = {}
        if filter_query and "client_id" in filter_query:
            client_query.update(filter_query)
            
        logger.info(f"Client query: {client_query}")
        
        async for client in client_info.find(client_query, {
            "client_id": 1, 
            "First_Legal_Name": 1, 
            "Last_Legal_Name": 1,
            "Preferred_Name": 1
        }):
            if client.get("client_id"):
                client_id = client["client_id"]
                accessible_clients.add(client_id)
                if client.get("Preferred_Name"):
                    client_names[client_id] = client["Preferred_Name"]
                else:
                    first_name = client.get("First_Legal_Name", "")
                    last_name = client.get("Last_Legal_Name", "")
                    if first_name or last_name:
                        client_names[client_id] = f"{first_name} {last_name}".strip()

        logger.info(f"Found accessible clients: {accessible_clients}")
        logger.info(f"Client names: {client_names}")

        # If we don't have a name, try getting it from Users collection
        users_collection = async_client[DB_NAME]['Users']
        users_query = {"client_id": {"$nin": list(client_names.keys())}}
        if filter_query and "client_id" in filter_query:
            users_query.update(filter_query)
            
        async for user in users_collection.find(users_query, {"client_id": 1, "name": 1}):
            if user.get("client_id") and user.get("name"):
                client_id = user["client_id"]
                client_names[client_id] = user["name"]
                accessible_clients.add(client_id)

        # Update filter query to use client_ID instead of client_id for uploads collection
        upload_filter = {}
        if filter_query and "client_id" in filter_query:
            upload_filter["client_ID"] = filter_query["client_id"]

        logger.info(f"Upload filter: {upload_filter}")
        
        logger.info("Using actual aggregated video length (all_video_length) for duration calculations")
        
        pipeline = [
            {
                "$match": upload_filter if upload_filter else {}
            },
            {
                "$unwind": "$sessions"
            },
            {
                "$group": {
                    "_id": {
                        "client_ID": "$client_ID",
                        "date": {
                            "$let": {
                                "vars": {
                                    "session_id": {"$ifNull": ["$sessions.session_id", ""]},
                                    "start_index": {"$indexOfBytes": [{"$ifNull": ["$sessions.session_id", ""]}, "F("]},
                                    "end_index": {"$indexOfBytes": [{"$ifNull": ["$sessions.session_id", ""]}, ")"]}
                                },
                                "in": {
                                    "$cond": {
                                        "if": {
                                            "$and": [
                                                {"$ne": ["$$start_index", -1]},
                                                {"$ne": ["$$end_index", -1]}
                                            ]
                                        },
                                        "then": {
                                            "$trim": {
                                                "input": {
                                                    "$substr": [
                                                        "$$session_id",
                                                        {"$add": ["$$start_index", 2]},
                                                        {"$subtract": ["$$end_index", {"$add": ["$$start_index", 2]}]}
                                                    ]
                                                }
                                            }
                                        },
                                        "else": ""
                                    }
                                }
                            }
                        }
                    },
                    "videoCount": {
                        "$sum": "$sessions.total_videos"
                    },
                    "imageCount": {
                        "$sum": "$sessions.total_images"
                    },
                    "videoLength": {
                        "$sum": "$sessions.all_video_length"
                    },
                    "session_data": {
                        "$push": {
                            "session_id": "$sessions.session_id",
                            "all_video_length": "$sessions.all_video_length",
                            "approved": "$sessions.approved"
                        }
                    }
                }
            },
            {
                "$match": {
                    "_id.date": {"$ne": ""}
                }
            },
            {
                "$group": {
                    "_id": "$_id.client_ID",
                    "clientName": {"$first": "$_id.client_ID"},
                    "uploads": {
                        "$push": {
                            "date": "$_id.date",
                            "stats": {
                                "videoCount": "$videoCount",
                                "imageCount": "$imageCount",
                                "videoMinutes": "$videoLength",
                                "hasContent": {
                                    "$gt": [{"$add": ["$videoCount", "$imageCount"]}, 0]
                                }
                            },
                            "sessions": "$session_data"
                        }
                    }
                }
            }
        ]
        
        results = await upload_collection.aggregate(pipeline).to_list(None)
        logger.info(f"Raw aggregation results: {results}")
        
        # Additional debug - check if all_video_length is being aggregated properly
        if results:
            logger.info(f"Number of clients with upload data: {len(results)}")
            sample_client = results[0]
            logger.info(f"Sample client ID: {sample_client.get('_id')}")
            
            # Log the session data to see each session's all_video_length
            if "sessions" in sample_client:
                logger.info(f"Session data for sample client: {sample_client['sessions']}")
            
            uploads_with_videos = [u for u in sample_client.get('uploads', []) 
                                if u.get('stats', {}).get('videoCount', 0) > 0]
            
            logger.info(f"Sample client has {len(uploads_with_videos)} uploads with videos")
            
            for i, upload in enumerate(uploads_with_videos[:3]):  # Log up to 3 samples
                logger.info(f"Sample upload {i+1}: date={upload.get('date')}, "
                           f"videoCount={upload.get('stats', {}).get('videoCount')}, "
                           f"videoLength={upload.get('stats', {}).get('videoMinutes')}, "
                           f"type={type(upload.get('stats', {}).get('videoMinutes'))}")
                
        # Create a map of existing results
        results_map = {result["clientName"]: result for result in results}
        logger.info(f"Results map: {results_map}")
        
        # Create final results list with all accessible clients
        final_results = []
        for client_id in accessible_clients:
            if client_id in results_map:
                result = results_map[client_id]
            else:
                # Create empty result for client with no uploads
                result = {
                    "_id": client_id,
                    "clientName": client_id,
                    "uploads": []
                }
            
            # Add client name
            result["clientName"] = client_names.get(client_id, f"Unknown Client ({client_id})")
            
            # Format existing uploads
            for upload in result["uploads"]:
                try:
                    date_str = upload["date"].strip()
                    try:
                        # Try parsing as "Jan 21, 2025" format first
                        parsed_date = datetime.strptime(date_str, "%b %d, %Y")
                    except ValueError:
                        # If that fails, try parsing as "01-21-2025" format
                        parsed_date = datetime.strptime(date_str, "%m-%d-%Y")
                    upload["date"] = parsed_date.strftime("%Y-%m-%d")
                except Exception as e:
                    logger.error(f"Error parsing date {upload['date']}: {str(e)}")
                
                # Log the raw video minutes value before formatting
                logger.info(f"Raw videoMinutes value: {upload['stats'].get('videoMinutes')}")
                
                upload["stats"]["videoMinutes"] = format_minutes(upload["stats"]["videoMinutes"])
                
                # Log the formatted video minutes value
                logger.info(f"Formatted videoMinutes value: {upload['stats']['videoMinutes']}")
                
                upload["stats"]["hasContent"] = upload["stats"]["videoCount"] > 0 or upload["stats"]["imageCount"] > 0
            
            final_results.append(result)
            
        # Log a sample of the final data being sent to frontend
        if final_results:
            logger.info(f"Sample final result for first client: {final_results[0]}")
            
        logger.info(f"Final results: {final_results}")
        return {"data": final_results}
        
    except Exception as e:
        logger.error(f"Error in get_upload_activity: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/upload-media-details/{client_id}/{date}")
async def get_media_details(client_id: str, date: str):
    """
    Get media details for a specific client and date.
    
    Args:
        client_id: Client identifier
        date: Date to fetch details for
        
    Returns:
        dict: Media file details
        
    Notes:
        - Handles content dumps
        - Formats video lengths
        - Processes session data
    """
    try:
        logger.info(f"Looking for client_id: {client_id} on date: {date}")
        
        # Check if this is a dump request
        is_dump = date.startswith("CONTENTDUMP_")
        
        if is_dump:
            # For dumps, use the content dump collection
            dump_doc = await content_dump_collection.find_one(
                {
                    "client_ID": client_id,
                    "sessions": {
                        "$elemMatch": {
                            "session_id": date,
                            "content_type": "content_dump"
                        }
                    }
                }
            )
            
            if dump_doc and dump_doc.get("sessions"):
                # Find the specific session
                session = next(
                    (s for s in dump_doc["sessions"] if s["session_id"] == date),
                    None
                )
                if session:
                    # Format video_length for each file
                    files = session.get("files", [])
                    for file in files:
                        if file.get("file_type") == "video" and file.get("video_length") is not None:
                            # Store the original value
                            original_length = file.get("video_length", 0)
                            # Format the video_length to MM:SS and replace original value
                            file["video_length"] = format_minutes(original_length)
                            logger.info(f"Formatted video length for {file.get('file_name')}: {original_length} -> {file['video_length']}")
                    return {"files": files}
            return {"files": []}
        
        # Regular story/spotlight handling
        raw_doc = await upload_collection.find_one({"client_ID": client_id})
        logger.info(f"Raw document found: {raw_doc}")
        
        # If date is already in folder ID format (F(...)), use it directly
        if date.startswith('F(') and date.endswith(f')_{client_id}'):
            folder_id = date
            folder_ids = [folder_id]
        else:
            # Try to parse the date and create folder IDs
            try:
                date_obj = datetime.strptime(date, "%Y-%m-%d")
                
                # Format 1: "F(Jan 6, 2025)_client_id"
                session_date = date_obj.strftime("%b %-d, %Y")
                folder_ids = [f"F({session_date})_{client_id}"]
                
                # Format 2: "F(01-06-2025)_client_id"
                alt_session_date = date_obj.strftime("%m-%d-%Y")
                folder_ids.append(f"F({alt_session_date})_{client_id}")
            except ValueError as e:
                logger.error(f"Error parsing date: {str(e)}")
                return JSONResponse(status_code=200, content={"files": []})
        
        logger.info(f"Looking for folder_ids: {folder_ids}")
        
        # Find document by client_ID and any of the folder_ids
        pipeline = [
            {
                "$match": {
                    "client_ID": client_id,
                    "sessions.folder_id": {"$in": folder_ids}
                }
            },
            {
                "$unwind": "$sessions"
            },
            {
                "$match": {
                    "sessions.folder_id": {"$in": folder_ids}
                }
            },
            {
                "$project": {
                    "files": "$sessions.files",
                    "folder_id": "$sessions.folder_id"
                }
            }
        ]
        
        result = await upload_collection.aggregate(pipeline).to_list(length=None)
        logger.info(f"Query result: {result}")
        
        if result and result[0].get('files'):
            files = result[0]['files']
            logger.info(f"Found {len(files)} files in folder {result[0].get('folder_id')}")
            
            # Format video_length for each file
            for file in files:
                if file.get("file_type") == "video" and file.get("video_length") is not None:
                    # Store the original value
                    original_length = file.get("video_length", 0)
                    # Format the video_length to MM:SS and replace original value
                    file["video_length"] = format_minutes(original_length)
                    logger.info(f"Formatted video length for {file.get('file_name')}: {original_length} -> {file['video_length']}")
            
            return {"files": files}
            
        logger.info("No files found in any format")
        return {"files": []}
        
    except Exception as e:
        logger.error(f"Error in get_media_details: {str(e)}")
        return JSONResponse(
            status_code=200,
            content={"files": []}
        )

@router.post("/content-notes/add")
async def add_content_note(note_data: dict):
    """
    Add a content note.
    
    Args:
        note_data: Note details and metadata
        
    Returns:
        dict: Note creation status
        
    Notes:
        - Validates input
        - Creates document structure
        - Handles session notes
    """
    try:
        client_id = note_data.get('client_ID')
        if not client_id:
            raise HTTPException(status_code=400, detail="client_ID is required")

        session_folder = note_data.get('session_folder')
        if not session_folder:
            raise HTTPException(status_code=400, detail="session_folder is required")

        # Sanitize session folder to prevent MongoDB injection
        session_folder = session_folder.replace('$', '').replace('.', '')

        # Create initial document structure if it doesn't exist
        existing_doc = await notes_collection.find_one({"client_ID": client_id})
        
        if not existing_doc:
            await notes_collection.insert_one({
                "client_ID": client_id,
                "sessions": {
                    session_folder: []
                }
            })
        elif not existing_doc.get('sessions', {}).get(session_folder):
            await notes_collection.update_one(
                {"client_ID": client_id},
                {"$set": {f"sessions.{session_folder}": []}}
            )

        # Create the note object
        note_object = {
            "file_name": note_data['file_data'].get('file_name'),
            "note": note_data['file_data'].get('note'),
            "created_at": note_data['file_data'].get('created_at'),
            "cdn_url": note_data['file_data'].get('cdn_url')  # Get cdn_url directly from the request
        }

        # If this is a session note (no file_name), use the files from the request
        if not note_data['file_data'].get('file_name'):
            note_object['session_files'] = note_data['file_data'].get('session_files', [])

        # Push the note to the array for this session
        update_result = await notes_collection.update_one(
            {"client_ID": client_id},
            {
                "$push": {
                    f"sessions.{session_folder}": note_object
                }
            }
        )

        if update_result.modified_count > 0:
            return {
                "status": "success", 
                "message": "Note saved successfully",
                "note": note_object
            }
        else:
            return {"status": "success", "message": "No changes were necessary"}

    except Exception as e:
        logger.error(f"Error adding note: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/content-notes/get/{client_id}/{session_folder}/{file_name}")
async def get_content_note(client_id: str, session_folder: str, file_name: str):
    try:
        note_doc = await notes_collection.find_one(
            {"client_ID": client_id},
            {f"sessions.{session_folder}": 1}
        )
        
        if not note_doc or session_folder not in note_doc.get("sessions", {}):
            return {"note": None}
            
        notes = note_doc["sessions"][session_folder]
        file_note = next((note for note in notes if note.get("file_name") == file_name), None)
        
        return {"note": file_note.get("note") if file_note else None}
        
    except Exception as e:
        logger.error(f"Error getting note: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/content-notes/delete-note/{client_id}/{session_folder}")
async def delete_session_note(client_id: str, session_folder: str, note_data: dict):
    try:
        # Find and delete the specific note based on timestamp and content
        result = await notes_collection.update_one(
            {"client_ID": client_id},
            {
                "$pull": {
                    f"sessions.{session_folder}": {
                        "created_at": note_data.get("created_at"),
                        "note": note_data.get("note")
                    }
                }
            }
        )
        
        if result.modified_count > 0:
            return {"status": "success", "message": "Note deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Note not found")
            
    except Exception as e:
        logger.error(f"Error deleting note: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/content-notes/get/{client_id}/{session_folder}")
async def get_session_notes(client_id: str, session_folder: str):
    try:
        note_doc = await notes_collection.find_one(
            {"client_ID": client_id},
            {f"sessions.{session_folder}": 1}
        )
        
        if not note_doc or session_folder not in note_doc.get("sessions", {}):
            return {"notes": []}
            
        notes = note_doc["sessions"][session_folder]
        return {"notes": notes}
        
    except Exception as e:
        logger.error(f"Error getting session notes: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/content-dumps")
async def get_content_dumps(user_groups: List[str] = Depends(get_current_user_group)):
    try:
        filter_query = await filter_by_partner(user_groups)
        logger.info(f"Filter query: {filter_query}")
        
        # Get client names from ClientInfo collection first
        client_info = async_client[DB_NAME]['ClientInfo']
        client_names = {}
        accessible_clients = set()
        
        # Add group filter to client info query
        client_query = {}
        if filter_query and "client_id" in filter_query:
            client_query.update(filter_query)
            
        async for client in client_info.find(client_query, {
            "client_id": 1, 
            "First_Legal_Name": 1, 
            "Last_Legal_Name": 1,
            "Preferred_Name": 1
        }):
            if client.get("client_id"):
                client_id = client["client_id"]
                accessible_clients.add(client_id)
                if client.get("Preferred_Name"):
                    client_names[client_id] = client["Preferred_Name"]
                else:
                    first_name = client.get("First_Legal_Name", "")
                    last_name = client.get("Last_Legal_Name", "")
                    if first_name or last_name:
                        client_names[client_id] = f"{first_name} {last_name}".strip()

        logger.info(f"Accessible clients: {accessible_clients}")
        logger.info(f"Client names: {client_names}")

        # Basic query for Content_Dump collection
        query = {}
        if filter_query and "client_id" in filter_query:
            query["client_ID"] = filter_query["client_id"]

        logger.info(f"Content_Dump query: {query}")

        # Use the predefined collection
        dumps = await content_dump_collection.find(query).to_list(None)
        logger.info(f"Found {len(dumps)} content dumps")
        
        # Format results with client names
        final_results = []
        for client_id in accessible_clients:
            raw_dump = next((d for d in dumps if d["client_ID"] == client_id), None)
            logger.info(f"Processing client {client_id}, found dump: {bool(raw_dump)}")
            
            result = {
                "_id": client_id,
                "clientName": client_names.get(client_id, f"Unknown Client ({client_id})"),
                "dumps": []
            }
            
            if raw_dump and raw_dump.get("sessions"):
                logger.info(f"Found {len(raw_dump['sessions'])} sessions for client {client_id}")
                for session in raw_dump["sessions"]:
                    try:
                        # Handle both date formats: MM-DD-YYYY and YYYY-MM-DD
                        scan_date = session.get("scan_date", "")
                        if not scan_date:
                            continue
                            
                        try:
                            # Try parsing as MM-DD-YYYY first
                            date_obj = datetime.strptime(scan_date, "%m-%d-%Y")
                        except ValueError:
                            try:
                                # Try parsing as YYYY-MM-DD
                                date_obj = datetime.strptime(scan_date, "%Y-%m-%d")
                            except ValueError:
                                logger.error(f"Invalid date format: {scan_date}")
                                continue
                        
                        formatted_date = date_obj.strftime("%Y-%m-%d")
                        
                        result["dumps"].append({
                            "date": formatted_date,
                            "stats": {
                                "totalFiles": session.get("total_files_count", 0),
                                "totalSize": session.get("total_files_size_human", "0 MB")
                            }
                        })
                    except Exception as e:
                        logger.error(f"Error processing session for client {client_id}: {str(e)}")
                        logger.error(f"Session data: {session}")
                        continue
            
            final_results.append(result)

        logger.info(f"Returning {len(final_results)} results")
        if final_results:
            logger.info(f"Sample result: {final_results[0]}")

        return {"status": "success", "data": final_results}
        
    except Exception as e:
        logger.error(f"Error in get_content_dumps: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/post-activity")
async def get_post_activity(user_groups: List[str] = Depends(get_current_user_group)):
    """Get post activity for all clients"""
    try:
        # Calculate date range
        today = datetime.now()
        start_date = today - timedelta(days=30)
        end_date = today + timedelta(days=7)
        
        # Get filter query based on user's groups
        filter_query = await filter_by_partner(user_groups)
        logger.info(f"Filter query: {filter_query}")
        
        # Get client names from ClientInfo collection first
        client_info = async_client[DB_NAME]['ClientInfo']
        client_names = {}
        accessible_clients = set()
        
        # Add group filter to client info query
        client_query = {}
        if filter_query and "client_id" in filter_query:
            client_query.update(filter_query)
            
        logger.info(f"Client query: {client_query}")
        
        async for client in client_info.find(client_query, {
            "client_id": 1, 
            "First_Legal_Name": 1, 
            "Last_Legal_Name": 1,
            "Preferred_Name": 1
        }):
            if client.get("client_id"):
                client_id = client["client_id"]
                accessible_clients.add(client_id)
                if client.get("Preferred_Name"):
                    client_names[client_id] = client["Preferred_Name"]
                else:
                    first_name = client.get("First_Legal_Name", "")
                    last_name = client.get("Last_Legal_Name", "")
                    if first_name or last_name:
                        client_names[client_id] = f"{first_name} {last_name}".strip()

        # If we don't have a name, try getting it from Users collection
        users_collection = async_client[DB_NAME]['Users']
        users_query = {"client_id": {"$nin": list(client_names.keys())}}
        if filter_query and "client_id" in filter_query:
            users_query.update(filter_query)
            
        async for user in users_collection.find(users_query, {"client_id": 1, "name": 1}):
            if user.get("client_id") and user.get("name"):
                client_id = user["client_id"]
                client_names[client_id] = user["name"]
                accessible_clients.add(client_id)

        # Query queue collection for date range
        queue_query = {
            "queue_date": {
                "$gte": start_date.strftime("%Y-%m-%d"),
                "$lte": end_date.strftime("%Y-%m-%d")
            }
        }
        
        # Add client filter if exists
        if filter_query and "client_id" in filter_query:
            queue_query["client_queues." + filter_query["client_id"]] = {"$exists": True}
            
        # Use the imported queue_collection directly
        queue_results = await queue_collection.find(queue_query).to_list(None)
        
        # Process queue results by client and date
        client_activity = {}
        for queue_doc in queue_results:
            queue_date = queue_doc["queue_date"]
            
            for client_id, client_data in queue_doc.get("client_queues", {}).items():
                if client_id not in accessible_clients:
                    continue
                    
                if client_id not in client_activity:
                    client_activity[client_id] = {
                        "_id": client_id,
                        "clientName": client_names.get(client_id, f"Unknown Client ({client_id})"),
                        "client_id": client_id,
                        "posts": []
                    }
                
                # Count stories as posts
                post_count = len(client_data.get("stories", []))
                
                client_activity[client_id]["posts"].append({
                    "date": queue_date,
                    "stats": {
                        "postCount": post_count,
                        "commentCount": 0,  # Not available in queue data
                        "reactionCount": 0,  # Not available in queue data
                        "hasActivity": post_count > 0
                    }
                })
        
        final_results = list(client_activity.values())
        
        # Add empty results for clients with no activity
        for client_id in accessible_clients:
            if client_id not in client_activity:
                client_activity[client_id] = {
                    "_id": client_id,
                    "clientName": client_names.get(client_id, f"Unknown Client ({client_id})"),
                    "client_id": client_id,
                    "posts": []
                }
        
        logger.info(f"Returning {len(final_results)} results")
        return {"data": final_results}
        
    except Exception as e:
        logger.error(f"Error in get_post_activity: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/post-activity/details/{client_id}/{date}")
async def get_post_activity_details(client_id: str, date: str):
    """Get queue data for a specific client and date"""
    try:
        # Add debug logging
        logger.info(f"Fetching post activity details for client_id: {client_id}, date: {date}")
        
        # Get the queue document for this date
        queue_doc = await queue_collection.find_one({
            "queue_date": date,
            f"client_queues.{client_id}": {"$exists": True}
        })
        
        if not queue_doc:
            logger.info(f"No queue document found for date {date}")
            return {"client_queues": {
                client_id: {
                    "stories": []
                }
            }}
            
        # Log what we found
        logger.info(f"Found queue document with keys: {queue_doc.keys()}")
        
        # Get the client's queue data
        client_data = queue_doc.get("client_queues", {}).get(client_id, {})
        
        # Log the client data structure
        logger.info(f"Client queue data structure: {client_data.keys() if client_data else 'No client data'}")
        
        if "stories" in client_data:
            logger.info(f"Found {len(client_data['stories'])} stories")
            for story in client_data["stories"]:
                logger.info(f"Story data structure: {story.keys() if story else 'Empty story'}")
                
        return {
            "client_queues": {
                client_id: client_data
            }
        }
        
    except Exception as e:
        # Add more detailed error logging
        logger.error(f"Error getting queue data: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=200,  # Return 200 with empty data instead of 500
            content={
                "client_queues": {
                    client_id: {
                        "stories": []
                    }
                }
            }
        )

@router.post("/content-notes/approve")
async def approve_content(approval_data: dict):
    try:
        client_id = approval_data.get("client_ID")
        session_folder = approval_data.get("session_folder")
        approved_by = approval_data.get("approved_by")
        files = approval_data.get("files", [])
        
        if not client_id or not session_folder:
            raise HTTPException(status_code=400, detail="Missing required fields")
        
        logger.info(f"Approving content for client_ID: {client_id}, session_folder: {session_folder}")
        
        # Update the approval status for the specific session
        update_data = {
            "sessions.$.approved": True,
            "sessions.$.approved_by": approved_by,
            "sessions.$.approved_at": datetime.utcnow().isoformat()
        }

        # Update all matching documents
        result = await upload_collection.update_one(
            {
                "client_ID": client_id,
                "sessions.session_id": session_folder  # Changed from folder_id to sessions.session_id
            },
            {"$set": update_data}
        )
        
        logger.info(f"Approval result: {result.matched_count} matched, {result.modified_count} modified")
        
        if result.matched_count == 0:
            logger.warning(f"No documents matched the update criteria")
            return {
                "status": "warning",
                "message": "No matching documents found to approve",
                "matched_count": 0,
                "modified_count": 0
            }
            
        return {
            "status": "success",
            "message": "Content approved successfully",
            "matched_count": result.matched_count,
            "modified_count": result.modified_count
        }
            
    except Exception as e:
        logger.error(f"Error approving content: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/spotlights")
async def get_spotlights(user_groups: List[str] = Depends(get_current_user_group)):
    try:
        filter_query = await filter_by_partner(user_groups)
        logger.info(f"Filter query: {filter_query}")
        
        # Get client names from ClientInfo collection
        client_info = async_client[DB_NAME]['ClientInfo']
        client_names = {}
        accessible_clients = set()
        
        # Add group filter to client info query
        client_query = {}
        if filter_query and "client_id" in filter_query:
            client_query.update(filter_query)
            
        async for client in client_info.find(client_query):
            if client.get("client_id"):
                client_id = client["client_id"]
                accessible_clients.add(client_id)
                if client.get("Preferred_Name"):
                    client_names[client_id] = client["Preferred_Name"]
                else:
                    first_name = client.get("First_Legal_Name", "")
                    last_name = client.get("Last_Legal_Name", "")
                    if first_name or last_name:
                        client_names[client_id] = f"{first_name} {last_name}".strip()

        # Query for spotlight content
        query = {}
        if filter_query and "client_id" in filter_query:
            query["client_ID"] = filter_query["client_id"]

        # Add content type filter for spotlights
        query["sessions.content_type"] = "SPOTLIGHT"

        spotlight_data = await spotlight_collection.find(query).to_list(None)
        
        # Format results
        final_results = []
        for client_id in accessible_clients:
            client_data = next((d for d in spotlight_data if d["client_ID"] == client_id), None)
            
            result = {
                "_id": client_id,
                "clientName": client_names.get(client_id, f"Unknown Client ({client_id})"),
                "uploads": []
            }
            
            if client_data:
                for session in client_data.get("sessions", []):
                    if session.get("content_type") == "SPOTLIGHT":
                        try:
                            date_str = session["session_id"].split("(")[1].split(")")[0]
                            date_obj = datetime.strptime(date_str, "%m-%d-%Y")
                            
                            result["uploads"].append({
                                "date": date_obj.strftime("%Y-%m-%d"),
                                "stats": {
                                    "videoCount": session.get("total_videos", 0),
                                    "imageCount": session.get("total_images", 0),
                                    "videoMinutes": format_minutes(session.get("all_video_length", 0)),
                                    "hasContent": session.get("total_videos", 0) > 0 or session.get("total_images", 0) > 0
                                }
                            })
                        except Exception as e:
                            logger.error(f"Error processing session for client {client_id}: {str(e)}")
                            continue
            
            final_results.append(result)

        return {"status": "success", "data": final_results}
        
    except Exception as e:
        logger.error(f"Error in get_spotlights: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/spotlight-details/{client_id}/{date}")
async def get_spotlight_details(client_id: str, date: str):
    """Get spotlight media details for a specific client and date"""
    try:
        # Convert date from YYYY-MM-DD to MM-DD-YYYY
        year, month, day = date.split('-')
        formatted_date = f"{month}-{day}-{year}"
        
        # Format the session ID with converted date
        session_id = f"F({formatted_date})_{client_id}"
        
        logger.info(f"Looking for session: {session_id}")
        logger.info(f"Date conversion: {date} -> {formatted_date}")
        
        # Query the spotlight collection
        spotlight_doc = await spotlight_collection.find_one({
            "client_ID": client_id,
            "sessions": {
                "$elemMatch": {
                    "session_id": session_id,
                    "content_type": "SPOTLIGHT"
                }
            }
        })
        
        if not spotlight_doc:
            logger.info(f"No spotlight document found for {client_id} on {formatted_date}")
            return {"status": "success", "files": []}  # Return empty array instead of 404
            
        # Find the specific session
        session = next(
            (s for s in spotlight_doc.get("sessions", []) 
             if s.get("session_id") == session_id and s.get("content_type") == "SPOTLIGHT"),
            None
        )
        
        if not session:
            logger.info(f"No spotlight session found for {session_id}")
            return {"status": "success", "files": []}  # Return empty array instead of 404
            
        logger.info(f"Found session with {len(session.get('files', []))} files")
        
        # Format video_length for each file
        files = session.get("files", [])
        for file in files:
            if file.get("file_type") == "video" and file.get("video_length") is not None:
                # Store the original value
                original_length = file.get("video_length", 0)
                # Format the video_length to MM:SS and replace original value
                file["video_length"] = format_minutes(original_length)
                logger.info(f"Formatted video length for {file.get('file_name')}: {original_length} -> {file['video_length']}")
        
        return {
            "status": "success",
            "files": files
        }
        
    except Exception as e:
        logger.error(f"Error getting spotlight details: {str(e)}")
        return {"status": "error", "files": [], "error": str(e)}  # Return error in response instead of raising

@router.post("/update-editor-note")
async def update_editor_note(note_data: dict):
    """Add/Update editor note"""
    try:
        client_id = note_data.get('client_ID')
        session_folder = note_data.get('session_folder')
        file_data = note_data.get('file_data')

        if not all([client_id, session_folder, file_data]):
            raise HTTPException(status_code=400, detail="Missing required fields")

        # Create initial document structure if it doesn't exist
        existing_doc = await notif_db.EditNotes.find_one({"client_ID": client_id})
        
        if not existing_doc:
            await notif_db.EditNotes.insert_one({
                "client_ID": client_id,
                "sessions": {
                    session_folder: []
                }
            })
        elif not existing_doc.get('sessions', {}).get(session_folder):
            await notif_db.EditNotes.update_one(
                {"client_ID": client_id},
                {"$set": {f"sessions.{session_folder}": []}}
            )

        # Push the note to the array for this session
        await notif_db.EditNotes.update_one(
            {"client_ID": client_id},
            {
                "$push": {
                    f"sessions.{session_folder}": file_data
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
        # Find all sessions with editor notes for this client
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
        
        notes = await notif_db.EditNotes.aggregate(pipeline).to_list(None)
        if not notes:
            return {"notes": []}
            
        # Format notes from all sessions
        formatted_notes = []
        for doc in notes:
            for folder_id, notes_array in doc.get('sessions', {}).items():
                for note in notes_array:
                    formatted_notes.append({
                        "folder_id": folder_id,
                        "note": note.get('note'),
                        "created_at": note.get('created_at')
                    })
                    
        return {"notes": formatted_notes}
        
    except Exception as e:
        logger.error(f"Error getting editor notes: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/saved-activity")
async def get_saved_activity(user_groups: List[str] = Depends(get_current_user_group)):
    try:
        # Get filter query based on user's groups
        filter_query = await filter_by_partner(user_groups)
        logger.info(f"Filter query: {filter_query}")
        
        # Get client names from ClientInfo collection first
        client_info = async_client[DB_NAME]['ClientInfo']
        client_names = {}
        accessible_clients = set()
        
        # Add group filter to client info query
        client_query = {}
        if filter_query and "client_id" in filter_query:
            client_query.update(filter_query)
            
        async for client in client_info.find(client_query, {
            "client_id": 1, 
            "First_Legal_Name": 1, 
            "Last_Legal_Name": 1,
            "Preferred_Name": 1
        }):
            if client.get("client_id"):
                client_id = client["client_id"]
                accessible_clients.add(client_id)
                if client.get("Preferred_Name"):
                    client_names[client_id] = client["Preferred_Name"]
                else:
                    first_name = client.get("First_Legal_Name", "")
                    last_name = client.get("Last_Legal_Name", "")
                    if first_name or last_name:
                        client_names[client_id] = f"{first_name} {last_name}".strip()

        # Query for saved content
        pipeline = [
            {
                "$match": filter_query if filter_query else {}
            },
            {
                "$unwind": "$sessions"
            },
            {
                "$match": {
                    "sessions.content_type": "SAVED"
                }
            },
            {
                "$group": {
                    "_id": {
                        "client_ID": "$client_ID",
                        "date": "$sessions.scan_date"
                    },
                    "videoCount": {
                        "$sum": "$sessions.total_videos"
                    },
                    "imageCount": {
                        "$sum": "$sessions.total_images"
                    },
                    "videoLength": {
                        "$sum": "$sessions.all_video_length"
                    }
                }
            },
            {
                "$group": {
                    "_id": "$_id.client_ID",
                    "clientName": {"$first": "$_id.client_ID"},
                    "uploads": {
                        "$push": {
                            "date": "$_id.date",
                            "stats": {
                                "videoCount": "$videoCount",
                                "imageCount": "$imageCount",
                                "videoMinutes": "$videoLength",
                                "hasContent": {
                                    "$gt": [{"$add": ["$videoCount", "$imageCount"]}, 0]
                                }
                            }
                        }
                    }
                }
            }
        ]
        
        results = await saved_collection.aggregate(pipeline).to_list(None)
        
        # Format results
        final_results = []
        for client_id in accessible_clients:
            result = next((r for r in results if r["_id"] == client_id), {
                "_id": client_id,
                "clientName": client_id,
                "uploads": []
            })
            
            # Add client name
            result["clientName"] = client_names.get(client_id, f"Unknown Client ({client_id})")
            
            # Format dates and stats
            formatted_uploads = []
            for upload in result.get("uploads", []):
                try:
                    # Convert MM-DD-YYYY to YYYY-MM-DD
                    date_parts = upload["date"].split("-")
                    if len(date_parts) == 3:
                        formatted_date = f"{date_parts[2]}-{date_parts[0]}-{date_parts[1]}"
                        
                        formatted_uploads.append({
                            "date": formatted_date,
                            "stats": {
                                "videoCount": upload["stats"]["videoCount"],
                                "imageCount": upload["stats"]["imageCount"],
                                "videoMinutes": format_minutes(upload["stats"]["videoMinutes"]),
                                "hasContent": upload["stats"]["hasContent"]
                            }
                        })
                except Exception as e:
                    logger.error(f"Error formatting date {upload.get('date')}: {str(e)}")
                    continue
            
            result["uploads"] = formatted_uploads
            final_results.append(result)
            
        logger.info(f"Returning {len(final_results)} results with saved content")
        return {"data": final_results}
        
    except Exception as e:
        logger.error(f"Error in get_saved_activity: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/saved-media-details/{client_id}/{date}")
async def get_saved_media_details(client_id: str, date: str):
    try:
        # Format the session ID
        session_id = date  # date is already in F(MM-DD-YYYY)_clientId format
        
        logger.info(f"Looking for saved content with session_id: {session_id}")
        
        # Find document by client_ID and session_id
        pipeline = [
            {
                "$match": {
                    "client_ID": client_id,
                    "sessions": {
                        "$elemMatch": {
                            "session_id": session_id,
                            "content_type": "SAVED"
                        }
                    }
                }
            },
            {
                "$unwind": "$sessions"
            },
            {
                "$match": {
                    "sessions.session_id": session_id,
                    "sessions.content_type": "SAVED"
                }
            },
            {
                "$project": {
                    "files": "$sessions.files"
                }
            }
        ]
        
        logger.info(f"Running pipeline: {pipeline}")
        
        result = await saved_collection.aggregate(pipeline).to_list(None)
        logger.info(f"Query result: {result}")
        
        if result and result[0].get('files'):
            files = result[0]['files']
            logger.info(f"Found {len(files)} files")
            
            # Format video_length for each file
            for file in files:
                if file.get("file_type") == "video" and file.get("video_length") is not None:
                    # Store the original value
                    original_length = file.get("video_length", 0)
                    # Format the video_length to MM:SS and replace original value
                    file["video_length"] = format_minutes(original_length)
                    logger.info(f"Formatted video length for {file.get('file_name')}: {original_length} -> {file['video_length']}")
            
            return {"files": files}
            
        logger.info("No files found")
        return {"files": []}
        
    except Exception as e:
        logger.error(f"Error in get_saved_media_details: {str(e)}")
        return JSONResponse(status_code=200, content={"files": []})

@router.get("/content-flags/{client_id}")
async def get_content_flags(client_id: str):
    """Get content flags for a specific client's uploads"""
    try:
        # Updated query to look for content_matches
        uploads = await upload_collection.find(
            {
                "client_ID": client_id,
                "sessions.files.content_matches": {"$exists": True}
            },
            {
                "sessions.folder_id": 1,
                "sessions.session_id": 1,
                "sessions.files.file_name": 1,
                "sessions.files.content_matches": 1,
                "sessions.files.CDN_link": 1
            }
        ).to_list(None)

        formatted_results = []
        for upload in uploads:
            for session in upload.get("sessions", []):
                for file in session.get("files", []):
                    if file.get("content_matches"):  # Changed from flagged to content_matches
                        formatted_results.append({
                            "folder_id": session.get("folder_id"),
                            "session_id": session.get("session_id"),
                            "file_name": file.get("file_name"),
                            "cdn_url": file.get("CDN_link"),
                            "flags": [{
                                "type": flag.get("search_query"),
                                "score": flag.get("score"),
                                "confidence": flag.get("confidence"),
                                "severity": flag.get("severity"),
                                "start_time": flag.get("start_time"),
                                "end_time": flag.get("end_time"),
                                "detected_at": flag.get("search_date"),
                                "thumbnail": flag.get("thumbnail_url")
                            } for flag in file.get("content_matches", [])]
                        })

        return {
            "status": "success",
            "data": formatted_results
        }

    except Exception as e:
        logger.error(f"Error getting content flags: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/content-flags/{client_id}/{folder_id}/{file_name}/status")
async def update_flag_status(
    client_id: str, 
    folder_id: str, 
    file_name: str,
    action: str = Query(..., enum=["remove", "reinstate"])
):
    """Update flag status for a specific file"""
    try:
        # Find the document and update the specific file's flag status
        if action == "remove":
            # Add content_matches_status:"removed" to the specific file in the session
            result = await upload_collection.update_one(
                {
                    "client_ID": client_id,
                    "sessions": {
                        "$elemMatch": {
                            "folder_id": folder_id,
                            "files.file_name": file_name
                        }
                    }
                },
                {
                    "$set": {
                        "sessions.$[session].files.$[file].content_matches_status": "removed"
                    }
                },
                array_filters=[
                    {"session.folder_id": folder_id},
                    {"file.file_name": file_name}
                ]
            )
        else:  # reinstate
            # Remove the content_matches_status field from the specific file
            result = await upload_collection.update_one(
                {
                    "client_ID": client_id,
                    "sessions": {
                        "$elemMatch": {
                            "folder_id": folder_id,
                            "files.file_name": file_name
                        }
                    }
                },
                {
                    "$unset": {
                        "sessions.$[session].files.$[file].content_matches_status": ""
                    }
                },
                array_filters=[
                    {"session.folder_id": folder_id},
                    {"file.file_name": file_name}
                ]
            )

        if result.modified_count == 0:
            raise HTTPException(
                status_code=404,
                detail="File not found or status not updated"
            )

        return {
            "status": "success",
            "message": f"Flag {action}d successfully",
            "file_name": file_name
        }

    except Exception as e:
        logger.error(f"Error updating flag status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/story-metrics")
async def get_story_metrics():
    """Get story metrics for grid display"""
    try:
        content_data = async_client['ClientDb']['content_data']
        pipeline = [
            {
                "$match": {
                    "platform": "snapchat",
                    "sessions": {"$exists": True, "$ne": []},
                    "user_id": {"$exists": True}  # Make sure we have user_id
                }
            },
            {
                "$project": {
                    "user_id": 1,  # Include user_id in projection
                    "snap_profile_name": 1,
                    "sessions": {
                        "$filter": {
                            "input": "$sessions",
                            "as": "session",
                            "cond": {
                                "$gte": [
                                    {"$dateFromString": {"dateString": "$$session.date", "format": "%m-%d-%Y"}},
                                    {"$dateSubtract": {"startDate": "$$NOW", "unit": "day", "amount": 30}}
                                ]
                            }
                        }
                    }
                }
            },
            {
                "$unwind": "$sessions"
            },
            {
                "$group": {
                    "_id": {
                        "profile": "$user_id",  # Group by user_id instead of snap_profile_name
                        "date": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": {"$dateFromString": {"dateString": "$sessions.date", "format": "%m-%d-%Y"}}
                            }
                        }
                    },
                    "views": {"$first": "$sessions.metrics.views.story_views"}
                }
            },
            {
                "$group": {
                    "_id": "$_id.profile",  # This will now be the user_id
                    "uploads": {
                        "$push": {
                            "date": "$_id.date",
                            "views": "$views"
                        }
                    }
                }
            }
        ]

        results = await content_data.aggregate(pipeline).to_list(None)

        return {
            "status": "success",
            "data": results
        }

    except Exception as e:
        logger.error(f"Error in get_story_metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/delete-content/{client_id}/{session_folder}")
async def delete_content(client_id: str, session_folder: str):
    """Delete content from both AWS S3 and MongoDB for a specific session"""
    try:
        logger.info(f"Attempting to delete content for client {client_id}, session {session_folder}")
        
        # First, get the session data to find all files that need to be deleted
        session_data = await upload_collection.find_one(
            {
                "client_ID": client_id,
                "sessions": {
                    "$elemMatch": {
                        "session_id": session_folder
                    }
                }
            }
        )

        if not session_data:
            logger.warning(f"No session data found for {client_id}/{session_folder}")
            raise HTTPException(status_code=404, detail="Session not found")

        # Find the specific session
        session = next(
            (s for s in session_data.get("sessions", []) if s.get("session_id") == session_folder),
            None
        )

        if not session:
            logger.warning(f"Session {session_folder} not found in document")
            raise HTTPException(status_code=404, detail="Session not found")

        # Initialize S3 client with proper configuration
        bucket_name = os.getenv('AWS_S3_BUCKET', 'snapped2')
        region = os.getenv('AWS_REGION', 'us-east-2')
        s3_client = boto3.client('s3', region_name=region)

        # Delete files from S3
        deletion_errors = []
        for file in session.get("files", []):
            if cdn_url := file.get("CDN_link"):
                try:
                    # Extract the S3 key from the CDN URL
                    # Example: https://c.snapped.cc/public/clientid/STO/date/file.jpg
                    # We want: public/clientid/STO/date/file.jpg
                    s3_key = cdn_url.split("snapped.cc/")[1]
                    
                    # Delete the file from S3
                    logger.info(f"Deleting file from S3: {s3_key}")
                    try:
                        s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
                        logger.info(f"Successfully deleted {s3_key} from bucket {bucket_name}")
                    except ClientError as e:
                        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                        error_msg = f"Failed to delete {s3_key} from S3 ({error_code}): {str(e)}"
                        logger.error(error_msg)
                        deletion_errors.append(error_msg)
                except Exception as e:
                    error_msg = f"Error processing file {cdn_url}: {str(e)}"
                    logger.error(error_msg)
                    deletion_errors.append(error_msg)

        # Delete from MongoDB collections
        try:
            # Delete from uploads collection
            upload_result = await upload_collection.update_one(
                {"client_ID": client_id},
                {"$pull": {"sessions": {"session_id": session_folder}}}
            )
            logger.info(f"Removed session from uploads collection: {upload_result.modified_count} modified")

            # Delete from notes collection
            notes_result = await notes_collection.update_one(
                {"client_ID": client_id},
                {"$unset": {f"sessions.{session_folder}": ""}}
            )
            logger.info(f"Removed notes for session: {notes_result.modified_count} modified")

            # Delete from editor notes collection
            editor_notes_result = await notif_db.EditNotes.update_one(
                {"client_ID": client_id},
                {"$unset": {f"sessions.{session_folder}": ""}}
            )
            logger.info(f"Removed editor notes for session: {editor_notes_result.modified_count} modified")

        except Exception as e:
            error_msg = f"Error deleting MongoDB data: {str(e)}"
            logger.error(error_msg)
            deletion_errors.append(error_msg)

        # Return appropriate response
        if deletion_errors:
            return {
                "status": "partial_success",
                "message": "Content deleted with some errors",
                "errors": deletion_errors,
                "s3_errors": deletion_errors  # Added for frontend handling
            }
        else:
            return {
                "status": "success",
                "message": "Content deleted successfully"
            }

    except Exception as e:
        logger.error(f"Error in delete_content: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))