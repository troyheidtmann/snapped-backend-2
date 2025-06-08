"""
Desktop Upload API Routes - File Upload and Management Endpoints

This module provides FastAPI routes for handling desktop client file uploads,
managing upload sessions, tracking progress, and controlling access to user
directories in the Snapped platform.

Features:
--------
1. File Upload Management:
   - Multi-file upload support (up to 200 files)
   - Progress tracking
   - Session management
   - Date validation for Stories

2. Access Control:
   - Role-based permissions (Admin vs Regular users)
   - Partner-based filtering
   - User directory access control
   - Folder-level permissions

3. Directory Management:
   - User directory listing
   - Folder structure management
   - Default folder creation
   - Path validation

4. Progress Tracking:
   - File-level upload progress
   - Session status monitoring
   - Real-time updates

Security:
--------
- Authentication via FastAPI dependencies
- Role-based access control
- Partner-based filtering
- Input validation
- Error handling

Dependencies:
-----------
- FastAPI: Web framework and routing
- Pydantic: Request/response models
- MongoDB: User and permission management
- S3UploadManager: File upload handling

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException, Depends, Request, File, UploadFile, Form
from typing import List, Optional, Dict
from datetime import datetime
from .desktop_upload import UploadManager
from app.shared.auth import get_current_user_id, get_current_user_group, get_filtered_query, filter_by_partner
import logging
from pydantic import BaseModel
import os
import asyncio
import re
from pathlib import Path
from app.shared.database import async_client

# Add this new class for the request body
class UploadRequest(BaseModel):
    """
    Request model for file uploads.
    
    Attributes:
        files (List[str]): List of file paths to upload
        folder (Optional[str]): Target folder path
        date (Optional[str]): Date for Stories uploads
    """
    files: List[str]
    folder: Optional[str] = None
    date: Optional[str] = None

class SessionResponse(BaseModel):
    """
    Response model for session operations.
    
    Attributes:
        session_id (str): Unique session identifier
    """
    session_id: str

class RenameRequest(BaseModel):
    """
    Request model for file renaming operations.
    
    Attributes:
        session_id (str): Session identifier for the rename operation
    """
    session_id: str

class DirectoryResponse(BaseModel):
    """
    Response model for directory operations.
    
    Attributes:
        user_id (str): User identifier
        folders (List[Dict[str, str]]): List of folder information
    """
    user_id: str
    folders: List[Dict[str, str]]

# Create router without prefix
router = APIRouter()
upload_manager = UploadManager()
logger = logging.getLogger(__name__)

# Add OPTIONS method support
@router.options("/desktop-upload/upload")
async def upload_options():
    """
    Handle OPTIONS request for upload endpoint.
    
    Returns:
        dict: Available HTTP methods for the endpoint
    """
    return {"methods": ["POST"]}

@router.post("/desktop-upload/upload")
async def handle_upload(
    files: List[UploadFile] = File(...),
    user_id: str = Form(...),
    folder_path: str = Form(...),
    date: Optional[str] = Form(None),
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Handle file uploads to S3 with access control and validation.
    
    Args:
        files (List[UploadFile]): List of files to upload
        user_id (str): Target user identifier
        folder_path (str): Destination folder path
        date (Optional[str]): Date for Stories uploads
        auth_data (dict): User authentication data
        
    Returns:
        dict: Upload status and list of uploaded files
        
    Raises:
        HTTPException: For permission, validation, or upload errors
    """
    try:
        # Check admin status or permissions
        if "ADMIN" not in auth_data["groups"]:
            # For non-admin users, validate access to this client
            filter_query = await filter_by_partner(auth_data)
            
            # If we have a specific list of allowed clients
            if filter_query.get("client_id", {}).get("$in"):
                allowed_clients = filter_query["client_id"]["$in"]
                # Check if user has access to the selected user_id
                if user_id not in allowed_clients:
                    logger.warning(f"User {auth_data['user_id']} attempted unauthorized access to {user_id}")
                    raise HTTPException(
                        status_code=403,
                        detail="You don't have permission to upload files for this user"
                    )
        
        # Check file limit
        if len(files) > 200:
            raise HTTPException(
                status_code=400,
                detail="Maximum 200 files can be uploaded at once"
            )
            
        # Validate date for Stories uploads
        if folder_path.endswith('/STO/') and date:
            if not upload_manager.validate_story_date(date):
                raise HTTPException(
                    status_code=400,
                    detail="Invalid date selected for Stories upload"
                )
            
        # Upload files to S3
        uploaded_files = await upload_manager.handle_upload(
            files=files,
            user_id=user_id,
            folder=folder_path,
            date=date
        )
        
        return {
            "status": "success",
            "uploaded_files": uploaded_files
        }
        
    except Exception as e:
        logger.error(f"Error uploading files: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/desktop-upload/upload/status")
async def get_session_status(
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Get current upload session status.
    
    Args:
        auth_data (dict): User authentication data
        
    Returns:
        dict: Current session status information
    """
    return upload_manager.get_session_status()

@router.get("/progress/{file_name}")
async def get_upload_progress(
    file_name: str,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Get progress for a specific file upload.
    
    Args:
        file_name (str): Name of the file to check
        auth_data (dict): User authentication data
        
    Returns:
        dict: Upload progress information:
            - file_name: Name of the file
            - progress: Upload progress percentage
    """
    progress = upload_manager.upload_progress.get(file_name, 0)
    return {
        "file_name": file_name,
        "progress": progress
    }

@router.get("/desktop-upload/users")
async def list_users(
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Get list of authorized user directories.
    
    Args:
        auth_data (dict): User authentication data
        
    Returns:
        dict: List of accessible users with their details
        
    Raises:
        HTTPException: For permission or server errors
    """
    try:
        # Get filtered users based on permissions
        users = await upload_manager.get_accessible_users(auth_data)
        
        # Return the filtered users with proper formatting
        return {
            "status": "success",
            "users": users
        }
        
    except Exception as e:
        logger.error(f"Error listing users: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/desktop-upload/folders/{user_id}")
async def get_user_folders(
    user_id: str,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Get folders for a specific user with access control.
    
    Args:
        user_id (str): User identifier to get folders for
        auth_data (dict): User authentication data
        
    Returns:
        dict: List of accessible folders with paths and names
        
    Raises:
        HTTPException: For permission or server errors
    """
    try:
        # Check permissions for requested user_id
        if "ADMIN" not in auth_data["groups"]:
            # For non-admin users, validate access to this client
            filter_query = await filter_by_partner(auth_data)
            
            # If we have a specific list of allowed clients
            if filter_query.get("client_id", {}).get("$in"):
                allowed_clients = filter_query["client_id"]["$in"]
                # Check if user has access to the requested user_id
                if user_id not in allowed_clients:
                    logger.warning(f"User {auth_data['user_id']} attempted unauthorized access to {user_id}")
                    raise HTTPException(
                        status_code=403,
                        detail="You don't have permission to access this user's folders"
                    )
        
        # Get all content dump folders
        folders = await upload_manager.get_user_content_dump_folders(user_id)
        
        # Add default folders if none exist
        if not folders:
            folders = [
                {
                    "path": f"public/{user_id}/ALL/",
                    "name": "Content Dump"
                },
                {
                    "path": f"public/{user_id}/SPOT/",
                    "name": "Spotlight"
                }
            ]
        
        return {
            "status": "success",
            "folders": folders
        }
        
    except Exception as e:
        logger.error(f"Error getting folders for user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 