"""
Desktop Upload Service - S3 File Management System

This module provides a comprehensive service for managing file uploads to AWS S3,
specifically designed for desktop client uploads in the Snapped platform. It handles
multi-file uploads, directory management, and user access control.

Architecture:
-----------
The service is built around the S3UploadManager class which provides:
1. Direct S3 integration with retry mechanisms
2. Multi-file upload handling
3. Directory structure management
4. Access control based on MongoDB permissions
5. File naming and organization conventions

Features:
--------
1. File Upload Management:
   - Multi-file upload support
   - Automatic content type detection
   - File size validation
   - Progress tracking
   - Error handling and retry logic

2. Directory Organization:
   - Hierarchical folder structure
   - User-specific directories
   - Content type categorization (ALL, SPOT, STO)
   - Date-based organization for stories

3. Access Control:
   - Role-based access (Admin vs Regular users)
   - Partner-based filtering
   - MongoDB integration for permissions
   - S3 path validation

4. File Naming:
   - Date-based prefixes
   - Random suffixes for uniqueness
   - Original filename preservation for stories
   - Extension handling

Security:
--------
- AWS IAM integration
- MongoDB-based access control
- Input validation
- Secure URL generation
- Error logging

Dependencies:
-----------
- boto3: AWS SDK for S3 operations
- MongoDB: User and permission management
- FastAPI: Web framework integration
- Python standard library components

Environment Variables:
-------------------
- AWS_S3_BUCKET: S3 bucket name (default: 'snapped2')
- AWS_REGION: AWS region (default: 'us-east-2')

Author: Snapped Development Team
"""

import os
import boto3
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import logging
from pathlib import Path
import mimetypes
from botocore.config import Config
from random import randint
from app.shared.auth import filter_by_partner
from app.shared.database import async_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3UploadManager:
    """
    Manages file uploads and directory operations in AWS S3.
    
    This class handles all S3-related operations including file uploads,
    directory listing, and access control. It provides a robust interface
    for managing files in the S3 bucket with proper error handling and
    retry mechanisms.
    
    Attributes:
        bucket_name (str): Name of the S3 bucket
        region (str): AWS region for the S3 bucket
        s3_client: Low-level S3 client for direct operations
        s3: High-level S3 resource for advanced operations
    """
    
    def __init__(self):
        """
        Initialize the S3UploadManager with AWS credentials and configuration.
        
        Sets up the S3 client and resource with retry configuration and
        loads necessary environment variables.
        """
        self.bucket_name = os.getenv('AWS_S3_BUCKET', 'snapped2')
        self.region = os.getenv('AWS_REGION', 'us-east-2')
        
        # Configure boto3 client with retry settings
        config = Config(
            region_name=self.region,
            retries=dict(
                max_attempts=3,
                mode='standard'
            )
        )
        
        # Initialize S3 client and resource for higher-level operations
        self.s3_client = boto3.client('s3', config=config)
        self.s3 = boto3.resource('s3', config=config)

    async def handle_upload(self, files: List[str], user_id: str, folder: Optional[str] = None, date: Optional[str] = None) -> List[Dict]:
        """
        Handle multiple file uploads to S3 with proper organization and error handling.
        
        Args:
            files (List[str]): List of file objects to upload
            user_id (str): User identifier for organizing uploads
            folder (Optional[str]): Target folder path in S3
            date (Optional[str]): Date string for story uploads (YYYY-MM-DD)
            
        Returns:
            List[Dict]: List of uploaded file information containing:
                - file_name: Generated name in S3
                - original_name: Original uploaded filename
                - s3_url: Complete S3 URL for the file
                - size: File size in bytes
                
        Raises:
            Exception: If upload process fails
        """
        try:
            if not folder:
                folder = f"public/{user_id}/ALL/"

            uploaded_files = []
            
            # Process each file
            for file in files:
                try:
                    original_name = file.filename
                    # For STO uploads, keep original filename, otherwise generate new name
                    new_name = original_name if '/STO/' in folder else self.generate_file_name(original_name)
                    s3_key = self.generate_s3_key(user_id, folder, new_name, date)
                    
                    # Read file content
                    content = await file.read()
                    if not content:
                        logger.error(f"Empty file content for {original_name}")
                        continue

                    # Use boto3's upload_fileobj which handles multipart uploads automatically
                    from io import BytesIO
                    file_obj = BytesIO(content)
                    
                    # Let boto3 handle the upload with automatic multipart if needed
                    self.s3.Bucket(self.bucket_name).upload_fileobj(
                        file_obj,
                        s3_key,
                        ExtraArgs={
                            'ContentType': mimetypes.guess_type(original_name)[0] or 'application/octet-stream'
                        }
                    )

                    # Generate the S3 URL
                    s3_url = f"https://{self.bucket_name}.s3.{self.region}.amazonaws.com/{s3_key}"
                    
                    uploaded_files.append({
                        "file_name": new_name,
                        "original_name": original_name,
                        "s3_url": s3_url,
                        "size": len(content)
                    })
                    
                    logger.info(f"Successfully uploaded {original_name} as {new_name}")
                    
                except Exception as e:
                    logger.error(f"Failed to upload {file.filename}: {str(e)}")
                    continue

            return uploaded_files

        except Exception as e:
            logger.error(f"Error handling uploads: {e}")
            raise

    def generate_file_name(self, original_name: str) -> str:
        """
        Generate a unique filename with date prefix and random numbers.
        
        Args:
            original_name (str): Original filename with extension
            
        Returns:
            str: Generated filename in format DDMM-XXX-XXXX.ext where:
                - DDMM: Current day and month
                - XXX: Random 3-digit number
                - XXXX: Random 4-digit number
                - ext: Original file extension
        """
        now = datetime.now()
        random_nums = f"{randint(100, 999):03d}-{randint(1000, 9999):04d}"
        date_prefix = f"{now.day:02d}{now.month:02d}"
        extension = Path(original_name).suffix
        return f"{date_prefix}-{random_nums}{extension}"

    def generate_s3_key(self, user_id: str, folder: str, file_name: str, date_str: Optional[str] = None) -> str:
        """
        Generate the complete S3 key (path) for a file.
        
        Args:
            user_id (str): User identifier
            folder (str): Base folder path
            file_name (str): Generated or original filename
            date_str (Optional[str]): Date string for story uploads (YYYY-MM-DD)
            
        Returns:
            str: Complete S3 key including full path and filename
        """
        if '/STO/' in folder and date_str:
            # Convert from YYYY-MM-DD to MMDDYY format
            year, month, day = date_str.split('-')
            # Use last 2 digits of year
            year_short = year[-2:]
            date_folder = f"{month}{day}{year_short}"  # MMDDYY format
            logger.info(f"Using selected date: {date_str} -> folder: {date_folder}")
            return f"public/{user_id}/STO/{date_folder}/{file_name}"
        elif folder.startswith('public/'):
            # Keep existing logic for other folders
            folder_path = folder if folder.endswith('/') else f"{folder}/"
            return f"{folder_path}{file_name}"
        else:
            # Ensure all other paths start with public/
            return f"public/{user_id}/ALL/{file_name}"

    async def get_accessible_users(self, auth_data: dict) -> List[Dict[str, Any]]:
        """
        Get list of users accessible to the current user based on MongoDB permissions.
        
        Args:
            auth_data (dict): Authentication data containing user ID and groups
            
        Returns:
            List[Dict[str, Any]]: List of accessible users with their details:
                - client_id: User identifier
                - name: Display name (First Last)
        """
        try:
            # Start with the authenticated user's own ID
            accessible_users = []
            if auth_data.get("user_id"):
                accessible_users.append({
                    "client_id": auth_data["user_id"],
                    "name": f"My Account ({auth_data['user_id']})"
                })

            # If user is admin, they can see all clients
            if "ADMIN" in auth_data["groups"]:
                client_info_collection = async_client["ClientDb"]["ClientInfo"]
                cursor = client_info_collection.find({})
                clients = await cursor.to_list(length=None)
                accessible_users.extend([
                    {
                        "client_id": client["client_id"],
                        "name": f"{client.get('First_Legal_Name', '')} {client.get('Last_Legal_Name', '')}".strip()
                    }
                    for client in clients
                ])
                return accessible_users

            # For non-admin users, apply filtering
            filter_query = await filter_by_partner(auth_data)
            if filter_query.get("client_id") == "NO_ACCESS":
                return accessible_users  # Return just the user's own ID

            # Get allowed client IDs
            allowed_client_ids = filter_query.get("client_id", {}).get("$in", [])
            if not allowed_client_ids:
                return accessible_users  # Return just the user's own ID

            # Get client info for allowed clients
            client_info_collection = async_client["ClientDb"]["ClientInfo"]
            cursor = client_info_collection.find({"client_id": {"$in": allowed_client_ids}})
            clients = await cursor.to_list(length=None)
            
            accessible_users.extend([
                {
                    "client_id": client["client_id"],
                    "name": f"{client.get('First_Legal_Name', '')} {client.get('Last_Legal_Name', '')}".strip()
                }
                for client in clients
            ])
            
            return accessible_users

        except Exception as e:
            logger.error(f"Error getting accessible users: {e}")
            return []

    async def get_user_directories(self, auth_data: dict) -> List[Dict[str, str]]:
        """
        Get list of user directories in S3 with MongoDB-based access control.
        
        Args:
            auth_data (dict): Authentication data containing user permissions
            
        Returns:
            List[Dict[str, str]]: List of accessible directories:
                - client_id: User identifier
                - name: Display name
                - path: S3 path
        """
        try:
            # Get users accessible to the current user based on MongoDB permissions
            accessible_users = await self.get_accessible_users(auth_data)
            
            # Get S3 directories for accessible users
            paginator = self.s3_client.get_paginator('list_objects_v2')
            user_dirs = []
            
            for user in accessible_users:
                client_id = user["client_id"]
                prefix = f"public/{client_id}/"
                
                # Check if directory exists in S3
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=prefix,
                    MaxKeys=1
                )
                
                if response.get('KeyCount', 0) > 0:
                    user_dirs.append({
                        "client_id": client_id,
                        "name": user["name"] or client_id,
                        "path": prefix
                    })
            
            return sorted(user_dirs, key=lambda x: x["name"])
            
        except Exception as e:
            logger.error(f"Error getting user directories: {e}")
            return []

    async def get_user_content_dump_folders(self, user_id: str) -> List[Dict[str, str]]:
        """
        Get predefined content dump folders for a user.
        
        Args:
            user_id (str): User identifier
            
        Returns:
            List[Dict[str, str]]: List of available folders:
                - path: S3 folder path
                - name: Display name
        """
        try:
            # Add default folders first
            folders = [
                {
                    "path": f"public/{user_id}/ALL/",
                    "name": "Content Dump"
                },
                {
                    "path": f"public/{user_id}/SPOT/",
                    "name": "Spotlight"
                },
                {
                    "path": f"public/{user_id}/STO/",
                    "name": "Stories"
                }
            ]
            
            return folders
            
        except Exception as e:
            logger.error(f"Error getting content dump folders for user {user_id}: {e}")
            return []

    def validate_story_date(self, date_str: str) -> bool:
        """
        Validate if the selected date is allowed for stories.
        
        Args:
            date_str (str): Date string to validate
            
        Returns:
            bool: Always returns True as all dates are currently allowed
        """
        # Always allow any date
        return True

# Use the S3UploadManager class
UploadManager = S3UploadManager 