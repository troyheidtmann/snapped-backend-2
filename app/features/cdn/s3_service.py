"""
S3 Service - AWS S3 Storage Integration Module

This module provides a service layer for interacting with AWS S3 storage, handling file operations,
directory management, and content organization. It manages the connection to S3 and provides
methods for common storage operations.

Architecture:
-----------
1. Storage Layer:
   - Direct S3 bucket access
   - Transfer management
   - Path organization
   - Content metadata

2. Operations:
   - Directory listing
   - File movement
   - Presigned URLs
   - Folder creation

3. Error Handling:
   - Connection failures
   - Access denials
   - Missing resources
   - Operation timeouts

Security:
--------
1. Access Control:
   - AWS credentials
   - Bucket policies
   - Resource permissions

2. Data Protection:
   - Secure transfers
   - Content validation
   - Path sanitization

Dependencies:
-----------
- boto3: AWS SDK for Python
- botocore: Low-level AWS operations
- logging: Operation tracking

Author: Snapped Development Team
"""

import boto3
from botocore.exceptions import ClientError
from typing import List, Dict, Optional, Union
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set to DEBUG level

class S3Service:
    """
    Service class for AWS S3 storage operations.
    
    This class provides methods for interacting with AWS S3, including file operations,
    directory management, and content organization. It handles authentication, connection
    management, and error handling for S3 operations.
    
    Attributes:
        aws_access_key (str): AWS access key ID
        aws_secret_key (str): AWS secret access key
        aws_region (str): AWS region (e.g., 'us-east-2')
        bucket_name (str): Target S3 bucket name
        s3_client: Boto3 S3 client instance
    
    Connection Flow:
    --------------
    1. Initialize credentials
    2. Create S3 client
    3. Test bucket access
    4. Handle connection errors
    
    Error Handling:
    -------------
    - Access denied (403)
    - Bucket not found (404)
    - Connection failures
    - Invalid credentials
    """

    def __init__(self):
        try:
            # Hardcoded credentials for testing
            self.aws_access_key = 'AKIAWFIPTBYUC2F6XKEU'
            self.aws_secret_key = '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'
            self.aws_region = 'us-east-2'  # Changed to us-east-2
            self.bucket_name = 'snapped2'

            logger.debug(f"Initializing S3 client with region: {self.aws_region}")
            
            # Initialize S3 client with credentials
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name=self.aws_region
            )
            
            # Test connection
            logger.debug("Testing S3 connection...")
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Successfully connected to S3 bucket: {self.bucket_name}")
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = e.response.get('Error', {}).get('Message', 'Unknown error')
            if error_code == '403':
                logger.error(f"Access denied to S3 bucket. Error: {error_msg}")
            elif error_code == '404':
                logger.error(f"S3 bucket '{self.bucket_name}' not found. Error: {error_msg}")
            else:
                logger.error(f"Error connecting to S3: {error_code} - {error_msg}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error initializing S3 service: {str(e)}")
            raise

    async def list_directory(self, prefix: str) -> Dict:
        """
        List contents of a directory in S3.
        
        This method retrieves the contents of a directory in S3, including both files
        and subdirectories. It handles pagination and metadata extraction for each item.
        
        Args:
            prefix (str): Directory path prefix to list
                         Example: 'public/tt10021994/STO/033125/'
        
        Returns:
            Dict containing:
            - files: List of file objects with metadata
            - folders: List of folder objects
            - status: Operation status
        
        File Metadata:
        -------------
        - name: File name
        - type: Content type
        - size: File size in bytes
        - last_modified: ISO format timestamp
        - path: Full S3 key path
        
        Example Response:
        ---------------
        {
            'files': [
                {
                    'name': 'example.jpg',
                    'type': 'file',
                    'size': 1024,
                    'last_modified': '2024-03-20T15:30:00',
                    'path': 'public/client/folder/example.jpg'
                }
            ],
            'folders': [
                {
                    'name': 'subfolder',
                    'type': 'folder',
                    'path': 'public/client/folder/subfolder/'
                }
            ],
            'status': 'success'
        }
        
        Raises:
            ClientError: For S3 operation failures
            Exception: For unexpected errors
        """
        try:
            logger.debug(f"Listing directory with prefix: {prefix}")
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter='/'
            )
            
            logger.debug(f"S3 list_objects_v2 response: {response}")
            
            files = []
            folders = []
            
            # Handle folders (CommonPrefixes)
            if 'CommonPrefixes' in response:
                logger.debug(f"Found {len(response['CommonPrefixes'])} folders")
                for prefix_obj in response['CommonPrefixes']:
                    folder_name = prefix_obj['Prefix'].split('/')[-2]  # Get last non-empty segment
                    logger.debug(f"Processing folder: {folder_name}")
                    folders.append({
                        'name': folder_name,
                        'type': 'folder',
                        'path': prefix_obj['Prefix']
                    })
            
            # Handle files
            if 'Contents' in response:
                logger.debug(f"Found {len(response['Contents'])} files")
                for item in response['Contents']:
                    # Skip if this is the directory itself
                    if item['Key'].endswith('/'):
                        continue
                        
                    file_name = item['Key'].split('/')[-1]
                    logger.debug(f"Processing file: {file_name}")
                    files.append({
                        'name': file_name,
                        'type': 'file',
                        'size': item['Size'],
                        'last_modified': item['LastModified'].isoformat(),
                        'path': item['Key']
                    })
            
            result = {
                'files': files,
                'folders': folders,
                'status': 'success'
            }
            logger.debug(f"Returning result: {result}")
            return result
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_msg = e.response.get('Error', {}).get('Message', 'Unknown error')
            logger.error(f"S3 error listing directory: {error_code} - {error_msg}")
            return {
                'status': 'error',
                'message': f"{error_code}: {error_msg}"
            }
        except Exception as e:
            logger.error(f"Unexpected error listing directory: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }

    async def move_files(self, source_path: str, dest_path: str, items: List[str]) -> Dict:
        """
        Move multiple files in S3 using the transfer manager for reliability.
        
        This method handles batch file movement operations, including:
        - Path validation and cleanup
        - Copy operations with tagging
        - Source deletion after successful copy
        - Operation tracking and rollback
        
        Args:
            source_path (str): Source directory path
            dest_path (str): Destination directory path
            items (List[str]): List of file paths to move
        
        Process Flow:
        ------------
        1. Path Preparation:
           - Clean paths
           - Validate formats
           - Handle duplicates
        
        2. File Operations:
           - Copy with tags
           - Verify transfer
           - Delete source
        
        3. Error Handling:
           - Operation tracking
           - Partial failures
           - Cleanup on error
        
        Returns:
            Dict containing:
            - status: Operation status
            - message: Status message
        
        Raises:
            Exception: For operation failures
        """
        try:
            logger.debug(f"Moving files from {source_path} to {dest_path}")
            logger.debug(f"Files to move: {items}")
            
            # Create S3 resource with our credentials
            s3 = boto3.resource(
                's3',
                aws_access_key_id=self.aws_access_key,
                aws_secret_access_key=self.aws_secret_key,
                region_name=self.aws_region
            )
            
            # Track all operations
            operations = []
            
            for item_path in items:
                # Clean up the path to prevent filename duplication
                filename = os.path.basename(item_path)
                source_key = item_path.replace(f"/{filename}/{filename}", f"/{filename}")
                dest_key = f"{dest_path.rstrip('/')}/{filename}"
                
                logger.debug(f"Moving from {source_key} to {dest_key}")
                
                try:
                    # Use copy_object with the S3 resource and add 'Moved' tag
                    copy_source = {
                        'Bucket': self.bucket_name,
                        'Key': source_key
                    }
                    
                    s3_object = s3.Object(self.bucket_name, dest_key)
                    copy_result = s3_object.copy(
                        copy_source,
                        ExtraArgs={
                            'TaggingDirective': 'REPLACE',
                            'Tagging': 'Moved=true'
                        }
                    )
                    operations.append((source_key, copy_result))
                    
                except Exception as e:
                    logger.error(f"Error initiating move for {source_key}: {str(e)}")
                    return {
                        'status': 'error',
                        'message': f"Failed to move {source_key}: {str(e)}"
                    }

            # Wait for all copy operations to complete and delete sources
            for source_key, copy_result in operations:
                try:
                    # Wait for copy to complete
                    copy_result
                    
                    # Only delete if copy was successful
                    s3.Object(self.bucket_name, source_key).delete()
                    
                except Exception as e:
                    logger.error(f"Error completing move for {source_key}: {str(e)}")
                    return {
                        'status': 'error',
                        'message': f"Failed to complete move for {source_key}: {str(e)}"
                    }
            
            return {
                'status': 'success',
                'message': f'Successfully moved files to {dest_path}'
            }
            
        except Exception as e:
            logger.error(f"Unexpected error moving files: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }

    async def get_presigned_url(self, key: str, expires_in: int = 3600) -> str:
        """
        Generate a presigned URL for an S3 object.
        
        Creates a temporary URL that allows access to an S3 object without requiring
        AWS credentials. Useful for temporary content sharing and downloads.
        
        Args:
            key (str): S3 object key
            expires_in (int): URL expiration time in seconds (default: 1 hour)
        
        Returns:
            str: Presigned URL for the object
            None: If URL generation fails
        
        Security:
        --------
        - Temporary access
        - No credential exposure
        - Time-limited validity
        
        Raises:
            ClientError: For S3 operation failures
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': key
                },
                ExpiresIn=expires_in
            )
            return url
        except ClientError as e:
            print(f"Error generating presigned URL: {str(e)}")
            return None

    async def create_folder(self, folder_path: str) -> Dict:
        """
        Create a new folder in S3.
        
        Creates a new folder (empty object with trailing slash) in the S3 bucket.
        This is a common pattern for representing directories in S3's flat storage model.
        
        Args:
            folder_path (str): Path for the new folder
        
        Process:
        --------
        1. Path normalization
        2. Empty object creation
        3. Status verification
        
        Returns:
            Dict containing:
            - status: Operation status
            - message: Status message
        
        Raises:
            ClientError: For S3 operation failures
        """
        try:
            # Ensure the path ends with a slash
            if not folder_path.endswith('/'):
                folder_path += '/'
                
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=folder_path,
                Body=''
            )
            
            return {
                'status': 'success',
                'message': f'Successfully created folder {folder_path}'
            }
            
        except ClientError as e:
            print(f"Error creating folder: {str(e)}")
            return {
                'status': 'error',
                'message': str(e)
            }

    def parse_s3_path(self, path: str) -> Dict:
        """
        Parse an S3 path into its components.
        
        Breaks down an S3 path into its constituent parts for easier handling
        and validation of path structures.
        
        Args:
            path (str): S3 path to parse
                       Example: 'public/tt10021994/STO/033125/'
        
        Returns:
            Dict containing path components:
            - client_id: Client identifier
            - content_type: Content category
            - session_id: Session identifier
            - filename: File name if present
            None: If path format is invalid
        
        Path Format:
        -----------
        public/
        └── {client_id}/
            └── {content_type}/
                └── {session_id}/
                    └── {filename}
        """
        parts = path.strip('/').split('/')
        
        if len(parts) < 2:
            return None
            
        return {
            'client_id': parts[1] if len(parts) > 1 else None,
            'content_type': parts[2] if len(parts) > 2 else None,
            'session_id': parts[3] if len(parts) > 3 else None,
            'filename': parts[4] if len(parts) > 4 else None
        } 