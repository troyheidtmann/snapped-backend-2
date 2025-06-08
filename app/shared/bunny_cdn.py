"""
BunnyCDN Integration Module

This module provides integration with BunnyCDN for content delivery
and storage management.

Features:
- File management
- Directory operations
- Content delivery
- Storage zones
- Access control

Data Model:
- Files and directories
- Storage paths
- CDN URLs
- Access tokens
- File metadata

Security:
- API key validation
- Access control
- Error handling
- Secure transfers
- Path validation

Dependencies:
- aiohttp for async HTTP
- dotenv for config
- logging for tracking
- os for paths

Author: Snapped Development Team
"""

import os
import aiohttp
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

class BunnyCDN:
    """
    BunnyCDN service integration.
    
    Manages CDN operations and storage.
    
    Attributes:
        api_key: BunnyCDN API key
        storage_zone: Storage zone name
        base_url: Base API URL
        cdn_url: Public CDN URL
        headers: Request headers
    """
    
    def __init__(self):
        """
        Initialize BunnyCDN client.
        
        Notes:
            - Loads config
            - Sets headers
            - Logs setup
        """
        self.api_key = os.getenv('BUNNY_API_KEY')
        self.storage_zone = os.getenv('BUNNY_STORAGE_ZONE')
        self.base_url = f"{os.getenv('BUNNY_BASE_URL')}/"
        self.cdn_url = os.getenv('BUNNY_CDN_URL')
        self.headers = {
            "AccessKey": self.api_key,
            "Accept": "*/*"
        }
        # Log configuration
        logger.info(f"BunnyCDN initialized with:")
        logger.info(f"Storage Zone: {self.storage_zone}")
        logger.info(f"Base URL: {self.base_url}")
        logger.info(f"CDN URL: {self.cdn_url}")

    def get_cdn_url(self, path: str) -> str:
        """
        Get public CDN URL for path.
        
        Args:
            path: File/directory path
            
        Returns:
            str: Public CDN URL
            
        Notes:
            - Cleans path
            - Joins URL
            - Formats path
        """
        clean_path = path.strip('/')
        return f"{self.cdn_url}/{clean_path}"

    async def list_directory(self, path: str = "", page: int = 1, limit: int = 50):
        """
        List directory contents.
        
        Args:
            path: Directory path
            page: Page number
            limit: Items per page
            
        Returns:
            dict: Directory contents
            
        Notes:
            - Handles pagination
            - Transforms data
            - Error handling
            - CDN URLs
        """
        async with aiohttp.ClientSession() as session:
            # Add storage zone to path and ensure trailing slash
            clean_path = f"{self.storage_zone}/{path.strip('/')}/"
            url = f"{self.base_url}/{clean_path}"
            logger.info(f"Fetching from BunnyCDN: {url}")
            
            async with session.get(url, headers=self.headers) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Raw BunnyCDN response: {data}")
                    
                    # Transform the response to include type field and CDN URL
                    transformed_data = []
                    for item in data:
                        file_path = f"{path.strip('/')}/{item.get('ObjectName', '')}"
                        transformed_item = {
                            'type': 'folder' if item.get('IsDirectory', False) else 'file',
                            'name': item.get('ObjectName', ''),
                            'size': item.get('Length', 0),
                            'lastModified': item.get('LastChanged', ''),
                            'guid': item.get('Guid', ''),
                            'contentType': item.get('ContentType', ''),
                            'url': self.get_cdn_url(file_path) if not item.get('IsDirectory', False) else None
                        }
                        transformed_data.append(transformed_item)
                    
                    return {
                        "contents": transformed_data,
                        "page": page,
                        "limit": limit,
                        "has_more": len(transformed_data) >= limit if transformed_data else False
                    }
                    
                logger.error(f"BunnyCDN returned status {response.status}")
                return {
                    "contents": [],
                    "page": page,
                    "limit": limit,
                    "has_more": False
                }

    async def create_directory(self, path: str):
        """
        Create new directory.
        
        Args:
            path: Directory path
            
        Returns:
            bool: Success status
            
        Notes:
            - Validates path
            - Creates dir
            - Error handling
            - Logs status
        """
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.base_url}/{self.storage_zone}/{path.lstrip('/')}"
                logger.info(f"Creating directory at URL: {url}")
                logger.info(f"Using headers: {self.headers}")
                async with session.put(url, headers=self.headers) as response:
                    status = response.status
                    text = await response.text()
                    logger.info(f"Response status: {status}")
                    logger.info(f"Response text: {text}")
                    return status == 200 or status == 201
        except Exception as e:
            logger.error(f"Error in create_directory: {str(e)}")
            logger.exception("Full traceback:")
            return False

    async def move_files(self, source_path: str, destination_path: str, items: list):
        """
        Move files between directories.
        
        Args:
            source_path: Source directory
            destination_path: Target directory
            items: Files to move
            
        Returns:
            bool: Success status
            
        Notes:
            - Path cleaning
            - File copying
            - Source deletion
            - Error handling
        """
        try:
            logger.info("\n=== START MOVE FILES ===")
            logger.info(f"Raw inputs:")
            logger.info(f"Source path: {source_path}")
            logger.info(f"Destination path: {destination_path}")
            logger.info(f"Items: {items}")
            logger.info(f"Storage zone: {self.storage_zone}")
            logger.info(f"Base URL: {self.base_url}")

            async with aiohttp.ClientSession() as session:
                results = []
                for item in items:
                    # Clean up paths - remove leading/trailing slashes but keep sc/
                    clean_source = item.strip('/')
                    logger.info(f"\nProcessing item: {clean_source}")
                    
                    # Don't remove sc/, just ensure proper format
                    if not clean_source.startswith('sc/') and not clean_source.startswith('/sc/'):
                        clean_source = f"sc/{clean_source}"
                    elif clean_source.startswith('/sc/'):
                        clean_source = clean_source[1:]  # Just remove leading slash
                    
                    clean_source = f"{self.storage_zone}/{clean_source}"
                    logger.info(f"Final source path: {clean_source}")

                    clean_dest = destination_path.strip('/')
                    logger.info(f"Initial dest path: {clean_dest}")
                    
                    # Same for destination - keep sc/
                    if not clean_dest.startswith('sc/') and not clean_dest.startswith('/sc/'):
                        clean_dest = f"sc/{clean_dest}"
                    elif clean_dest.startswith('/sc/'):
                        clean_dest = clean_dest[1:]  # Just remove leading slash
                    
                    clean_dest = f"{self.storage_zone}/{clean_dest}"
                    logger.info(f"Final dest path: {clean_dest}")

                    filename = item.split('/')[-1]
                    logger.info(f"Filename: {filename}")
                    
                    # Construct URLs
                    source_url = f"{self.base_url.rstrip('/')}/{clean_source}"
                    dest_url = f"{self.base_url.rstrip('/')}/{clean_dest}/{filename}"
                    
                    logger.info(f"\nFinal URLs:")
                    logger.info(f"Source URL: {source_url}")
                    logger.info(f"Destination URL: {dest_url}")

                    # First GET the file data
                    async with session.get(source_url, headers=self.headers) as get_response:
                        if get_response.status == 200:
                            file_data = await get_response.read()
                            
                            # Then PUT directly like in curl
                            headers = {
                                **self.headers,
                                'Content-Type': 'application/octet-stream'
                            }
                            
                            async with session.put(dest_url, headers=headers, data=file_data) as put_response:
                                if put_response.status == 201:
                                    # Only delete if PUT was successful
                                    async with session.delete(source_url, headers=self.headers) as del_response:
                                        success = del_response.status == 200
                                        results.append(success)
                                else:
                                    error_text = await put_response.text()
                                    logger.error(f"PUT failed with status {put_response.status}: {error_text}")
                                    results.append(False)
                        else:
                            error_text = await get_response.text()
                            logger.error(f"GET failed with status {get_response.status}: {error_text}")
                            results.append(False)
                
                return all(results)
        except Exception as e:
            logger.error(f"Error in move_files: {str(e)}")
            logger.exception("Full traceback:")
            return False

    async def delete_files(self, items: list):
        """
        Delete files or directories.
        
        Args:
            items: Files/dirs to delete
            
        Returns:
            bool: Success status
            
        Notes:
            - Path cleaning
            - Batch deletion
            - Error handling
            - Status tracking
        """
        async with aiohttp.ClientSession() as session:
            results = []
            for item in items:
                try:
                    # Construct proper URL with storage zone
                    clean_path = f"{self.storage_zone}/{item.strip('/')}"
                    url = f"{self.base_url}{clean_path}"
                    logger.info(f"Attempting to delete: {url}")
                    
                    async with session.delete(url, headers=self.headers) as response:
                        success = response.status == 200
                        if not success:
                            error_text = await response.text()
                            logger.error(f"Delete failed with status {response.status}: {error_text}")
                        else:
                            logger.info(f"Successfully deleted: {item}")
                        results.append(success)
                except Exception as e:
                    logger.error(f"Error deleting {item}: {str(e)}")
                    results.append(False)
            return all(results)

    async def download_file(self, file_path: str, local_path: str) -> bool:
        """
        Download file from BunnyCDN.
        
        Args:
            file_path: CDN file path
            local_path: Local save path
            
        Returns:
            bool: Success status
            
        Notes:
            - Path cleaning
            - Chunked download
            - Error handling
            - File writing
        """
        try:
            # Construct the full URL
            clean_path = f"{self.storage_zone}/{file_path.strip('/')}"
            url = f"{self.base_url}/{clean_path}"
            
            # Add additional headers for download
            download_headers = {
                **self.headers,
                "Content-Type": "application/octet-stream"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=download_headers) as response:
                    if response.status == 200:
                        with open(local_path, 'wb') as f:
                            while True:
                                chunk = await response.content.read(8192)  # 8KB chunks
                                if not chunk:
                                    break
                                f.write(chunk)
                        return True
                    else:
                        logger.error(f"Failed to download file {file_path}: {response.status}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error downloading file {file_path}: {str(e)}")
            return False

    async def upload_file(self, file_path: str, file_data: bytes) -> bool:
        """
        Upload file to BunnyCDN.
        
        Args:
            file_path: Target CDN path
            file_data: File contents
            
        Returns:
            bool: Success status
            
        Notes:
            - Path cleaning
            - Data upload
            - Error handling
            - Status tracking
        """
        try:
            # Construct the full URL
            clean_path = f"{self.storage_zone}/{file_path.strip('/')}"
            url = f"{self.base_url}/{clean_path}"
            
            # Add headers for upload
            upload_headers = {
                **self.headers,
                "Content-Type": "application/octet-stream"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.put(url, headers=upload_headers, data=file_data) as response:
                    if response.status in [200, 201]:
                        logger.info(f"Successfully uploaded file to {file_path}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to upload file {file_path}: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error uploading file {file_path}: {str(e)}")
            return False
