#!/usr/bin/env python3
import subprocess
import sys
import os
import requests
import json
import tempfile
from urllib.parse import urlparse
import logging
import aiohttp
import asyncio
import ffmpeg
import shutil
from PIL import Image  # Add PIL for image processing

logger = logging.getLogger(__name__)

"""
Media Orientation Module

This module provides functionality to detect and correct media orientation,
ensuring vertical format for Snapchat content.

Features:
- Orientation detection
- Image flipping
- Video rotation
- EXIF handling
- Format validation

Data Model:
- Media metadata
- Orientation data
- Processing status
- File paths

Dependencies:
- ffmpeg for video processing
- PIL for image processing
- aiohttp for async operations
- logging for tracking
- tempfile for processing

Author: Snapped Development Team
"""

# Define supported media types
SUPPORTED_EXTENSIONS = {
    'video': ('.mp4', '.mov', '.avi', '.webm', '.mkv'),
    'image': ('.jpg', '.jpeg', '.png', '.webp', '.gif')
}

def is_image_file(file_path):
    """
    Check if file is an image.
    
    Args:
        file_path: Path to file
        
    Returns:
        bool: True if image file
        
    Notes:
        - Checks extension
        - Case insensitive
        - Supported formats
    """
    ext = os.path.splitext(file_path)[1].lower()
    return ext in SUPPORTED_EXTENSIONS['image']

def is_video_file(file_path):
    """
    Check if file is a video.
    
    Args:
        file_path: Path to file
        
    Returns:
        bool: True if video file
        
    Notes:
        - Checks extension
        - Case insensitive
        - Supported formats
    """
    ext = os.path.splitext(file_path)[1].lower()
    return ext in SUPPORTED_EXTENSIONS['video']


def download_media(url):
    """
    Download media from the given URL
    """
    try:
        # Get the filename from the URL
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path)
        
        # Download the file
        print(f"Downloading {filename}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Save the file
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        return filename
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        sys.exit(1)

async def flip_to_vertical(file_path: str, bunny_cdn=None) -> bool:
    """
    Flip media to vertical orientation.
    
    Args:
        file_path: Path to media file
        bunny_cdn: CDN client instance
        
    Returns:
        bool: Success status
        
    Notes:
        - Handles images and videos
        - Checks EXIF data
        - Preserves quality
        - CDN integration
    """
    try:
        if not bunny_cdn:
            return False

        filename = os.path.basename(file_path)
        if filename[4:5] == 'v':
            logger.info(f"File {filename} already has 'v' marker. Skipping.")
            return False

        with tempfile.TemporaryDirectory() as temp_dir:
            input_name = os.path.basename(file_path)
            input_path = os.path.join(temp_dir, input_name)

            if not await bunny_cdn.download_file(file_path, input_path):
                logger.error("Failed to download file")
                return False

            needs_flip = False
            if is_image_file(input_path):
                try:
                    img = Image.open(input_path)
                    width, height = img.size
                    
                    # Check if image is horizontal
                    if width > height:
                        needs_flip = True
                        logger.info(f"Image is horizontal ({width}x{height}). Will flip.")
                    else:
                        logger.info(f"Image is already vertical ({width}x{height}). Skipping.")
                        return False

                    if needs_flip:
                        # Check EXIF for rotation direction
                        orientation = None
                        try:
                            exif = img._getexif()
                            if exif:
                                orientation = exif.get(274)
                        except:
                            pass

                        output_name = f"{input_name[:4]}v{input_name[4:]}"
                        output_path = os.path.join(temp_dir, output_name)

                        # Rotate based on EXIF or default
                        if orientation == 6:
                            rotated = img.transpose(Image.ROTATE_270)
                        elif orientation == 8:
                            rotated = img.transpose(Image.ROTATE_90)
                        else:
                            rotated = img.transpose(Image.ROTATE_270)
                        
                        rotated.save(output_path)
                        
                        # Upload flipped version
                        cdn_output_path = os.path.join(os.path.dirname(file_path), output_name)
                        with open(output_path, 'rb') as f:
                            if await bunny_cdn.upload_file(cdn_output_path, f.read()):
                                await bunny_cdn.delete_files([file_path])
                                return True
                except Exception as e:
                    logger.error(f"Image processing failed: {str(e)}")
                    return False

            elif is_video_file(input_path):
                try:
                    # Check video dimensions
                    probe = ffmpeg.probe(input_path)
                    video_stream = next((stream for stream in probe['streams'] 
                                       if stream['codec_type'] == 'video'), None)
                    if not video_stream:
                        logger.error("No video stream found")
                        return False
                        
                    width = int(video_stream['width'])
                    height = int(video_stream['height'])
                    
                    if width > height:
                        needs_flip = True
                        logger.info(f"Video is horizontal ({width}x{height}). Will flip.")
                    else:
                        logger.info(f"Video is already vertical ({width}x{height}). Skipping.")
                        return False

                    if needs_flip:
                        output_name = f"{input_name[:4]}v{input_name[4:]}"
                        output_path = os.path.join(temp_dir, output_name)
                        
                        stream = ffmpeg.input(input_path)
                        stream = ffmpeg.filter(stream, 'transpose', 2)
                        stream = ffmpeg.output(stream, output_path)
                        ffmpeg.run(stream, overwrite_output=True, quiet=True)
                        
                        # Upload flipped version
                        cdn_output_path = os.path.join(os.path.dirname(file_path), output_name)
                        with open(output_path, 'rb') as f:
                            if await bunny_cdn.upload_file(cdn_output_path, f.read()):
                                await bunny_cdn.delete_files([file_path])
                                return True
                except Exception as e:
                    logger.error(f"Video processing failed: {str(e)}")
                    return False

            return False

    except Exception as e:
        logger.error(f"Flip failed: {str(e)}")
        return False


def main():
    # Configure basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
   

if __name__ == "__main__":
    main()
