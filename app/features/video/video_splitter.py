"""
Video Splitting Module

This module provides functionality to split videos into segments while
maintaining quality and metadata.

Features:
- Video segmentation
- Duration analysis
- Metadata preservation
- Format standardization
- Quality control

Data Model:
- Video segments
- Duration data
- Media dimensions
- Processing status

Dependencies:
- ffmpeg for video processing
- aiohttp for async operations
- PIL for image handling
- logging for tracking
- tempfile for processing

Author: Snapped Development Team
"""

import logging
import tempfile
import os
import aiohttp
import ffmpeg
from app.shared.bunny_cdn import BunnyCDN
from datetime import datetime
import re
from PIL import Image
import io
import asyncio
import json
from typing import Tuple
import math

logger = logging.getLogger(__name__)

class VideoSplitter:
    """
    Video splitting handler.
    
    Manages video segmentation while preserving quality and metadata.
    
    Attributes:
        MIN_SEGMENT_DURATION (int): Minimum segment length in seconds
        bunny (BunnyCDN): CDN client instance
    """
    
    MIN_SEGMENT_DURATION = 6  # minimum segment duration in seconds
    
    def __init__(self, bunny_cdn):
        """
        Initialize splitter.
        
        Args:
            bunny_cdn: CDN client instance
        """
        self.bunny = bunny_cdn

    async def split_video(self, input_url: str, output_path: str, start_time: float, duration: float, local_input: str = None) -> None:
        """
        Split video into segments.
        
        Args:
            input_url: Source video URL
            output_path: Output path
            start_time: Start time in seconds
            duration: Segment duration
            local_input: Optional local file path
            
        Raises:
            Exception: For processing errors
            
        Notes:
            - Downloads if needed
            - Preserves rotation
            - Standardizes format
            - Uploads result
        """
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # Use provided local file if available, otherwise download
                if local_input and os.path.exists(local_input):
                    temp_input = local_input
                else:
                    input_name = os.path.basename(input_url)
                    temp_input = os.path.join(temp_dir, input_name)
                    logger.info(f"Downloading video to {temp_input}")
                    if not await self.bunny.download_file(input_url, temp_input):
                        raise Exception("Failed to download input file")

                # Use output_path directly without modification
                temp_output = os.path.join(temp_dir, os.path.basename(output_path))
                
                # Process the video
                stream = ffmpeg.input(temp_input, ss=start_time, t=duration)
                stream = ffmpeg.output(
                    stream,
                    temp_output,
                    vf="scale=1080:1920:force_original_aspect_ratio=decrease,format=yuv420p",
                    movflags='+faststart',
                    acodec='copy',
                    preset='ultrafast',
                    crf=30
                )
                
                logger.info(f"Running ffmpeg split command...")
                stream.overwrite_output().run()
                
                logger.info(f"Uploading processed video to {output_path}")
                with open(temp_output, 'rb') as f:
                    if not await self.bunny.upload_file(output_path, f.read()):
                        raise Exception("Failed to upload processed video")
            
        except ffmpeg.Error as e:
            logger.error(f"FFmpeg error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error splitting video: {str(e)}")
            raise

    async def get_video_duration(self, video_url: str) -> float:
        """
        Get video duration.
        
        Args:
            video_url: Video URL
            
        Returns:
            float: Duration in seconds
            
        Raises:
            Exception: For processing errors
            
        Notes:
            - Uses ffmpeg
            - Handles errors
            - Returns float
        """
        try:
            probe = ffmpeg.probe(video_url)
            duration = float(probe['streams'][0]['duration'])
            return duration
        except Exception as e:
            logger.error(f"Error getting video duration: {str(e)}")
            raise

    async def get_media_info(self, file_path: str) -> dict:
        """
        Get media metadata.
        
        Args:
            file_path: Path to media file
            
        Returns:
            dict: Media metadata
            
        Raises:
            Exception: For processing errors
            
        Notes:
            - Handles images/videos
            - Gets dimensions
            - Gets duration
            - Validates format
        """
        try:
            # Create temporary directory for processing
            with tempfile.TemporaryDirectory() as temp_dir:
                # Download file to temp location first
                temp_file = os.path.join(temp_dir, os.path.basename(file_path))
                
                # Construct full URL with storage zone
                clean_path = file_path.strip('/')
                file_url = f"{self.bunny.base_url}{self.bunny.storage_zone}/{clean_path}"
                logger.info(f"Getting media info for: {file_url}")
                
                # Download file
                if not await self.bunny.download_file(clean_path, temp_file):
                    raise Exception(f"Failed to download file")
                
                # Determine file type
                file_ext = os.path.splitext(file_path)[1].lower()
                
                # Handle images
                if file_ext in self.IMAGE_EXTENSIONS:
                    img = Image.open(temp_file)
                    return {
                        'width': img.width,
                        'height': img.height
                    }
                
                # Handle videos
                elif file_ext in self.VIDEO_EXTENSIONS:
                    probe = ffmpeg.probe(temp_file)
                    video_stream = next((stream for stream in probe['streams'] 
                                       if stream['codec_type'] == 'video'), None)
                    
                    if not video_stream:
                        raise Exception("No video stream found")
                        
                    info = {
                        'width': int(video_stream['width']),
                        'height': int(video_stream['height'])
                    }
                    
                    if video_stream.get('duration'):
                        info['duration'] = float(video_stream['duration'])
                        
                    return info
                else:
                    raise Exception(f"Unsupported file type: {file_ext}")
                    
        except Exception as e:
            logger.error(f"Error getting media info: {str(e)}")
            raise

    async def get_media_dimensions(self, video_url: str) -> Tuple[int, int]:
        """Get media dimensions using ffprobe."""
        try:
            # Download file to temp location first
            temp_dir = tempfile.gettempdir()
            temp_file = os.path.join(temp_dir, 'temp_video.mp4')
            
            async with aiohttp.ClientSession() as session:
                async with session.get(video_url) as response:
                    if response.status != 200:
                        raise Exception(f"Failed to download video: {response.status}")
                    with open(temp_file, 'wb') as f:
                        while True:
                            chunk = await response.content.read(8192)
                            if not chunk:
                                break
                            f.write(chunk)
            
            # Now get dimensions from local file
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_streams',
                temp_file
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                raise Exception(f"ffprobe failed: {stderr.decode()}")
            
            # Clean up temp file
            os.remove(temp_file)
            
            probe = json.loads(stdout.decode())
            video_info = next(
                (stream for stream in probe['streams'] if stream['codec_type'] == 'video'),
                None
            )
            
            if not video_info:
                raise Exception("No video stream found")
            
            width = int(video_info['width'])
            height = int(video_info['height'])
            
            return width, height
            
        except Exception as e:
            logger.error(f"Error getting media dimensions: {str(e)}")
            raise 