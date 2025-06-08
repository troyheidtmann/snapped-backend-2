"""
Bunny Scanner Video Processing Module

This module handles automated video processing for Snapchat story content stored in BunnyCDN.
It provides video optimization, orientation correction, and duration management for social
media content delivery.

System Architecture:
    - Storage Integration:
        * BunnyCDN: Primary content storage
        * Directory structure: /sc/{client_id}/STORIES/F(date)_{client_id}/
    
    - Video Processing Pipeline:
        1. Content Discovery:
            - Daily folder scanning
            - Client content organization
            - File type filtering
        
        2. Video Analysis:
            - Orientation detection
            - Duration validation
            - Metadata extraction
        
        3. Processing Operations:
            - Vertical orientation conversion
            - Duration adjustment
            - Video segmentation
    
    - Dependencies:
        * FFmpeg: Video processing and analysis
        * BunnyCDN API: Content storage and retrieval
        * Custom video utilities:
            - flipmedia: Orientation correction
            - extendmedia: Duration extension
            - video_splitter: Content segmentation

Video Processing Rules:
    - Duration Constraints:
        * Minimum: 6.4 seconds
        * Maximum: 22 seconds
        * Segment Length: 15 seconds
    
    - Orientation:
        * Target: Vertical format
        * Auto-detection of rotation metadata
        * Dimension-based orientation analysis

Error Handling:
    - FFmpeg processing errors
    - Network connectivity issues
    - File access failures
    - Invalid content handling
"""

import os
import sys
import logging
import traceback
import ffmpeg
import asyncio
from app.features.video.flipmedia import flip_to_vertical
from app.features.video.extendmedia import extend_video_cdn
from app.features.video.video_splitter import VideoSplitter
from app.shared.bunny_cdn import BunnyCDN
from datetime import datetime
import tempfile
import re

# Configure logging for video processing operations
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BunnyScanner:
    """
    Video processing service for BunnyCDN-hosted Snapchat content.
    
    Responsibilities:
        - Content discovery and organization
        - Video analysis and optimization
        - Format standardization
        - Duration management
    
    Integration Points:
        - BunnyCDN storage
        - FFmpeg processing
        - Video utility modules
    """
    
    def __init__(self):
        """
        Initialize scanner with configuration and dependencies.
        
        Components:
            - BunnyCDN client
            - Video processing utilities
            - Timing constraints
            - Date-based processing
        """
        self.bunny = BunnyCDN()
        self.video_splitter = VideoSplitter(bunny_cdn=self.bunny)
        
        # Processing date configuration
        self.today = datetime.now().strftime("%m-%d-%Y")
        logger.info(f"Scanner will only process folders for date: {self.today}")
        
        # Video constraints
        self.MIN_DURATION = 6.4    # Minimum video duration
        self.MAX_DURATION = 22     # Maximum video duration
        self.SEGMENT_DURATION = 15 # Target segment length

    async def check_orientation(self, file_path: str) -> bool:
        """
        Analyze video orientation using FFmpeg metadata.
        
        Analysis Steps:
            1. Probe video metadata
            2. Extract dimensions
            3. Check rotation flags
            4. Determine final orientation
        
        Metadata Processing:
            - Width/height analysis
            - Rotation metadata detection
            - Display matrix interpretation
        
        Args:
            file_path: Path to video in BunnyCDN
        
        Returns:
            bool: True if video needs vertical conversion
        """
        try:
            probe = ffmpeg.probe(f"{self.bunny.cdn_url}{file_path}")
            video_stream = next(s for s in probe['streams'] if s['codec_type'] == 'video')
            
            width = int(video_stream['width'])
            height = int(video_stream['height'])
            
            # Check for rotation in side_data
            side_data = video_stream.get('side_data_list', [])
            rotation = None
            
            for data in side_data:
                if data.get('side_data_type') == 'Display Matrix':
                    rotation = data.get('rotation')
                    break
            
            logger.info(f"Raw dimensions: {width}x{height}")
            logger.info(f"Display rotation: {rotation}")
            
            # If we have a 90 or -90 degree rotation, the video is actually vertical
            if rotation in [90, -90]:
                logger.info("Video has 90° rotation - treating as vertical")
                return False
                
            # If we have a 180 or -180 degree rotation, maintain horizontal/vertical logic
            is_horizontal = height < width
            logger.info(f"Final orientation decision: {'horizontal' if is_horizontal else 'vertical'}")
            
            return is_horizontal
                
        except Exception as e:
            logger.error(f"Error checking orientation: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    async def process_media(self, file_path: str) -> None:
        """
        Process individual video files for optimization.
        
        Processing Pipeline:
            1. File Validation:
                - Name pattern checking
                - Format verification
            
            2. Orientation Processing:
                - Metadata analysis
                - Vertical conversion if needed
            
            3. Duration Management:
                - Length verification
                - Extension of short videos
                - Segmentation of long videos
            
            4. Output Management:
                - File renaming
                - Original cleanup
                - Segment organization
        
        Args:
            file_path: Path to video in BunnyCDN
        """
        try:
            file_name = os.path.basename(file_path)
            logger.info(f"\nProcessing file: {file_name}")
            
            # TEMPORARILY REMOVE THIS CHECK
            # if re.search(r'^(\d{4})(e|v|-\d+(?!-\d{4}))', file_name):
            #     logger.info(f"Skipping already processed file: {file_name}")
            #     return
            
            # Track current file path as it may change after operations
            current_path = file_path
            
            # Check actual video content
            needs_flip = await self.check_orientation(current_path)
            
            if needs_flip:
                logger.info("Video confirmed horizontal, will flip")
                # Update the path BEFORE flipping
                current_path = current_path.replace(file_name, f"{file_name[:4]}v{file_name[4:]}")
                await flip_to_vertical(file_path, self.bunny)
            else:
                logger.info("Video confirmed vertical, skipping flip")
            
            # Use current_path for subsequent operations
            if current_path.lower().endswith(('.mp4', '.mov', '.avi')):
                try:
                    # Add a small delay to ensure file is available
                    await asyncio.sleep(1)
                    duration = float(ffmpeg.probe(f"{self.bunny.cdn_url}{current_path}")['format']['duration'])
                    logger.info(f"Video duration: {duration}s")
                    
                    if duration < self.MIN_DURATION:
                        logger.info(f"Video is too short ({duration}s), extending")
                        await extend_video_cdn(current_path, self.MIN_DURATION, self.bunny)
                    elif duration > self.MAX_DURATION:
                        logger.info(f"Video is too long ({duration}s), splitting")
                        
                        # Calculate number of segments needed
                        num_segments = int(duration / self.SEGMENT_DURATION) + (1 if duration % self.SEGMENT_DURATION > self.MIN_DURATION else 0)
                        logger.info(f"Will create {num_segments} segments")
                        
                        segments_created = []
                        # Store original filename at the start
                        original_name = os.path.basename(current_path)
                        for segment in range(num_segments):
                            start_time = segment * self.SEGMENT_DURATION
                            # For last segment, use remaining duration
                            if segment == num_segments - 1:
                                segment_duration = duration - start_time
                            else:
                                segment_duration = self.SEGMENT_DURATION
                                
                            # Create segment-specific output path
                            segment_output = current_path.replace(
                                original_name,
                                f"{original_name[:4]}-{segment+1}{original_name[4:]}"
                            )
                                
                            logger.info(f"Creating segment {segment + 1} of {num_segments}")
                            try:
                                await self.video_splitter.split_video(
                                    input_url=current_path,
                                    output_path=segment_output,  # Use segment-specific output path
                                    start_time=start_time,
                                    duration=segment_duration
                                )
                                segments_created.append(segment_output)
                            except Exception as e:
                                logger.error(f"Failed to create segment {segment + 1}: {str(e)}")
                                return
                        
                        # If we successfully created all segments, delete the original
                        if len(segments_created) == num_segments:
                            logger.info(f"Successfully created {len(segments_created)} segments, deleting original file")
                            await self.bunny.delete_files([current_path])
                except ffmpeg.Error as e:
                    logger.error(f"FFmpeg error with path {current_path}: {str(e)}")
                    raise

        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            logger.error(traceback.format_exc())

    async def scan_all_folders(self):
        """
        Scan and process all client content folders.
        
        Directory Structure:
            /sc/
            ├── {client_id}/
            │   └── STORIES/
            │       └── F(date)_{client_id}/
            │           └── video files
        
        Processing Flow:
            1. Client Directory Discovery:
                - Root folder scanning
                - Client folder identification
            
            2. Content Type Filtering:
                - STORIES folder focus
                - Date-based folder selection
            
            3. File Processing:
                - Media file identification
                - Sequential processing
                - Error isolation
        
        Error Handling:
            - Directory access errors
            - Processing failures
            - Network issues
        """
        try:
            logger.info("Starting BunnyCDN scan...")
            
            response = await self.bunny.list_directory("sc/")
            sc_contents = response.get('contents', []) if isinstance(response, dict) else response
            
            if not sc_contents:
                return

            for client_dir in sc_contents:
                if client_dir.get('type') != 'folder':
                    continue
                    
                client_id = client_dir["name"]
                logger.info(f"Scanning client: {client_id}")
                
                response = await self.bunny.list_directory(f"sc/{client_id}/")
                content_dirs = response.get('contents', []) if isinstance(response, dict) else response
                
                if not content_dirs:
                    continue

                for content_dir in content_dirs:
                    if content_dir.get('type') != 'folder':
                        continue
                        
                    content_type = content_dir["name"]
                    if content_type == "STORIES":
                        # Check for today's folder
                        today_folder = f"F({self.today})_{client_id}"
                        folder_path = f"/sc/{client_id}/{content_type}/{today_folder}/"
                        
                        logger.info(f"Checking today's folder: {folder_path}")
                        response = await self.bunny.list_directory(folder_path)
                        files = response.get('contents', []) if isinstance(response, dict) else response
                        
                        if files:
                            for file in files:
                                if file.get('type') == 'folder':
                                    continue
                                
                                file_path = f"{folder_path}{file['name']}"
                                await self.process_media(file_path)
                                await asyncio.sleep(1)
                        else:
                            logger.info(f"No files found in today's folder: {folder_path}")
            
        except Exception as e:
            logger.error(f"Error in scan_all_folders: {str(e)}")
            logger.error(traceback.format_exc())

async def main():
    scanner = BunnyScanner()
    await scanner.scan_all_folders()

if __name__ == "__main__":
    asyncio.run(main())

