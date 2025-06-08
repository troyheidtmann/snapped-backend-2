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
from fractions import Fraction

logger = logging.getLogger(__name__)

"""
Media Extension Module

This module provides functionality to extend short videos by holding
the last frame to meet minimum duration requirements.

Features:
- Video duration extension
- Local file processing
- URL media handling
- Duration checking
- Format preservation

Data Model:
- Video metadata
- Duration settings
- Processing status
- File paths

Dependencies:
- ffmpeg for video processing
- requests for downloads
- aiohttp for async operations
- logging for tracking
- tempfile for processing

Author: Snapped Development Team
"""

def get_media_duration_from_url(url):
    """
    Get media duration from URL using ffprobe.
    
    Args:
        url: Media file URL
        
    Returns:
        float: Duration in seconds
        
    Notes:
        - Uses ffprobe
        - Returns 0.0 on error
        - Handles JSON parsing
    """
    try:
        cmd = [
            'ffprobe',
            '-v', 'quiet',
            '-print_format', 'json',
            '-show_format',
            url
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        # Get duration from format
        if 'format' in data and 'duration' in data['format']:
            return float(data['format']['duration'])
            
        return 0.0
            
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError) as e:
        print(f"Error getting media duration: {e}")
        return 0.0

def download_media(url):
    """
    Download media from URL.
    
    Args:
        url: Media file URL
        
    Returns:
        str: Local filename
        
    Raises:
        SystemExit: On download failure
        
    Notes:
        - Streams content
        - Creates local file
        - Handles large files
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

async def extend_video_cdn(file_path: str, target_duration: float = 6.4, bunny_cdn=None) -> bool:
    """
    Handle video extension and CDN upload.
    
    Args:
        file_path: Path to video file
        target_duration: Target duration in seconds
        bunny_cdn: CDN client instance
        
    Returns:
        bool: Success status
        
    Notes:
        - Downloads from CDN
        - Extends video
        - Uploads back to CDN
        - Handles cleanup
    """
    try:
        if not bunny_cdn:
            return False

        with tempfile.TemporaryDirectory() as temp_dir:
            # Setup paths
            input_name = os.path.basename(file_path)
            input_path = os.path.join(temp_dir, input_name)
            output_name = f"{input_name[:4]}e{input_name[4:]}"
            output_path = os.path.join(temp_dir, output_name)

            # Download the file
            if not await bunny_cdn.download_file(file_path, input_path):
                logger.error("Failed to download file")
                return False

            # Get video duration
            probe = ffmpeg.probe(input_path)
            duration = float(probe.get('format', {}).get('duration', 0))
            
            logger.info(f"Video duration: {duration} seconds")
            
            if duration >= target_duration:
                logger.info(f"Video is already longer than {target_duration} seconds")
                return False

            # Extend the video
            if not extend_video_with_ffmpeg(input_path, output_path, target_duration):
                logger.error("Failed to extend video")
                return False

            # Upload with new name
            cdn_output_path = os.path.join(os.path.dirname(file_path), output_name)
            with open(output_path, 'rb') as f:
                if await bunny_cdn.upload_file(cdn_output_path, f.read()):
                    logger.info(f"Successfully uploaded extended video: {cdn_output_path}")
                    await bunny_cdn.delete_files([file_path])
                    return True
                
            return False

    except Exception as e:
        logger.error(f"Error processing media: {str(e)}")
        return False

def extend_video_with_ffmpeg(input_path, output_path, target_duration=6.4):
    """
    Extend video by holding last frame.
    
    Args:
        input_path: Input video path
        output_path: Output video path
        target_duration: Target duration in seconds
        
    Returns:
        bool: Success status
        
    Notes:
        - Uses ffmpeg
        - Preserves quality
        - Hardware acceleration
        - Error handling
    """
    try:
        # Get video info
        probe = ffmpeg.probe(input_path)
        orig_duration = float(probe.get('format', {}).get('duration', 0))
        
        if orig_duration >= target_duration:
            logger.info(f"Video is already longer than {target_duration} seconds")
            return False
            
        # Calculate extra duration needed
        extra_duration = target_duration - orig_duration
        
        # Using ffmpeg-python library with proper filter syntax
        stream = ffmpeg.input(input_path)
        
        # Apply tpad filter and use fast encoding settings
        stream = ffmpeg.filter(stream, 'tpad', 
                             stop_mode='clone', 
                             stop_duration=extra_duration)
        
        # Add output settings for faster processing
        stream = ffmpeg.output(stream, 
                             output_path,
                             vcodec='h264_nvenc' if os.path.exists('/dev/nvidia0') else 'libx264',
                             preset='ultrafast',
                             crf=30,
                             acodec='copy',
                             movflags='+faststart')
        
        # Run ffmpeg
        ffmpeg.run(stream, overwrite_output=True)
        
        logger.info(f"Successfully extended video to {target_duration} seconds: {output_path}")
        return True
            
    except ffmpeg.Error as e:
        logger.error(f"FFmpeg error: {e.stderr.decode() if e.stderr else str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error extending video: {str(e)}")
        return False

async def extend_local(input_file: str, target_duration: float = 6.4, output_dir: str = None) -> bool:
    """
    Extend a video locally by holding the last frame
    
    Args:
        input_file: Path to the local video file
        target_duration: Target duration in seconds
        output_dir: Directory to save the output file (defaults to same directory as input)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if not os.path.exists(input_file):
            print(f"Input file not found: {input_file}")
            return False
            
        if not output_dir:
            output_dir = os.path.dirname(input_file) or '.'
            
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Get video info
        probe = ffmpeg.probe(input_file)
        orig_duration = float(probe.get('format', {}).get('duration', 0))
        
        print(f"Video duration: {orig_duration} seconds")
        
        if orig_duration >= target_duration:
            print(f"Video is already longer than {target_duration} seconds")
            return False
            
        # Create the output filename
        input_name = os.path.basename(input_file)
        base_name = os.path.splitext(input_name)[0]
        extension = os.path.splitext(input_name)[1]
        output_name = f"{base_name}-e{extension}"
        output_path = os.path.join(output_dir, output_name)
        
        # Extend the video
        return extend_video_with_ffmpeg(input_file, output_path, target_duration)
        
    except Exception as e:
        print(f"Error processing media: {str(e)}")
        return False

def main():
    # Configure basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Command-line arguments for non-interactive mode
    if len(sys.argv) > 1:
        if sys.argv[1] == '--local-file':
            if len(sys.argv) < 3:
                print("Usage: python extendmedia.py --local-file <input_file> [target_duration] [output_directory]")
                sys.exit(1)
                
            input_file = sys.argv[2]
            target_duration = float(sys.argv[3]) if len(sys.argv) > 3 else 6.4
            output_dir = sys.argv[4] if len(sys.argv) > 4 else None
            
            asyncio.run(extend_local(input_file, target_duration, output_dir))
            sys.exit(0)
    
    # Interactive mode
    while True:
        print("\nMedia Extender - Extend short videos by holding the last frame")
        print("Enter 'q' to quit")
        print("Options:")
        print("1. Process from URL (download and extend locally)")
        print("2. Process local file")
        choice = input("\nEnter choice (1/2/q): ").strip().lower()
        
        if choice == 'q':
            print("Goodbye!")
            break
            
        if choice == '1':
            url = input("Enter media URL: ").strip()
            
            if url.startswith(('http://', 'https://')):
                # Check duration before downloading
                duration = get_media_duration_from_url(url)
                
                if duration == 0:
                    print("Could not determine media duration. Proceeding anyway...")
                    
                elif duration >= 6.4:
                    print(f"Video is already long enough ({duration:.2f} seconds). Skipping...")
                    continue
                else:
                    print(f"Detected short video ({duration:.2f} seconds)")
                    
                input_file = download_media(url)
                target_duration = float(input("Enter target duration in seconds (default: 6.4): ").strip() or "6.4")
                output_dir = input("Enter output directory (press Enter for current directory): ").strip() or None
                
                # Use the local extend function
                success = asyncio.run(extend_local(input_file, target_duration, output_dir))
                if success:
                    print("Processing completed successfully.")
                else:
                    print("Processing failed.")
                
        elif choice == '2':
            input_file = input("Enter path to local media file: ").strip()
            if not os.path.exists(input_file):
                print(f"File not found: {input_file}")
                continue
                
            target_duration = float(input("Enter target duration in seconds (default: 6.4): ").strip() or "6.4")
            output_dir = input("Enter output directory (press Enter for same directory): ").strip() or None
            
            success = asyncio.run(extend_local(input_file, target_duration, output_dir))
            if success:
                print("Processing completed successfully.")
            else:
                print("Processing failed.")
            
        else:
            print("Invalid choice. Please enter 1, 2, or q.")

if __name__ == "__main__":
    main()
