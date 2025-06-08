"""
TikTok Video Processing Module

This module handles TikTok video downloading, processing, and storage functionality,
including progress tracking and S3 integration.

Features:
- Video downloading
- Progress tracking
- S3 upload management
- Status monitoring
- Error handling
- Client tracking

Data Model:
- Video metadata
- Progress tracking
- Upload status
- Client information
- File organization

Security:
- S3 encryption
- Access control
- File validation
- Error handling

Dependencies:
- FastAPI for routing
- yt-dlp for downloads
- boto3 for S3
- aiohttp for async
- logging for tracking

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException, Response
from app.shared.database import async_client, spotlight_collection
import aiohttp
import aiofiles
import os
from datetime import datetime
import json
import logging
import yt_dlp
import asyncio
from sse_starlette.sse import EventSourceResponse
from app.features.desktop_upload.desktop_upload import S3UploadManager
from io import BytesIO
import boto3
import urllib.parse
import tempfile

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tiktok")

# Add a dictionary to store progress
progress_store = {}

# Initialize S3 manager
s3_manager = S3UploadManager()

async def download_tiktok_video(url, client_id, current_video=1, total_videos=1):
    """
    Download a TikTok video using yt-dlp.
    
    Args:
        url (str): TikTok video URL
        client_id (str): Client identifier
        current_video (int): Current video number
        total_videos (int): Total videos to process
        
    Returns:
        tuple: (video_data, video_info) or (None, None) on failure
        
    Notes:
        - Uses temporary directory
        - Tracks download progress
        - Updates progress store
        - Handles download errors
    """
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = os.path.join(temp_dir, f"video_{datetime.now().strftime('%Y%m%d_%H%M%S')}.mp4")
            
            def progress_hook(d):
                if d['status'] == 'downloading':
                    total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
                    downloaded = d.get('downloaded_bytes', 0)
                    speed = d.get('speed', 0)
                    eta = d.get('eta', 0)
                    
                    # Calculate progress percentage
                    progress = (downloaded / total * 100) if total > 0 else 0
                    
                    # Update progress store with detailed information
                    progress_store[client_id].update({
                        "downloaded_bytes": downloaded,
                        "total_bytes": total,
                        "speed": speed,
                        "eta": eta,
                        "download_progress": progress,
                        "status": f"Downloading video... {progress:.1f}%",
                        "currentFile": f"Video {current_video} of {total_videos}",
                        "current": current_video,
                        "total": total_videos
                    })
                    
                    # Log progress for debugging
                    logger.debug(f"Download progress: {progress:.1f}% ({downloaded}/{total} bytes)")
                
                elif d['status'] == 'finished':
                    progress_store[client_id].update({
                        "download_progress": 100,
                        "status": "Download complete, preparing for upload...",
                        "currentFile": f"Video {current_video} of {total_videos}",
                        "current": current_video,
                        "total": total_videos
                    })
                    logger.debug(f"Download finished for video {current_video}")
                
                elif d['status'] == 'error':
                    error_msg = d.get('error', 'Unknown error')
                    logger.error(f"Download error: {error_msg}")
                    progress_store[client_id].update({
                        "status": f"Error: {error_msg}",
                        "currentFile": f"Video {current_video} of {total_videos}"
                    })
            
            ydl_opts = {
                'format': 'best',
                'progress_hooks': [progress_hook],
                'outtmpl': temp_file,
                'quiet': True
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                try:
                    info = ydl.extract_info(url, download=True)
                    if not info:
                        logger.error("No info returned from yt-dlp")
                        return None, None
                    
                    with open(temp_file, 'rb') as f:
                        return f.read(), info
                except Exception as e:
                    logger.error(f"Error in yt-dlp download: {str(e)}")
                    return None, None
                    
    except Exception as e:
        logger.error(f"Download error: {str(e)}")
        return None, None

async def upload_to_s3(video_data, client_id, sequence_number, upload_path):
    """
    Upload video data to S3 with progress tracking.
    
    Args:
        video_data (bytes): Video file data
        client_id (str): Client identifier
        sequence_number (int): Video sequence number
        upload_path (str): S3 upload path
        
    Returns:
        dict: Upload metadata or None on failure
        
    Notes:
        - Uses AWS SDK
        - Tracks upload progress
        - Sets metadata
        - Generates CDN URL
    """
    try:
        # Create a callback class to track upload progress
        class ProgressCallback:
            def __init__(self, client_id, sequence_number, total_bytes):
                self._client_id = client_id
                self._sequence_number = sequence_number
                self._total_bytes = total_bytes
                self._seen_bytes = 0

            def __call__(self, bytes_amount):
                self._seen_bytes += bytes_amount
                percentage = (self._seen_bytes / self._total_bytes) * 100
                if self._client_id in progress_store:
                    current_progress = progress_store[self._client_id]
                    update = {
                        "current": self._sequence_number,
                        "total": current_progress["total"],
                        "currentFile": f"Video {self._sequence_number} of {current_progress['total']}",
                        "status": f"{percentage:.1f}%"
                    }
                    progress_store[self._client_id].update(update)

        # Use AWS SDK's TransferConfig for uploads
        transfer_config = boto3.s3.transfer.TransferConfig(
            use_threads=True,
            max_concurrency=10,
            multipart_threshold=8 * 1024 * 1024,
            multipart_chunksize=8 * 1024 * 1024,
            max_bandwidth=None
        )
        
        timestamp = datetime.now().strftime('%m%d-%H%M')
        sequence_str = f"{sequence_number:04d}"
        key = f"public/{client_id}/SPOT/TIKTOKS/{upload_path}/{sequence_str}-{timestamp}.mp4"
        
        file_obj = BytesIO(video_data)
        total_bytes = len(video_data)
        progress_callback = ProgressCallback(client_id, sequence_number, total_bytes)
        
        tags = urllib.parse.urlencode({
            'SPOT': 'true'
        })
        
        s3_manager.s3_client.upload_fileobj(
            file_obj,
            s3_manager.bucket_name,
            key,
            ExtraArgs={
                'ContentType': 'video/mp4',
                'ServerSideEncryption': 'AES256',
                'Metadata': {
                    'timestamp': datetime.utcnow().isoformat(),
                    'sequence': sequence_str
                },
                'Tagging': tags,
                'ChecksumAlgorithm': 'SHA256'
            },
            Config=transfer_config,
            Callback=progress_callback
        )
        
        head = s3_manager.s3_client.head_object(
            Bucket=s3_manager.bucket_name,
            Key=key
        )
        
        cloudfront_url = f"https://c.snapped.cc/{key}"
        
        return {
            "key": key,
            "url": cloudfront_url,
            "cdn_url": cloudfront_url,
            "etag": head['ETag'],
            "version_id": head.get('VersionId'),
            "size": head['ContentLength'],
            "last_modified": head['LastModified'].isoformat(),
            "checksum": head.get('ChecksumSHA256'),
            "server_side_encryption": head.get('ServerSideEncryption'),
            "metadata": head.get('Metadata', {})
        }
        
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        return None

async def get_profile_videos(profile_url):
    """
    Get list of videos from a TikTok profile.
    
    Args:
        profile_url (str): TikTok profile URL
        
    Returns:
        list: List of video information dictionaries
        
    Notes:
        - Uses yt-dlp
        - Extracts metadata
        - Handles errors
        - Returns empty list on failure
    """
    try:
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
            'dump_single_json': True,
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(profile_url, download=False)
            if 'entries' in info:
                return [{
                    'url': entry['url'],
                    'caption': entry.get('description', ''),
                    'timestamp': entry.get('timestamp', ''),
                    'view_count': entry.get('view_count', 0),
                    'like_count': entry.get('like_count', 0),
                    'comment_count': entry.get('comment_count', 0),
                    'duration': entry.get('duration', 0)
                } for entry in info['entries']]
            return [{
                'url': info['url'],
                'caption': info.get('description', ''),
                'timestamp': info.get('timestamp', ''),
                'view_count': info.get('view_count', 0),
                'like_count': info.get('like_count', 0),
                'comment_count': info.get('comment_count', 0),
                'duration': info.get('duration', 0)
            }]
    except Exception as e:
        logger.error(f"Error getting profile videos: {str(e)}")
        return []

@router.options("/progress/{client_id}")
async def progress_options(client_id: str):
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Max-Age": "86400",
        }
    )

@router.get("/progress/{client_id}")
async def progress_stream(client_id: str):
    """
    Stream progress updates for video processing.
    
    Args:
        client_id (str): Client identifier
        
    Returns:
        EventSourceResponse: Server-sent events stream
        
    Notes:
        - Uses SSE
        - Real-time updates
        - Includes progress details
        - Handles connection
    """
    async def event_generator():
        while True:
            if client_id in progress_store:
                yield f"data: {json.dumps(progress_store[client_id])}\n\n"
            await asyncio.sleep(0.1)
            
    return EventSourceResponse(event_generator())

@router.get("/progress-check/{client_id}")
async def check_progress(client_id: str):
    current_progress = progress_store.get(client_id, {
        "current": 0,
        "total": 0,
        "currentFile": "",
        "status": "Waiting to start..."
    })
    
    return current_progress

@router.post("/download")
async def download_tiktoks(request_data: dict):
    """
    Handle TikTok video download and processing request.
    
    Args:
        request_data (dict): Request data with client_id and tiktok_url
        
    Returns:
        dict: Processing results and file information
        
    Raises:
        HTTPException: For processing errors
        
    Notes:
        - Downloads videos
        - Tracks progress
        - Uploads to S3
        - Updates database
    """
    client_id = request_data["client_id"]
    tiktok_url = request_data["tiktok_url"]
    upload_path = datetime.now().strftime('%Y%m%d')
    uploaded_files = []
    
    try:
        # Initialize progress
        progress_store[client_id] = {
            "status": "Getting video list...",
            "current": 0,
            "total": 0,
            "downloaded_bytes": 0,
            "total_bytes": 0,
            "speed": 0,
            "download_progress": 0
        }
        
        # Get video list
        videos = await get_profile_videos(tiktok_url)
        total_videos = len(videos)
        progress_store[client_id]["total"] = total_videos
        
        # Download and upload each video
        for idx, video in enumerate(videos, 1):
            progress_store[client_id].update({
                "current": idx,
                "status": f"Starting download for video {idx}/{total_videos}"
            })
            
            # Download video
            video_data, info = await download_tiktok_video(video['url'], client_id, idx, total_videos)
            if not video_data or not info:
                continue
                
            # Upload to S3
            progress_store[client_id]["status"] = f"Uploading video {idx}/{total_videos}"
            result = await upload_to_s3(video_data, client_id, idx, upload_path)
            
            if result:
                uploaded_files.append({
                    **result,
                    'caption': video['caption'],
                    'stats': {
                        'views': video['view_count'],
                        'likes': video['like_count'],
                        'comments': video['comment_count']
                    }
                })
        
        # Update database
        progress_store[client_id]["status"] = "Saving to database..."
        
        # Format files with consistent field names
        formatted_files = []
        for file in uploaded_files:
            file_name = file['key'].split('/')[-1]  # Extract filename from key
            formatted_files.append({
                "file_name": file_name,  # Use consistent file_name field
                "cdn_url": file['cdn_url'],
                "etag": file['etag'],
                "version_id": file['version_id'],
                "size": file['size'],
                "last_modified": file['last_modified'],
                "checksum": file['checksum'],
                "server_side_encryption": file['server_side_encryption'],
                "metadata": file['metadata'],
                "caption": file.get('caption', ''),
                "stats": file.get('stats', {}),
                "queued": False,  # Initialize as not queued
                "queue_time": None  # Initialize with no queue time
            })
        
        await spotlight_collection.update_one(
            {"client_ID": client_id},
            {
                "$push": {
                    "tt_sessions": {
                        "session_id": f"TT_{datetime.now().strftime('%Y%m%d')}_{client_id}",
                        "upload_date": datetime.utcnow(),
                        "folder_path": f"public/{client_id}/SPOT/TIKTOKS/{upload_path}",
                        "total_videos": len(formatted_files),
                        "files": formatted_files
                    }
                }
            },
            upsert=True
        )
        
        progress_store[client_id]["status"] = "Complete!"
        return {"status": "success", "files": uploaded_files}
        
    except Exception as e:
        if client_id in progress_store:
            progress_store[client_id]["status"] = f"Error: {str(e)}"
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
        
    finally:
        # Clean up progress after delay
        await asyncio.sleep(2)
        if client_id in progress_store:
            del progress_store[client_id] 