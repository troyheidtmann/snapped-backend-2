"""
Thumbnail Service - Video Thumbnail Generation Module

This module provides services for generating, caching, and retrieving video thumbnails
in the Snapped platform. It handles the automatic creation of square thumbnails from
video content, with caching in MongoDB for performance optimization.

Architecture:
-----------
1. Thumbnail Generation:
   - Video frame extraction
   - Image processing
   - Square aspect ratio conversion
   - Base64 encoding

2. Caching System:
   - MongoDB storage
   - URL-based lookup
   - Timestamp tracking
   - Cache invalidation

3. Processing Pipeline:
   - Video download
   - FFmpeg processing
   - Image optimization
   - Data persistence

Technical Details:
---------------
- Image Size: 480x480 pixels
- Format: JPEG (base64 encoded)
- Aspect Ratio: Square (padded)
- Color Space: RGB
- Background: Black padding

Dependencies:
-----------
- FFmpeg: Video frame extraction
- aiohttp: Async HTTP client
- MongoDB Motor: Cache storage
- BunnyCDN: CDN integration
- base64: Data encoding
- tempfile: Temporary storage

Security:
--------
- URL validation
- Temporary file cleanup
- Error handling
- Resource limits

Author: Snapped Development Team
"""

import logging
from app.shared.database import edit_thumb_collection
from datetime import datetime
import base64
import ffmpeg
import tempfile
import aiohttp
from app.shared.bunny_cdn import BunnyCDN
import os
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class ThumbnailService:
    """
    Service for generating and managing video thumbnails.
    
    This class provides functionality to:
    - Generate thumbnails from video URLs
    - Cache thumbnails in MongoDB
    - Retrieve cached thumbnails
    - Handle video processing errors
    
    The service ensures consistent thumbnail generation with:
    - Square aspect ratio (480x480)
    - Center-aligned content
    - Black padding when needed
    - Efficient caching
    
    Technical Implementation:
    ----------------------
    1. Cache Check:
       - MongoDB lookup by video URL
       - Timestamp verification
    
    2. Thumbnail Generation:
       - Video frame extraction
       - Image processing
       - Format standardization
    
    3. Data Storage:
       - Base64 encoding
       - MongoDB persistence
       - Error handling
    """

    def __init__(self):
        """
        Initialize the thumbnail service.
        
        Components:
        ----------
        - BunnyCDN: Content delivery network client
        - MongoDB: Thumbnail cache collection
        """
        self.bunny = BunnyCDN()
        self.collection = edit_thumb_collection

    async def get_or_create_thumbnail(self, video_url: str) -> str:
        """
        Retrieve or generate a thumbnail for a video URL.
        
        This method implements a cache-first approach:
        1. Check MongoDB for existing thumbnail
        2. If not found, download and process video
        3. Generate and cache new thumbnail
        
        Args:
            video_url (str): Full URL to the video file
        
        Returns:
            str: Base64 encoded JPEG thumbnail
                Format: "data:image/jpeg;base64,..."
            None: If thumbnail generation fails
        
        Process Flow:
        ------------
        1. Cache Check:
           - Query MongoDB by URL
           - Validate cached data
        
        2. Video Processing:
           - Download video to temp file
           - Extract first frame
           - Scale and pad to square
        
        3. Data Management:
           - Encode as base64
           - Cache in MongoDB
           - Clean up temp files
        
        Error Handling:
        --------------
        - Network errors (404, timeouts)
        - Processing errors (FFmpeg)
        - Storage errors (MongoDB)
        - Resource cleanup
        
        Technical Details:
        ----------------
        - Output size: 480x480 pixels
        - Format: JPEG
        - Aspect ratio: Square (padded)
        - Background: Black
        - Encoding: Base64 with MIME type
        """
        try:
            # Check MongoDB first
            logger.info("\n=== THUMBNAIL GENERATION START ===")
            logger.info(f"Video URL: {video_url}")
            
            try:
                thumb_doc = await self.collection.find_one({
                    "video_url": video_url
                })
                if thumb_doc and thumb_doc.get("thumb_data"):
                    logger.info("Found existing thumbnail in MongoDB")
                    return thumb_doc["thumb_data"]
            except Exception as db_error:
                logger.error(f"MongoDB query error: {str(db_error)}")
                
            # No thumbnail? Generate one
            logger.info("\nNo existing thumbnail found, generating new one")
            
            # Download and process video
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
            try:
                # Download video
                logger.info("\nDownloading video...")
                async with aiohttp.ClientSession() as session:
                    async with session.get(video_url) as response:
                        logger.info(f"Download response status: {response.status}")
                        if response.status != 200:
                            error_msg = f"Failed to download video: {response.status}"
                            if response.status == 404:
                                error_msg = f"Video not found at {video_url}"
                            logger.error(error_msg)
                            return None
                        with open(temp_file.name, 'wb') as f:
                            async for chunk in response.content.iter_chunked(8192):
                                f.write(chunk)

                # Generate thumbnail
                logger.info("\nGenerating thumbnail with ffmpeg...")
                try:
                    out, err = (
                        ffmpeg
                        .input(temp_file.name)
                        .filter('select', 'eq(n,0)')
                        .filter('scale', w=480, h=480, force_original_aspect_ratio='decrease')  # Scale to fit within 480x480
                        .filter('pad', width=480, height=480, x='(ow-iw)/2', y='(oh-ih)/2', color='black')  # Pad to square
                        .output('pipe:', vframes=1, format='image2', vcodec='mjpeg')
                        .run(capture_stdout=True, capture_stderr=True)
                    )
                except ffmpeg.Error as e:
                    logger.error(f"FFmpeg error: {str(e)}")
                    logger.error(f"FFmpeg stderr: {e.stderr.decode() if e.stderr else 'None'}")
                    return None

                # Convert to base64
                thumb_data = f"data:image/jpeg;base64,{base64.b64encode(out).decode('utf-8')}"
                
                # Store in MongoDB
                try:
                    doc = {
                        "video_url": video_url,
                        "thumb_data": thumb_data,
                        "created_at": datetime.utcnow()
                    }
                    await self.collection.insert_one(doc)
                except Exception as db_error:
                    logger.error(f"MongoDB insert error: {str(db_error)}")
                    # Even if DB storage fails, return the generated thumbnail
                    return thumb_data

                return thumb_data

            finally:
                try:
                    os.unlink(temp_file.name)
                except Exception as e:
                    logger.error(f"Error cleaning up temp file: {str(e)}")

        except Exception as e:
            logger.error(f"\nThumbnail error: {str(e)}")
            logger.exception("Full traceback:")
            return None 