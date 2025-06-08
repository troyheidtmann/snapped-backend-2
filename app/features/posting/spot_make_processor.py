"""
Spot Posts Make.com Integration Module

This module handles the processing and sending of spot posts to Make.com
webhooks. It manages the scheduling, batching, and status tracking of
spot story posts for Snapchat integration.

Features:
--------
1. Queue Processing:
   - Spot posts retrieval
   - Batch management
   - Status tracking
   - Error handling

2. Make.com Integration:
   - Webhook communication
   - Story scheduling
   - Response handling
   - Retry logic

3. Time Management:
   - Immediate scheduling
   - Interval management
   - Batch timing
   - UTC handling

4. Status Tracking:
   - Queue processing
   - Success monitoring
   - Error logging
   - Result verification

Data Model:
----------
Queue Structure:
- Client queues
- Spot posts
- Schedule timing
- Processing status

Security:
--------
- Webhook authentication
- Error handling
- Rate limiting
- Status tracking

Dependencies:
-----------
- FastAPI: API framework
- requests: HTTP client
- MongoDB: Data storage
- pytz: Timezone handling
- logging: Debug tracking

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException
import requests
import logging
from datetime import datetime, timedelta
import pytz
from typing import List, Dict
import time
from app.shared.database import async_client

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api/spot-queue", tags=["posting"])

class SpotMakeProcessor:
    """
    Make.com Integration Processor for Spot Posts.
    
    Handles the processing and sending of spot posts to Make.com
    webhooks for Snapchat story publishing.
    
    Attributes:
        spot_queue: MongoDB spot queue collection
        MAKE_WEBHOOK: Make.com webhook URL
        
    Notes:
        - Manages spot posts
        - Handles story batches
        - Tracks processing status
        - Implements retry logic
    """
    
    def __init__(self):
        self.spot_queue = async_client['QueueDB']['SpotQueue']
        self.MAKE_WEBHOOK = "https://hook.us2.make.com/6mv68gk7h51xn3l9i2ftd173qyt2xcwx"  # Updated webhook URL

    async def get_todays_queue(self) -> Dict:
        """Load today's spotlight queue from MongoDB"""
        try:
            today = datetime.now(pytz.UTC).date().isoformat()  # Match the format from queue builder
            logger.info(f"Looking for spotlight queue with date: {today}")
            
            queue = await self.spot_queue.find_one({"queue_date": today})
            if not queue:
                logger.error(f"No spotlight queue found for today ({today})")
                return {}
            
            logger.info(f"Found spotlight queue with {len(queue.get('client_queues', {}))} clients")
            return queue.get('client_queues', {})
        except Exception as e:
            logger.error(f"Error loading spotlight queue: {str(e)}")
            return {}

    def calculate_schedule_time(self, base_time: datetime, index: int) -> str:
        """
        Calculate the scheduled time for each post with 4-minute intervals.
        
        Args:
            base_time (datetime): Base time for scheduling
            index (int): Post index for interval calculation
            
        Returns:
            str: ISO formatted scheduled time
            
        Notes:
            - Uses 4-minute intervals
            - Starts immediately
            - Handles UTC conversion
            - Maintains consistent spacing
        """
        scheduled_time = base_time + timedelta(minutes=4 * index)
        return scheduled_time.isoformat()

    async def send_to_make(self, client_name: str, queue_data: dict) -> bool:
        """
        Send spot posts to Make.com webhook.
        
        Args:
            client_name (str): Client identifier
            queue_data (dict): Post data to be sent
            
        Returns:
            bool: True if successful, False otherwise
            
        Notes:
            - Processes spot posts
            - Handles scheduling
            - Implements retry logic
            - Tracks success status
        """
        max_retries = 3
        success = True
        
        posts = queue_data.get('posts', [])
        total_posts = len(posts)
        logger.info(f"Processing {total_posts} spotlight posts for {client_name}")
        
        for post_index, post in enumerate(posts, 1):
            retry_count = 0
            while retry_count < max_retries:
                try:
                    # Parse the scheduled time directly - it's already in UTC format
                    scheduled_time = post['scheduled_time']
                    
                    payload = {
                        "profile": post['snap_id'],
                        "media_urls": [post['cdn_url']],
                        "publish_at": scheduled_time,
                        "draft": 'false',
                        "snapchat_publish_as": 'SPOTLIGHT',
                        "caption": post.get('caption') or '#spotlight'  # Default caption if none provided
                    }
                    
                    logger.info(f"Sending spotlight payload to Make for file {post['file_name']}: {payload}")
                    response = requests.post(self.MAKE_WEBHOOK, json=payload)
                    
                    if response.status_code != 200:
                        raise Exception(f"Make returned status code {response.status_code}")
                    
                    logger.info(f"✓ Successfully uploaded spotlight post {post_index}/{total_posts} ({post['file_name']}) for {client_name}")
                    
                    if post_index < total_posts:
                        logger.info(f"Waiting 10 seconds before next post...")
                        time.sleep(10)
                        
                    break
                except Exception as e:
                    logger.error(f"✗ Error uploading spotlight post {post_index}/{total_posts} for {client_name} (attempt {retry_count + 1}/{max_retries}): {str(e)}")
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(5)
                        continue
                    success = False
                    break
        
        return success

    async def process_all_queues(self):
        """
        Process all spot posts from the queue.
        
        Notes:
            - Processes all clients
            - Updates queue status
            - Tracks completion
            - Handles errors
        """
        try:
            queue = await self.get_todays_queue()
            if queue:
                for client, client_queue in queue.items():
                    if await self.send_to_make(client, client_queue):
                        await self.spot_queue.update_one(
                            {"queue_date": datetime.now().date().isoformat()},
                            {"$set": {f"client_queues.{client}.processed": True}}
                        )
                logger.info("Spotlight queue processing completed")
            else:
                logger.info("No spotlight queue found for today")
        except Exception as e:
            logger.error(f"Error processing spotlight queues: {str(e)}")
            raise

# API Routes
@router.post("/process-spot-make")
async def trigger_spot_make_processing():
    """
    Manual trigger endpoint for Make.com spot posts queue processing.
    
    Returns:
        dict: Operation status
        
    Raises:
        HTTPException: For processing errors
        
    Notes:
        - Handles manual triggers
        - Reports status
        - Tracks errors
    """
    processor = SpotMakeProcessor()
    try:
        await processor.process_all_queues()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/process-make")
async def process_make_queue():
    """
    Process spot posts queue through Make.com.
    
    Returns:
        dict: Operation status and details
        
    Raises:
        HTTPException: For processing errors
        
    Notes:
        - Processes spot posts
        - Records timestamps
        - Tracks client count
        - Handles errors
    """
    try:
        processor = SpotMakeProcessor()
        result = await processor.process_all_queues()
        
        return {
            "status": "success",
            "message": "Processed spotlight queue",
            "details": {
                "processed_at": datetime.now().isoformat(),
                "total_clients": len(result) if result else 0
            }
        }
    except Exception as e:
        logger.error(f"Error processing spotlight queue: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process spotlight queue: {str(e)}"
        ) 