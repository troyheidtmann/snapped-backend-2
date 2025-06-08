"""
Saved Posts Make.com Integration Module

This module handles the processing and sending of saved posts to Make.com
webhooks. It manages the scheduling, batching, and status tracking of
saved story posts for Snapchat integration.

Features:
--------
1. Queue Processing:
   - Saved posts retrieval
   - Batch management
   - Status tracking
   - Error handling

2. Make.com Integration:
   - Webhook communication
   - Story scheduling
   - Response handling
   - Retry logic

3. Time Management:
   - Late night scheduling
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
- Saved posts
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
router = APIRouter(prefix="/api/saved-queue", tags=["posting"])

class SavedMakeProcessor:
    """
    Make.com Integration Processor for Saved Posts.
    
    Handles the processing and sending of saved posts to Make.com
    webhooks for Snapchat story publishing.
    
    Attributes:
        saved_queue: MongoDB saved queue collection
        MAKE_WEBHOOK: Make.com webhook URL
        
    Notes:
        - Manages saved posts
        - Handles story batches
        - Tracks processing status
        - Implements retry logic
    """
    
    def __init__(self):
        self.saved_queue = async_client['QueueDB']['SavedQueue']
        self.MAKE_WEBHOOK = "https://hook.us2.make.com/7cqutngufim36r18w01rgixdv2ymcuqa"

    async def get_todays_queue(self) -> Dict:
        """
        Load today's saved posts queue from MongoDB.
        
        Returns:
            Dict: Queue data containing client queues and status
            
        Notes:
            - Retrieves today's queue
            - Validates queue data
            - Logs queue details
            - Reports queue status
        """
        try:
            today = datetime.now(pytz.UTC).date().isoformat()
            logger.info(f"Looking for saved posts queue with date: {today}")
            
            queue = await self.saved_queue.find_one({"queue_date": today})
            if not queue:
                logger.error(f"No saved posts queue found for today ({today})")
                return {}
            
            logger.info(f"Found saved posts queue with {len(queue.get('client_queues', {}))} clients")
            return queue.get('client_queues', {})
        except Exception as e:
            logger.error(f"Error loading saved posts queue: {str(e)}")
            return {}

    def calculate_schedule_time(self, base_time: datetime, index: int) -> str:
        """
        Calculate the scheduled time for each post with 4-minute intervals.
        
        Args:
            base_time (datetime): Base time for scheduling (11 PM)
            index (int): Post index for interval calculation
            
        Returns:
            str: ISO formatted scheduled time
            
        Notes:
            - Uses 4-minute intervals
            - Starts at 11 PM
            - Handles UTC conversion
            - Maintains consistent spacing
        """
        scheduled_time = base_time + timedelta(minutes=4 * index)
        return scheduled_time.isoformat()

    async def send_to_make(self, client_name: str, queue_data: dict) -> bool:
        """
        Send saved posts to Make.com webhook.
        
        Args:
            client_name (str): Client identifier
            queue_data (dict): Post data to be sent
            
        Returns:
            bool: True if successful, False otherwise
            
        Notes:
            - Processes saved posts
            - Handles scheduling
            - Implements retry logic
            - Tracks success status
        """
        max_retries = 3
        success = True
        
        posts = queue_data.get('posts', [])
        total_posts = len(posts)
        logger.info(f"Processing {total_posts} saved posts for {client_name}")
        
        # Calculate base time (11 PM UTC today)
        base_time = datetime.now(pytz.UTC).replace(hour=23, minute=0, second=0, microsecond=0)
        
        for post_index, post in enumerate(posts):
            retry_count = 0
            while retry_count < max_retries:
                try:
                    # Calculate scheduled time for this post
                    scheduled_time = self.calculate_schedule_time(base_time, post_index)
                    
                    payload = {
                        "profile": post['snap_id'],
                        "media_urls": [post['cdn_url']],
                        "publish_at": scheduled_time,
                        "draft": 'false',
                        "snapchat_publish_as": 'STORY',  # Posts as story for saved posts
                        "caption": post.get('caption', '')  # Empty string if no caption
                    }
                    
                    logger.info(f"Sending saved post payload to Make for file {post['file_name']}: {payload}")
                    response = requests.post(self.MAKE_WEBHOOK, json=payload)
                    
                    if response.status_code != 200:
                        raise Exception(f"Make returned status code {response.status_code}")
                    
                    logger.info(f"✓ Successfully uploaded saved post {post_index + 1}/{total_posts} ({post['file_name']}) for {client_name}")
                    
                    if post_index < total_posts - 1:
                        logger.info("Waiting 5 seconds before next post...")
                        time.sleep(5)
                        
                    break
                except Exception as e:
                    logger.error(f"✗ Error uploading saved post {post_index + 1}/{total_posts} for {client_name} (attempt {retry_count + 1}/{max_retries}): {str(e)}")
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(5)
                        continue
                    success = False
                    break
        
        return success

    async def process_all_queues(self):
        """
        Process all saved posts from the queue.
        
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
                        await self.saved_queue.update_one(
                            {"queue_date": datetime.now().date().isoformat()},
                            {"$set": {f"client_queues.{client}.processed": True}}
                        )
                logger.info("Saved posts queue processing completed")
            else:
                logger.info("No saved posts queue found for today")
        except Exception as e:
            logger.error(f"Error processing saved posts queues: {str(e)}")
            raise

# API Routes
@router.post("/process-saved-make")
async def trigger_saved_make_processing():
    """
    Manual trigger endpoint for Make.com saved posts queue processing.
    
    Returns:
        dict: Operation status
        
    Raises:
        HTTPException: For processing errors
        
    Notes:
        - Handles manual triggers
        - Reports status
        - Tracks errors
    """
    processor = SavedMakeProcessor()
    try:
        await processor.process_all_queues()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/process-make")
async def process_make_queue():
    """
    Process saved posts queue through Make.com.
    
    Returns:
        dict: Operation status and details
        
    Raises:
        HTTPException: For processing errors
        
    Notes:
        - Processes saved posts
        - Records timestamps
        - Tracks client count
        - Handles errors
    """
    try:
        processor = SavedMakeProcessor()
        result = await processor.process_all_queues()
        
        return {
            "status": "success",
            "message": "Processed saved posts queue",
            "details": {
                "processed_at": datetime.now().isoformat(),
                "total_clients": len(result) if result else 0
            }
        }
    except Exception as e:
        logger.error(f"Error processing saved posts queue: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process saved posts queue: {str(e)}"
        ) 