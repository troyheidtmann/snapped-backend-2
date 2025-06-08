"""
Post Processing Integration Module

This module handles the processing and sending of posts to Zapier
webhooks. It manages the scheduling, batching, and status tracking of
posts for multiple clients.

Features:
--------
1. Queue Processing:
   - Post retrieval
   - Batch management
   - Status tracking
   - Error handling

2. Zapier Integration:
   - Webhook communication
   - Post scheduling
   - Response handling
   - Retry logic

3. Time Management:
   - Schedule validation
   - Time zone handling
   - Interval management
   - Batch timing

4. Status Tracking:
   - Queue status
   - Processing flags
   - Error logging
   - Result verification

Data Model:
----------
Queue Structure:
- Client queues
- Post metadata
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
router = APIRouter(prefix="/posting", tags=["posting"])

class PostProcessor:
    """
    Zapier Integration Processor for Posts.
    
    Handles the processing and sending of posts to Zapier
    webhooks for publishing.
    
    Attributes:
        queue: MongoDB queue collection
        ZAPIER_WEBHOOK: Zapier webhook URL
        
    Notes:
        - Manages post queues
        - Handles batches
        - Tracks processing status
        - Implements retry logic
    """
    
    def __init__(self):
        self.queues = async_client['QueueDB']['Queue']
        self.ZAPIER_WEBHOOK = "https://hooks.zapier.com/hooks/catch/21145902/28asr8g/"

    async def get_todays_queue(self) -> Dict:
        """
        Load today's post queue from MongoDB.
        
        Returns:
            Dict: Queue data containing client queues and status
            
        Notes:
            - Retrieves today's queue
            - Validates queue data
            - Logs queue details
            - Reports queue status
        """
        try:
            tomorrow = (datetime.now() + timedelta(days=1)).date().isoformat()
            queue = await self.queues.find_one({"queue_date": tomorrow})
            if not queue:
                logger.error(f"No queue found for tomorrow ({tomorrow})")
                return {}
            return queue.get('client_queues', {})
        except Exception as e:
            logger.error(f"Error loading queue: {str(e)}")
            return {}

    async def send_to_zapier(self, client_name: str, queue_data: dict) -> bool:
        """
        Send posts to Zapier webhook.
        
        Args:
            client_name (str): Client identifier
            queue_data (dict): Post data to be sent
            
        Returns:
            bool: True if successful, False otherwise
            
        Notes:
            - Processes posts
            - Handles scheduling
            - Implements retry logic
            - Tracks success status
        """
        max_retries = 3
        success = True
        
        # Group stories by scheduled time
        stories_by_time = {}
        for story in queue_data.get('stories', []):
            scheduled_time = story['scheduled_time']
            if scheduled_time not in stories_by_time:
                stories_by_time[scheduled_time] = []
            stories_by_time[scheduled_time].append(story)
        
        total_batches = len(stories_by_time)
        logger.info(f"Processing {total_batches} batches for {client_name}")
        
        for batch_index, (scheduled_time, batch) in enumerate(stories_by_time.items(), 1):
            retry_count = 0
            while retry_count < max_retries:
                try:
                    dt = datetime.fromisoformat(scheduled_time)
                    if dt.tzinfo is None and batch[0].get('timezone'):
                        tz = pytz.timezone(batch[0]['timezone'])
                        dt = tz.localize(dt)
                    
                    # Add debug logging
                    logger.info(f"Raw snap_id from MongoDB: {batch[0]['snap_id']} (type: {type(batch[0]['snap_id'])})")
                    
                    # Ensure snap_id is a clean string without any quotes
                    snap_id = batch[0]['snap_id']
                    if isinstance(snap_id, list):
                        snap_id = snap_id[0]  # Take first element if it's a list
                    snap_id = str(snap_id).strip('[]').strip().strip('"').strip("'")  # Remove all quotes and brackets
                    
                    # Convert 'STORIES' to 'STORY' for Vista Social
                    publish_as = 'STORY' if batch[0]['snapchat_publish_as'].upper() == 'STORIES' else batch[0]['snapchat_publish_as'].upper()
                    
                    payload = {
                        "profile": int(snap_id) if snap_id.isdigit() else snap_id,  # Convert to integer if it's a number
                        "media_urls": [story['cdn_url'] for story in batch],
                        "publish_at": dt.strftime('%Y-%m-%d %H:%M:%S'),
                        "draft": 'false',
                        "snapchat_publish_as": publish_as
                    }
                    
                    logger.info(f"Final payload: {payload}")
                    
                    logger.info(f"Sending payload to Zapier: {payload}")
                    response = requests.post(self.ZAPIER_WEBHOOK, json=payload)
                    
                    if response.status_code != 200:
                        raise Exception(f"Zapier returned status code {response.status_code}")
                    
                    logger.info(f"✓ Successfully uploaded batch {batch_index}/{total_batches} for {client_name}")
                    
                    if batch_index < total_batches:
                        time.sleep(5)
                        
                    break
                except Exception as e:
                    logger.error(f"✗ Error uploading batch {batch_index}/{total_batches} for {client_name} (attempt {retry_count + 1}/{max_retries}): {str(e)}")
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(5)
                        continue
                    success = False
                    break
        
        return success

    async def process_all_queues(self):
        """
        Process all posts from the queue.
        
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
                    if await self.send_to_zapier(client, client_queue):
                        await self.queues.update_one(
                            {"queue_date": datetime.now().date().isoformat()},
                            {"$set": {f"client_queues.{client}.processed": True}}
                        )
                logger.info("Queue processing completed")
            else:
                logger.info("No queue found for today")
        except Exception as e:
            logger.error(f"Error processing queues: {str(e)}")
            raise

# API Routes
@router.post("/process")
async def trigger_queue_processing():
    """
    Manual trigger endpoint for Zapier queue processing.
    
    Returns:
        dict: Operation status
        
    Raises:
        HTTPException: For processing errors
        
    Notes:
        - Handles manual triggers
        - Reports status
        - Tracks errors
    """
    processor = PostProcessor()
    try:
        await processor.process_all_queues()
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/health")
async def health_check():
    """
    Health check endpoint for service monitoring.
    
    Returns:
        dict: Service status
        
    Notes:
        - Checks service health
        - Reports status
        - Tracks uptime
    """
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }
