"""
Make.com Integration Processor Module

This module handles the processing and sending of queued posts to Make.com
webhooks. It manages the scheduling, batching, and status tracking of
story posts for Snapchat integration.

Features:
--------
1. Queue Processing:
   - Daily queue retrieval
   - Batch management
   - Status tracking
   - Error handling

2. Make.com Integration:
   - Webhook communication
   - Story scheduling
   - Response handling
   - Retry logic

3. Time Management:
   - UTC conversion
   - Timezone handling
   - Schedule coordination
   - Batch timing

4. Status Tracking:
   - Queue processing
   - Success monitoring
   - Error logging
   - Result verification

Data Model:
----------
Queue Structure:
- Client queues
- Story batches
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

class MakeProcessor:
    """
    Make.com Integration Processor.
    
    Handles the processing and sending of queued posts to Make.com
    webhooks for Snapchat story publishing.
    
    Attributes:
        queues: MongoDB queue collection
        MAKE_WEBHOOK: Make.com webhook URL
        
    Notes:
        - Manages daily queues
        - Handles story batches
        - Tracks processing status
        - Implements retry logic
    """
    
    def __init__(self):
        self.queues = async_client['QueueDB']['Queue']
        self.MAKE_WEBHOOK = "https://hook.us2.make.com/fheaw13hclbts7ght5n8tmvl8r57qldj"

    async def get_todays_queue(self, target_date: str = None) -> Dict:
        """
        Load queue from MongoDB for today or specified date.
        
        Args:
            target_date (str, optional): Specific date to load queue for
            
        Returns:
            Dict: Queue data containing client queues and status
            
        Notes:
            - Handles date targeting
            - Validates queue data
            - Logs queue details
            - Reports queue status
        """
        try:
            # Use provided date or default to today
            queue_date = target_date if target_date else datetime.now(pytz.UTC).date().isoformat()
            logger.info(f"Looking for queue with date: {queue_date}")
            
            # Log the full query result
            queue = await self.queues.find_one({"queue_date": queue_date})
            logger.info(f"Raw queue from database: {queue}")
            
            if not queue:
                logger.error(f"No queue found for date ({queue_date})")
                return {}
            
            client_queues = queue.get('client_queues', {})
            logger.info(f"Found queue with {len(client_queues)} clients")
            logger.info(f"Client queue details: {client_queues}")
            return client_queues
        except Exception as e:
            logger.error(f"Error loading queue: {str(e)}")
            return {}

    async def send_to_make(self, client_name: str, queue_data: dict) -> bool:
        """
        Send posts to Make.com webhook in batches by scheduled time.
        
        Args:
            client_name (str): Client identifier
            queue_data (dict): Story data to be sent
            
        Returns:
            bool: True if successful, False otherwise
            
        Notes:
            - Processes stories individually
            - Handles timezone conversion
            - Implements retry logic
            - Tracks success status
        """
        max_retries = 3
        success = True
        
        # Process each story individually instead of grouping by time
        stories = queue_data.get('stories', [])
        total_stories = len(stories)
        logger.info(f"Processing {total_stories} stories for {client_name}")
        
        for story_index, story in enumerate(stories, 1):
            retry_count = 0
            while retry_count < max_retries:
                try:
                    dt = datetime.fromisoformat(story['scheduled_time'])
                    if dt.tzinfo is None and story.get('timezone'):
                        tz = pytz.timezone(story['timezone'])
                        dt = tz.localize(dt)
                    
                    # Convert to UTC and format with Z
                    utc_time = dt.astimezone(pytz.UTC)
                    
                    payload = {
                        "profile": story['snap_id'],
                        "media_urls": [story['cdn_url']],  # Single URL in array
                        "publish_at": utc_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        "draft": 'false',
                        "snapchat_publish_as": 'STORY'
                    }
                    
                    logger.info(f"Sending payload to Make for file {story['file_name']}: {payload}")
                    response = requests.post(self.MAKE_WEBHOOK, json=payload)
                    
                    if response.status_code != 200:
                        raise Exception(f"Make returned status code {response.status_code}")
                    
                    logger.info(f"✓ Successfully uploaded story {story_index}/{total_stories} ({story['file_name']}) for {client_name}")
                    
                    if story_index < total_stories:
                        logger.info(f"Waiting 10 seconds before next story...")
                        time.sleep(10)
                        
                    break
                except Exception as e:
                    logger.error(f"✗ Error uploading story {story_index}/{total_stories} for {client_name} (attempt {retry_count + 1}/{max_retries}): {str(e)}")
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(5)
                        continue
                    success = False
                    break
        
        return success

    async def process_all_queues(self, target_date: str = None):
        """
        Process all due posts from the aggregated queue.
        
        Args:
            target_date (str, optional): Specific date to process
            
        Notes:
            - Processes all clients
            - Updates queue status
            - Tracks completion
            - Handles errors
        """
        try:
            logger.info("Starting process_all_queues...")
            queue = await self.get_todays_queue(target_date)
            logger.info(f"Queue contents: {queue}")
            
            if not queue:
                logger.warning("No queue found for today")
                return
            
            # Get the correct date for the update
            queue_date = target_date if target_date else datetime.now(pytz.UTC).date().isoformat()
            
            for client, client_queue in queue.items():
                logger.info(f"Processing client {client} with queue: {client_queue}")
                success = await self.send_to_make(client, client_queue)
                logger.info(f"Send to make result for {client}: {success}")
                
                if success:
                    update_result = await self.queues.update_one(
                        {"queue_date": queue_date},  # Use the correct date here
                        {"$set": {f"client_queues.{client}.processed": True}}
                    )
                    logger.info(f"Update result: {update_result.modified_count} documents modified")
            
            logger.info("Queue processing completed")
        except Exception as e:
            logger.error(f"Error processing queues: {str(e)}")
            raise

# API Routes
@router.post("/process-make")
async def trigger_make_processing(target_date: str = None):
    """
    Manual trigger endpoint for Make.com queue processing.
    
    Args:
        target_date (str, optional): Specific date to process
        
    Returns:
        dict: Operation status
        
    Raises:
        HTTPException: For processing errors
        
    Notes:
        - Handles manual triggers
        - Processes specific dates
        - Reports status
        - Tracks errors
    """
    processor = MakeProcessor()
    try:
        await processor.process_all_queues(target_date)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 