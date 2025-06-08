"""
Post Queue Builder Module

This module handles the construction and management of post queues for
Snapchat story publishing. It manages the scheduling, organization, and
validation of posts for multiple clients.

Features:
--------
1. Queue Management:
   - Post organization
   - Client grouping
   - Schedule management
   - Status tracking

2. Data Processing:
   - Post validation
   - Media handling
   - Caption processing
   - Schedule calculation

3. Time Management:
   - Schedule validation
   - Time zone handling
   - Interval management
   - Batch timing

4. Status Tracking:
   - Queue status
   - Processing flags
   - Error handling
   - Validation results

Data Model:
----------
Queue Structure:
- Client queues
- Post metadata
- Schedule timing
- Processing status

Security:
--------
- Data validation
- Error handling
- Status tracking
- Access control

Dependencies:
-----------
- FastAPI: API framework
- MongoDB: Data storage
- pytz: Timezone handling
- logging: Debug tracking

Author: Snapped Development Team
"""

from datetime import datetime, timedelta, timezone
import logging
from app.shared.database import async_client
from typing import Dict, List
from fastapi import APIRouter, HTTPException
import pytz
from app.shared.models import QueueStatus

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class QueueBuilder:
    """
    Post Queue Builder for Snapchat Stories.
    
    Handles the construction and management of post queues for
    scheduled Snapchat story publishing.
    
    Attributes:
        queue: MongoDB queue collection
        
    Notes:
        - Manages post queues
        - Handles scheduling
        - Tracks processing status
        - Implements validation
    """
    
    def __init__(self):
        try:
            # Log the database names we're connecting to
            logger.info("Initializing database connections...")
            logger.info(f"Available databases: {async_client.list_database_names()}")
            
            self.client_db = async_client['ClientDB']['ClientInfo']  # Note: Changed from ClientDb to ClientDB
            logger.info(f"Connected to ClientDB. Collection names: {async_client['ClientDB'].list_collection_names()}")
            
            self.queues = async_client['QueueDB']['Queue']
            self.uploads = async_client['UploadDB']['Uploads']
            
            logger.info("QueueBuilder initialized with all collections")
            
            # Base times in UTC that result in these ET display times:
            self.MORNING_POST_TIME = 12    # 12:00 UTC = 7:00 AM ET (base)
            self.AFTERNOON_POST_TIME = 17  # 17:00 UTC = 12:00 PM ET (base)
            self.EVENING_POST_TIME = 21    # 21:00 UTC = 4:00 PM ET (base)
            
            # Define timezone adjustments relative to ET
            self.timezone_adjustments = {
                'America/New_York': 0,      # ET
                'America/Chicago': -1,      # CT
                'America/Denver': -2,       # MT
                'America/Phoenix': -2,      # MT (no DST)
                'America/Los_Angeles': -3,  # PT
                'America/Anchorage': -4,    # AT
                'Pacific/Honolulu': -5,     # HT
            }
            
        except Exception as e:
            logger.error(f"Error initializing QueueBuilder: {str(e)}")
            raise

    async def build_daily_queue(self, queue_date: datetime = None, client_filter: dict = None) -> Dict:
        """
        Build content queue for specified date
        
        Args:
            queue_date (datetime): Date for queue
            client_filter (dict): Filter for client selection
            
        Returns:
            Dict: Queue data containing client queues and status
            
        Notes:
            - Retrieves queue data
            - Validates queue data
            - Logs queue details
            - Reports queue status
        """
        try:
            if not queue_date:
                queue_date = datetime.now(timezone.utc)
            
            logger.info(f"=== STARTING QUEUE BUILD ===")
            logger.info(f"Queue date: {queue_date.date()}")
            logger.info(f"Current UTC: {datetime.now(timezone.utc)}")
            
            # Debug the query
            query = client_filter if client_filter else {}
            logger.info(f"Using MongoDB query: {query}")
            
            # Check what's in uploads collection
            count = await self.uploads.count_documents({})
            logger.info(f"Total documents in uploads collection: {count}")
            
            async for doc in self.uploads.find({}):
                logger.info(f"Found upload doc:")
                logger.info(f"  Client ID: {doc.get('client_ID')}")
                logger.info(f"  Sessions: {[s.get('session_id') for s in doc.get('sessions', [])]}")
                logger.info(f"  Already queued?: {[s.get('queued', False) for s in doc.get('sessions', [])]}")
            
            daily_queue = {
                "queue_date": queue_date.date().isoformat(),
                "created_at": datetime.now(),
                "status": "pending",
                "client_queues": {},
                "total_posts": 0
            }
            
            # Use client filter if provided
            query = client_filter if client_filter else {}
            logger.info(f"Using query filter: {query}")
            
            # Find eligible content
            async for doc in self.uploads.find(query):
                client_id = doc.get('client_ID')
                logger.info(f"Found document for client: {client_id}")
                logger.info(f"Number of sessions: {len(doc.get('sessions', []))}")
                logger.info(f"Sessions: {[s.get('session_id') for s in doc.get('sessions', [])]}")
                
                client_queue = await self._prepare_client_queue(doc, queue_date)
                if client_queue and client_queue.get('stories'):
                    logger.info(f"Adding {len(client_queue['stories'])} stories to queue for {client_id}")
                    daily_queue['client_queues'][client_id] = client_queue
                    daily_queue['total_posts'] += len(client_queue['stories'])
                    await self._mark_sessions_as_queued(client_id, client_queue)
                else:
                    logger.info(f"No stories found for client {client_id}")

            # Update or create queue document
            result = await self.queues.update_one(
                {"queue_date": daily_queue["queue_date"]},
                {"$set": daily_queue},
                upsert=True
            )
            logger.info(f"Queue {'updated' if result.modified_count else 'created'} with {daily_queue['total_posts']} posts")

            return daily_queue

        except Exception as e:
            logger.error(f"Error in build_daily_queue: {str(e)}")
            raise

    async def _prepare_client_queue(self, client: Dict, queue_date: datetime) -> Dict:
        """
        Prepare queue data for a single client
        
        Args:
            client (Dict): Client data
            queue_date (datetime): Date for queue
            
        Returns:
            Dict: Queue data for the client
            
        Notes:
            - Retrieves client data
            - Validates client data
            - Logs client details
            - Handles timezone adjustments
        """
        try:
            client_id = client['client_ID']
            client_timezone = client.get('timezone')
            
            # Check document-level approval
            doc_approval = client.get('approved')
            if doc_approval is True:
                logger.info(f"Client {client_id} is approved at document level - will check session approvals")
            else:
                logger.info(f"Client {client_id} has no document level approval or is not approved - processing all sessions")
            
            # Debug timezone adjustment
            hour_adjustment = self.timezone_adjustments.get(client_timezone, 0)
            logger.info(f"\n=== TIMEZONE DEBUG for {client_id} ===")
            logger.info(f"Client timezone: {client_timezone}")
            logger.info(f"Hour adjustment from ET: {hour_adjustment}")
            
            # Store original date for session matching
            target_date = queue_date.date()

            # Create time blocks with detailed logging
            time_blocks = []
            for base_time in [self.MORNING_POST_TIME, self.AFTERNOON_POST_TIME, self.EVENING_POST_TIME]:
                logger.info(f"\nProcessing base time: {base_time}:00 UTC")
                logger.info(f"Before adjustment: {base_time}:00 UTC")
                
                next_time = self._get_next_occurrence(base_time, hour_adjustment, queue_date)
                
                logger.info(f"After adjustment: {next_time.hour}:00 UTC")
                logger.info(f"Expected local time: {(next_time.hour - hour_adjustment) % 24}:00")
                
                time_blocks.append(next_time)
                logger.info(f"Added time block: {next_time} for {client_id}")

            logger.info(f"Created {len(time_blocks)} time blocks for {client_id}: {time_blocks}")

            # Initialize eligible_sessions list
            eligible_sessions = []
            
            # Log the sessions we're checking
            for session in client['sessions']:
                session_id = session.get('session_id', '')
                logger.info(f"Checking session: {session_id}")
                
                if not session_id:
                    continue

                # If document is approved, sessions must be explicitly approved
                if doc_approval is True:
                    session_approval = session.get('approved')
                    if not session_approval:  # This will catch both None and False
                        logger.info(f"Session {session_id} is not explicitly approved - skipping")
                        continue
                    logger.info(f"Session {session_id} is approved - will process")
                else:
                    logger.info(f"No document level approval required - processing session {session_id}")
                    
                # Fix date parsing to handle both formats
                try:
                    date_str = session_id[3:].split(')')[0]  # Skip "F(" to get just "01-28-2025"
                    try:
                        # Try mm-dd-yyyy format first
                        session_date = datetime.strptime(date_str, '%m-%d-%Y')
                    except ValueError:
                        # If that fails, try Month d, yyyy format
                        session_date = datetime.strptime(date_str, '%b %d, %Y')
                    
                    logger.info(f"Session {session_id} date: {session_date.date()}")
                    logger.info(f"Is queued?: {session.get('queued', False)}")
                    
                    if session_date.date() == target_date:  # Compare against original date
                        logger.info("Date matches!")
                        if not session.get('queued'):
                            logger.info("Session not queued - adding to eligible sessions")
                            eligible_sessions.append(session)
                        else:
                            logger.info("Session already queued - skipping")
                except ValueError as e:
                    logger.error(f"Error parsing date from session_id {session_id}: {str(e)}")
                    continue

            if not eligible_sessions:
                logger.info(f"No eligible sessions found for client {client_id}")
                return None

            snap_id = client.get('snap_ID')  # Get snap_ID directly from the upload document
            if not snap_id:
                logger.error(f"No snap_ID found for client {client_id}")
                return None

            logger.info(f"Using snap_ID: {snap_id}")

            scheduled_stories = []
            current_files = []
            current_block = 0

            # Process all files from eligible sessions
            for session in eligible_sessions:
                sorted_files = sorted(session['files'], key=lambda x: x['file_name'])
                
                # Check if there are any thumbnails
                has_thumbnails = any(f.get('is_thumbnail') for f in sorted_files)
                
                if has_thumbnails:
                    # Process files with thumbnail breaks
                    for f in sorted_files:
                        file_data = {
                            "file_name": f['file_name'],
                            "cdn_url": f['CDN_link'],
                            "file_type": f['file_type'],
                            "snap_id": snap_id,
                            "timezone": client.get('timezone', 'America/New_York'),
                            "snapchat_publish_as": session.get('content_type', 'STORY'),
                            "session_id": session['session_id'],
                            "is_thumbnail": f.get('is_thumbnail', False)
                        }
                        
                        if file_data['is_thumbnail']:
                            # Schedule current files
                            if current_files:
                                base_time = time_blocks[current_block]
                                for i, cf in enumerate(current_files):
                                    scheduled_time = base_time + timedelta(minutes=i*2)
                                    scheduled_stories.append({
                                        **cf,
                                        "scheduled_time": scheduled_time.isoformat()
                                    })
                                # Add thumbnail 2 minutes after last file
                                thumbnail_time = base_time + timedelta(minutes=len(current_files)*2)
                                scheduled_stories.append({
                                    **file_data,
                                    "scheduled_time": thumbnail_time.isoformat()
                                })
                                current_files = []
                                current_block = min(current_block + 1, 2)
                        else:
                            current_files.append(file_data)
                else:
                    # Split files equally between time blocks
                    total_files = len(sorted_files)
                    files_per_block = total_files // 3
                    extra_files = total_files % 3
                    
                    for i, f in enumerate(sorted_files):
                        # Determine which block this file belongs to
                        if i < files_per_block + (1 if extra_files > 0 else 0):
                            block = 0
                        elif i < (files_per_block * 2) + (2 if extra_files > 1 else 1 if extra_files > 0 else 0):
                            block = 1
                        else:
                            block = 2
                            
                        file_data = {
                            "file_name": f['file_name'],
                            "cdn_url": f['CDN_link'],
                            "file_type": f['file_type'],
                            "snap_id": snap_id,
                            "timezone": client.get('timezone', 'America/New_York'),
                            "snapchat_publish_as": session.get('content_type', 'STORY'),
                            "session_id": session['session_id'],
                            "is_thumbnail": f.get('is_thumbnail', False)
                        }
                        
                        base_time = time_blocks[block]
                        if block == 0:
                            position = i
                        elif block == 1:
                            position = i - (files_per_block + (1 if extra_files > 0 else 0))
                        else:
                            position = i - (files_per_block * 2 + (2 if extra_files > 1 else 1 if extra_files > 0 else 0))
                            
                        scheduled_time = base_time + timedelta(minutes=position*2)
                        scheduled_stories.append({
                            **file_data,
                            "scheduled_time": scheduled_time.isoformat()
                        })

            # Handle any remaining files from thumbnail processing
            if current_files:
                base_time = time_blocks[current_block]
                for i, f in enumerate(current_files):
                    scheduled_time = base_time + timedelta(minutes=i*2)
                    scheduled_stories.append({
                        **f,
                        "scheduled_time": scheduled_time.isoformat()
                    })

            if not scheduled_stories:
                logger.info(f"No stories scheduled for client {client_id}")
                return None

            logger.info(f"Scheduled {len(scheduled_stories)} stories for client {client_id}")
            return {
                "stories": scheduled_stories,
                "processed": False
            }

        except Exception as e:
            logger.error(f"Error preparing queue for client {client.get('client_ID')}: {str(e)}")
            return None

    async def _mark_sessions_as_queued(self, client_id: str, client_queue: Dict):
        """
        Mark the queued sessions and files in ClientDb
        
        Args:
            client_id (str): Client identifier
            client_queue (Dict): Queue data for the client
            
        Notes:
            - Updates session status
            - Handles errors
        """
        try:
            session_ids = {story['session_id'] for story in client_queue['stories']}
            
            for session_id in session_ids:
                await self.uploads.update_one(
                    {
                        "client_ID": client_id,
                        "sessions.session_id": session_id
                    },
                    {
                        "$set": {
                            "sessions.$.queued": True,
                            "sessions.$.queue_date": datetime.now()
                        }
                    }
                )
        except Exception as e:
            logger.error(f"Error marking sessions as queued for {client_id}: {str(e)}")

    def _get_next_occurrence(self, base_hour: int, hour_adjustment: int, queue_date: datetime) -> datetime:
        """
        Calculate the next occurrence of a time considering timezone adjustments
        
        Args:
            base_hour (int): Base hour for scheduling
            hour_adjustment (int): Timezone adjustment
            queue_date (datetime): Date for scheduling
            
        Returns:
            datetime: Scheduled time
            
        Notes:
            - Handles timezone adjustments
            - Validates time ranges
        """
        adjusted_hour = (base_hour + hour_adjustment) % 24
        return queue_date.replace(hour=adjusted_hour, minute=0, second=0, microsecond=0)

async def build_queue(queue_date: datetime = None):
    """Convenience function to build queue"""
    builder = QueueBuilder()
    return await builder.build_daily_queue(queue_date)

router = APIRouter(prefix="/queue", tags=["queue"])



@router.post("/build")
async def build_queue_endpoint(client_id: str = None):
    try:
        builder = QueueBuilder()
        query = {"client_ID": client_id} if client_id else {}
        logger.info(f"Building queue with filter: {query}")
        result = await builder.build_daily_queue(client_filter=query)
        return {
            "status": "success",
            "total_posts": result['total_posts'],
            "queue_date": result['queue_date'],
            "client_queues": result['client_queues']
        }
    except Exception as e:
        logger.error(f"Error building queue: {str(e)}")
        raise
    

if __name__ == "__main__":
    import asyncio
    asyncio.run(build_queue()) 