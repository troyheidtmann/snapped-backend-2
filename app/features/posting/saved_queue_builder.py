"""
Saved Posts Queue Builder Module

This module handles the construction and management of saved post queues for
Snapchat story publishing. It manages the scheduling, organization, and
validation of saved posts for multiple clients.

Features:
--------
1. Queue Management:
   - Saved post organization
   - Client grouping
   - Time block scheduling
   - Status tracking

2. Data Processing:
   - Post validation
   - Media handling
   - Caption processing
   - Schedule calculation

3. Time Management:
   - Timezone handling
   - Block distribution
   - Interval management
   - Regional adjustments

4. Status Tracking:
   - Queue status
   - Processing flags
   - Error handling
   - Validation results

Data Model:
----------
Queue Structure:
- Client queues
- Saved posts
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

from fastapi import APIRouter
from app.shared.database import async_client
from datetime import datetime, timezone, timedelta
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/saved-queue")

class SavedQueueBuilder:
    """
    Saved Posts Queue Builder for Snapchat Stories.
    
    Handles the construction and management of saved post queues for
    scheduled Snapchat story publishing.
    
    Attributes:
        saved: MongoDB saved posts collection
        saved_queue: MongoDB saved queue collection
        MORNING_POST_TIME: Base time for morning posts (UTC)
        AFTERNOON_POST_TIME: Base time for afternoon posts (UTC)
        EVENING_POST_TIME: Base time for evening posts (UTC)
        timezone_adjustments: Regional timezone offsets
        
    Notes:
        - Manages saved posts
        - Handles time blocks
        - Tracks processing status
        - Implements timezone logic
    """
    
    def __init__(self):
        try:
            self.saved = async_client['UploadDB']['Saved']
            self.saved_queue = async_client['QueueDB']['SavedQueue']
            
            # Base times in UTC that result in these ET display times
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
            logger.error(f"Error initializing SavedQueueBuilder: {str(e)}")
            raise

    async def build_daily_queue(self, queue_date: datetime = None) -> dict:
        """
        Build saved content queue for specified date.
        
        Args:
            queue_date (datetime): Date for which to build the queue
            
        Returns:
            dict: Queue data containing client queues and status
            
        Notes:
            - Retrieves saved content
            - Organizes into queues
            - Saves queue data
            - Handles errors
        """
        try:
            if not queue_date:
                queue_date = datetime.now(timezone.utc)
            
            logger.info(f"Building saved queue for date: {queue_date.date()}")

            daily_queue = {
                "queue_date": queue_date.date().isoformat(),
                "created_at": datetime.now(),
                "status": "pending",
                "client_queues": {},
                "total_posts": 0
            }

            # Find all saved documents
            async for doc in self.saved.find({}):
                client_id = doc.get('client_ID')
                if not client_id:
                    continue
                    
                logger.info(f"Processing saved content for client: {client_id}")
                
                client_queue = await self._prepare_client_queue(doc, queue_date)
                if client_queue and client_queue.get('posts'):
                    daily_queue['client_queues'][client_id] = client_queue
                    daily_queue['total_posts'] += len(client_queue['posts'])

            await self.saved_queue.insert_one(daily_queue)
            logger.info(f"Saved queue with {daily_queue['total_posts']} posts")

            return daily_queue

        except Exception as e:
            logger.error(f"Error in build_daily_queue: {str(e)}")
            raise

    async def _prepare_client_queue(self, client: dict, queue_date: datetime) -> dict:
        """
        Prepare queue data for a single client.
        
        Args:
            client (dict): Client data
            queue_date (datetime): Date for which to build the queue
            
        Returns:
            dict: Queue data for the client
            
        Notes:
            - Retrieves eligible sessions
            - Collects eligible files
            - Distributes across time blocks
            - Handles errors
        """
        try:
            client_id = client.get('client_ID')
            client_timezone = client.get('timezone', 'America/New_York')
            
            # Debug timezone adjustment
            hour_adjustment = self.timezone_adjustments.get(client_timezone, 0)
            logger.info(f"Client timezone: {client_timezone}")
            logger.info(f"Hour adjustment from ET: {hour_adjustment}")
            
            # Create time blocks
            time_blocks = []
            for base_time in [self.MORNING_POST_TIME, self.AFTERNOON_POST_TIME, self.EVENING_POST_TIME]:
                next_time = self._get_next_occurrence(base_time, hour_adjustment, queue_date)
                time_blocks.append(next_time)

            target_date = queue_date.date()
            eligible_sessions = []
            
            # Find eligible sessions for the target date
            for session in client.get('sessions', []):
                session_id = session.get('session_id', '')
                if not session_id:
                    continue
                    
                try:
                    date_str = session_id[3:].split(')')[0]  # Skip "F(" to get just "01-28-2025"
                    try:
                        session_date = datetime.strptime(date_str, '%m-%d-%Y')
                    except ValueError:
                        session_date = datetime.strptime(date_str, '%b %d, %Y')
                    
                    if session_date.date() == target_date and not any(f.get('queued', False) for f in session.get('files', [])):
                        eligible_sessions.append(session)
                        
                except ValueError as e:
                    logger.error(f"Error parsing date from session_id {session_id}: {str(e)}")
                    continue

            if not eligible_sessions:
                return None

            scheduled_posts = []
            
            # Process all files from eligible sessions
            for session in eligible_sessions:
                sorted_files = sorted(session.get('files', []), key=lambda x: x.get('file_name', ''))
                total_files = len(sorted_files)
                files_per_block = total_files // 3
                extra_files = total_files % 3
                
                for i, file in enumerate(sorted_files):
                    if file.get('queued'):
                        continue
                        
                    # Determine which block this file belongs to
                    if i < files_per_block + (1 if extra_files > 0 else 0):
                        block = 0
                    elif i < (files_per_block * 2) + (2 if extra_files > 1 else 1 if extra_files > 0 else 0):
                        block = 1
                    else:
                        block = 2
                        
                    base_time = time_blocks[block]
                    if block == 0:
                        position = i
                    elif block == 1:
                        position = i - (files_per_block + (1 if extra_files > 0 else 0))
                    else:
                        position = i - (files_per_block * 2 + (2 if extra_files > 1 else 1 if extra_files > 0 else 0))
                        
                    scheduled_time = base_time + timedelta(minutes=position*2)
                    
                    # Create post data
                    post_data = {
                        "file_name": file.get('file_name'),
                        "cdn_url": file.get('CDN_link'),
                        "file_type": "video/mp4",
                        "snap_id": client.get('snap_ID', ''),
                        "scheduled_time": scheduled_time.isoformat(),
                        "session_id": session['session_id'],
                        "content_type": "saved",
                        "caption": file.get('caption', ''),
                        "queued": True,
                        "queue_time": datetime.now().isoformat()
                    }
                    
                    scheduled_posts.append(post_data)
                    
                    # Mark file as queued in database
                    await self.saved.update_one(
                        {
                            "client_ID": client_id,
                            "sessions.session_id": session['session_id'],
                            "sessions.files.file_name": file.get('file_name')
                        },
                        {
                            "$set": {
                                "sessions.$[session].files.$[file].queued": True,
                                "sessions.$[session].files.$[file].queue_time": datetime.now().isoformat()
                            }
                        },
                        array_filters=[
                            {"session.session_id": session['session_id']},
                            {"file.file_name": file.get('file_name')}
                        ]
                    )

            if not scheduled_posts:
                return None

            return {
                "posts": scheduled_posts,
                "processed": False
            }

        except Exception as e:
            logger.error(f"Error preparing queue for client {client.get('client_ID')}: {str(e)}")
            return None

    def _get_next_occurrence(self, base_hour: int, hour_adjustment: int, queue_date: datetime) -> datetime:
        """
        Calculate the next occurrence of a time considering timezone adjustments.
        
        Args:
            base_hour (int): Base hour for scheduling
            hour_adjustment (int): Timezone adjustment
            queue_date (datetime): Date for scheduling
            
        Returns:
            datetime: Adjusted scheduled time
            
        Notes:
            - Handles timezone adjustments
            - Validates time ranges
            - Maintains consistency
        """
        adjusted_hour = (base_hour + hour_adjustment) % 24
        return queue_date.replace(hour=adjusted_hour, minute=0, second=0, microsecond=0)

@router.post("/build")
async def build_queue(queue_date: str = None):
    """
    Build saved queue for specified date.
    
    Args:
        queue_date (str): Optional date string (YYYY-MM-DD)
        
    Returns:
        dict: Operation status and queue data
        
    Notes:
        - Handles date parsing
        - Builds queue
        - Reports status
        - Tracks errors
    """
    try:
        builder = SavedQueueBuilder()
        if queue_date:
            date = datetime.strptime(queue_date, '%Y-%m-%d')
        else:
            date = None
            
        queue = await builder.build_daily_queue(date)
        
        if '_id' in queue:
            queue['_id'] = str(queue['_id'])
            
        return {"status": "success", "queue": queue}
    except Exception as e:
        logger.error(f"Error building saved queue: {str(e)}")
        return {"status": "error", "message": str(e)} 