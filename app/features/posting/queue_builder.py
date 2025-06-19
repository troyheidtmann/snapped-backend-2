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
from typing import Dict, List, Tuple
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
            
            # Legacy time blocks (for posts without time extensions)
            self.MORNING_POST_TIME = 12    # 12:00 UTC = 7:00 AM ET (base)
            self.AFTERNOON_POST_TIME = 17  # 17:00 UTC = 12:00 PM ET (base)
            self.EVENING_POST_TIME = 21    # 21:00 UTC = 4:00 PM ET (base)
            
            # New time blocks for extensions
            self.MORNING_START = 15        # 15:00 UTC = 10:00 AM ET
            self.AFTERNOON_START = 19      # 19:00 UTC = 2:00 PM ET
            self.EVENING_START = 1         # 01:00 UTC = 8:00 PM ET
            self.ALL_DAY_START = 14        # 14:00 UTC = 9:00 AM ET
            self.ALL_DAY_END = 24          # 24:00 UTC = 7:00 PM ET
            
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
            
    def _get_time_extension(self, file_name: str) -> str:
        """
        Extract time-of-day extension from filename if present
        
        Args:
            file_name (str): Name of the file
            
        Returns:
            str: Time extension ('m', 'a', 'e', 'l') or None
        """
        try:
            # Split filename and check for time extension
            parts = file_name.split('.')
            if len(parts) < 2:
                return None
                
            name_part = parts[0]
            if name_part.endswith('-m'):
                return 'm'
            elif name_part.endswith('-a'):
                return 'a'
            elif name_part.endswith('-e'):
                return 'e'
            elif name_part.endswith('-l'):
                return 'l'
            return None
        except Exception as e:
            logger.error(f"Error parsing time extension from filename {file_name}: {str(e)}")
            return None
            
    def _calculate_schedule_time(self, base_time: int, file_index: int, total_files: int, 
                               time_extension: str, queue_date: datetime, hour_adjustment: int) -> datetime:
        """
        Calculate scheduled time based on time extension and file position
        
        Args:
            base_time (int): Base hour in UTC
            file_index (int): Position of file in sequence
            total_files (int): Total number of files to schedule
            time_extension (str): Time-of-day extension
            queue_date (datetime): Base date for scheduling
            hour_adjustment (int): Timezone adjustment
            
        Returns:
            datetime: Scheduled posting time
        """
        try:
            if time_extension == 'l':  # All day scheduling
                # Calculate time spread between 9 AM and 7 PM
                total_minutes = (self.ALL_DAY_END - self.ALL_DAY_START) * 60
                minutes_per_post = total_minutes / (total_files + 1)  # +1 to avoid ending exactly at 7 PM
                minutes_offset = minutes_per_post * file_index
                
                base_datetime = queue_date.replace(
                    hour=(self.ALL_DAY_START - hour_adjustment) % 24,
                    minute=0,
                    second=0,
                    microsecond=0
                )
                return base_datetime + timedelta(minutes=minutes_offset)
            else:
                # For other extensions, use 2-minute spacing
                if time_extension == 'm':
                    base_time = self.MORNING_START
                elif time_extension == 'a':
                    base_time = self.AFTERNOON_START
                elif time_extension == 'e':
                    base_time = self.EVENING_START
                    
                base_datetime = queue_date.replace(
                    hour=(base_time - hour_adjustment) % 24,
                    minute=0,
                    second=0,
                    microsecond=0
                )
                return base_datetime + timedelta(minutes=file_index * 2)
                
        except Exception as e:
            logger.error(f"Error calculating schedule time: {str(e)}")
            return None

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
            
            # Debug: Print all documents for this client
            logger.info(f"\n=== CHECKING UPLOADS FOR CLIENT ===")
            async for doc in self.uploads.find(query):
                logger.info(f"Found document:")
                logger.info(f"  Client ID: {doc.get('client_ID')}")
                logger.info(f"  Sessions: {[s.get('session_id') for s in doc.get('sessions', [])]}")
                logger.info(f"  Session dates: {[s.get('scan_date') for s in doc.get('sessions', [])]}")
                logger.info(f"  Already queued?: {[s.get('queued', False) for s in doc.get('sessions', [])]}")
                logger.info(f"  Files count: {sum(len(s.get('files', [])) for s in doc.get('sessions', []))}")
            
            daily_queue = {
                "queue_date": queue_date.date().isoformat(),
                "created_at": datetime.now(),
                "status": "pending",
                "client_queues": {},
                "total_posts": 0
            }
            
            # Find eligible content
            async for doc in self.uploads.find(query):
                client_id = doc.get('client_ID')
                logger.info(f"\nProcessing document for client: {client_id}")
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
            
            # Debug timezone adjustment
            hour_adjustment = self.timezone_adjustments.get(client_timezone, 0)
            logger.info(f"\n=== TIMEZONE DEBUG for {client_id} ===")
            logger.info(f"Client timezone: {client_timezone}")
            logger.info(f"Hour adjustment from ET: {hour_adjustment}")
            
            # Store original date for session matching
            target_date = queue_date.date()
            logger.info(f"Target date for matching: {target_date}")

            # Create time blocks for legacy scheduling (no time extension)
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
                logger.info(f"\nChecking session: {session_id}")
                
                if not session_id:
                    continue

                # Fix date parsing to handle both formats
                try:
                    # Extract date from session_id: F(06-21-2025)_th10021994
                    date_str = session_id[2:].split(')')[0].strip('(')  # Get just "06-21-2025"
                    logger.info(f"Processing date string: {date_str} for target date: {target_date}")
                    
                    # Try mm-dd-yyyy format first
                    try:
                        session_date = datetime.strptime(date_str, '%m-%d-%Y')
                    except ValueError:
                        # If that fails, try Month d, yyyy format
                        session_date = datetime.strptime(date_str, '%b %d, %Y')
                    
                    # Convert both dates to string format for comparison
                    session_date_str = session_date.strftime('%Y-%m-%d')
                    target_date_str = target_date.strftime('%Y-%m-%d')
                    
                    logger.info(f"Comparing dates - Session: {session_date_str}, Target: {target_date_str}")
                    
                    if session_date_str == target_date_str:
                        logger.info("Date matches!")
                        if not session.get('queued'):
                            logger.info("Session not queued - adding to eligible sessions")
                            eligible_sessions.append(session)
                        else:
                            logger.info("Session already queued - skipping")
                    else:
                        logger.info(f"Date mismatch - session date {session_date_str} != target date {target_date_str}")
                        
                except Exception as e:
                    logger.error(f"Error processing session {session_id}: {str(e)}")
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
            
            # Group files by time extension
            for session in eligible_sessions:
                sorted_files = sorted(session['files'], key=lambda x: x['file_name'])
                
                # Group files by time extension
                extension_groups = {}
                for f in sorted_files:
                    if f.get('is_thumbnail', False):
                        continue  # Handle thumbnails separately
                        
                    time_ext = self._get_time_extension(f['file_name'])
                    if time_ext not in extension_groups:
                        extension_groups[time_ext] = []
                    extension_groups[time_ext].append(f)
                
                # Schedule files based on their time extension
                for ext, files in extension_groups.items():
                    if ext is None:
                        # Handle legacy scheduling (no time extension)
                        total_files = len(files)
                        files_per_block = total_files // 3
                        extra_files = total_files % 3
                        
                        for i, f in enumerate(files):
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
                                "session_id": session['session_id']
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
                    else:
                        # Handle new time extension scheduling
                        for i, f in enumerate(files):
                            file_data = {
                                "file_name": f['file_name'],
                                "cdn_url": f['CDN_link'],
                                "file_type": f['file_type'],
                                "snap_id": snap_id,
                                "timezone": client.get('timezone', 'America/New_York'),
                                "snapchat_publish_as": session.get('content_type', 'STORY'),
                                "session_id": session['session_id']
                            }
                            
                            scheduled_time = self._calculate_schedule_time(
                                base_time=0,  # Not used for new scheduling
                                file_index=i,
                                total_files=len(files),
                                time_extension=ext,
                                queue_date=queue_date,
                                hour_adjustment=hour_adjustment
                            )
                            
                            if scheduled_time:
                                scheduled_stories.append({
                                    **file_data,
                                    "scheduled_time": scheduled_time.isoformat()
                                })
                
                # Handle thumbnails
                thumbnails = [f for f in sorted_files if f.get('is_thumbnail', False)]
                for thumbnail in thumbnails:
                    # Find the original file this thumbnail belongs to
                    original_name = thumbnail['file_name'].replace('-t', '')
                    original_stories = [s for s in scheduled_stories if s['file_name'] == original_name]
                    
                    if original_stories:
                        original_time = datetime.fromisoformat(original_stories[0]['scheduled_time'])
                        thumbnail_time = original_time + timedelta(minutes=2)
                        
                        scheduled_stories.append({
                            "file_name": thumbnail['file_name'],
                            "cdn_url": thumbnail['CDN_link'],
                            "file_type": thumbnail['file_type'],
                            "snap_id": snap_id,
                            "timezone": client.get('timezone', 'America/New_York'),
                            "snapchat_publish_as": session.get('content_type', 'STORY'),
                            "session_id": session['session_id'],
                            "is_thumbnail": True,
                            "scheduled_time": thumbnail_time.isoformat()
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

@router.post("/build_test_queues")
async def build_test_queues_endpoint(test_client_id: str, days: int = 5):
    """
    Build queues for multiple days for a test account
    
    Args:
        test_client_id (str): Test account client ID
        days (int): Number of days to build queues for (default 5)
        
    Returns:
        Dict: Results for each day's queue build
    """
    try:
        builder = QueueBuilder()
        results = []
        
        # Build queues for specified number of days
        for day_offset in range(days):
            queue_date = datetime.now(timezone.utc) + timedelta(days=day_offset)
            logger.info(f"Building queue for date: {queue_date.date()} for test client: {test_client_id}")
            
            result = await builder.build_daily_queue(
                queue_date=queue_date,
                client_filter={"client_ID": test_client_id}
            )
            
            if result:
                results.append({
                    "date": queue_date.date().isoformat(),
                    "total_posts": result['total_posts'],
                    "status": "success"
                })
            else:
                results.append({
                    "date": queue_date.date().isoformat(),
                    "total_posts": 0,
                    "status": "no_posts"
                })
        
        return {
            "status": "success",
            "test_client_id": test_client_id,
            "days_processed": days,
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error building test queues: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import asyncio
    asyncio.run(build_queue()) 