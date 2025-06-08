"""
Post Queue Builder Module (Copy)

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
            self.client_db = async_client['ClientDb']['ClientInfo']
            self.queues = async_client['QueueDB']['Queue']
            self.uploads = async_client['UploadDB']['Uploads']

            logger.info("QueueBuilder initialized with all collections")
            
            # Time blocks for post scheduling
            self.MORNING_POST_TIME = 9    # 9 AM
            self.AFTERNOON_POST_TIME = 14 # 2 PM
            self.EVENING_POST_TIME = 18   # 6 PM
        except Exception as e:
            logger.error(f"Error initializing QueueBuilder: {str(e)}")
            raise

    async def build_daily_queue(self, queue_date: datetime = None) -> Dict:
        """
        Build content queue for specified date
        
        Args:
            queue_date (datetime): Date for which to build the queue
            
        Returns:
            Dict: Queue data containing client queues and status
            
        Notes:
            - Retrieves eligible content
            - Organizes content into queues
            - Saves queue to database
            - Handles errors
        """
        try:
            queue_date = queue_date or datetime.now() + timedelta(days=1)
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=48)
            
            logger.info(f"Building queue for date: {queue_date.date()}")
            logger.info(f"Using cutoff time: {cutoff_time}")

            daily_queue = {
                "queue_date": queue_date.date().isoformat(),
                "created_at": datetime.now(),
                "status": "pending",
                "client_queues": {},
                "total_posts": 0
            }

            # Find eligible content
            async for doc in self.uploads.find({}):
                client_id = doc.get('client_ID')
                logger.info(f"Processing client: {client_id}")
                
                for session in doc.get('sessions', []):
                    try:
                        upload_date = session.get('upload_date')
                        logger.info(f"Raw upload_date: {upload_date}, type: {type(upload_date)}")
                        
                        # Handle different date types and make timezone-aware
                        if isinstance(upload_date, datetime):
                            session_date = upload_date
                        elif isinstance(upload_date, str):
                            session_date = datetime.fromisoformat(upload_date)
                        else:
                            logger.error(f"Invalid upload_date format: {upload_date}")
                            continue
                            
                        # Make timezone-aware if it isn't already
                        if session_date.tzinfo is None:
                            session_date = session_date.replace(tzinfo=timezone.utc)
                            
                        session_queued = session.get('queued', False)
                        
                        logger.info(f"Processed session date: {session_date}")
                        logger.info(f"Session date tzinfo: {session_date.tzinfo}")
                        logger.info(f"Cutoff time: {cutoff_time}")
                        logger.info(f"Cutoff tzinfo: {cutoff_time.tzinfo}")
                        logger.info(f"Is session before cutoff? {session_date < cutoff_time}")
                        logger.info(f"Is session queued? {session_queued}")
                        
                        if session_date < cutoff_time and not session_queued:
                            logger.info(f"Found eligible session for {client_id}")
                            client_queue = await self._prepare_client_queue(doc, queue_date)
                            if client_queue and client_queue.get('stories'):
                                logger.info(f"Adding {len(client_queue['stories'])} stories to queue")
                                daily_queue['client_queues'][client_id] = client_queue
                                daily_queue['total_posts'] += len(client_queue['stories'])
                                await self._mark_sessions_as_queued(client_id, client_queue)
                            else:
                                logger.info("No stories found in client queue")
                    except Exception as e:
                        logger.error(f"Error processing session date: {str(e)}")
                        continue

            # Save the queue to database before returning
            await self.queues.insert_one(daily_queue)
            logger.info(f"Saved queue to database with {daily_queue['total_posts']} posts")

            return daily_queue

        except Exception as e:
            logger.error(f"Error in build_daily_queue: {str(e)}")
            raise

    async def _prepare_client_queue(self, client: Dict, queue_date: datetime) -> Dict:
        """
        Prepare queue data for a single client
        
        Args:
            client (Dict): Client data
            queue_date (datetime): Date for which to build the queue
            
        Returns:
            Dict: Queue data for the client
            
        Notes:
            - Retrieves eligible sessions
            - Collects eligible files
            - Distributes files across time blocks
            - Handles errors
        """
        try:
            client_id = client['client_ID']
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=48)
            
            # Convert dates and make timezone-aware for comparison
            eligible_sessions = []
            for session in client['sessions']:
                try:
                    upload_date = session.get('upload_date')
                    if isinstance(upload_date, datetime):
                        session_date = upload_date
                    elif isinstance(upload_date, str):
                        session_date = datetime.fromisoformat(upload_date)
                    else:
                        continue
                        
                    # Make timezone-aware if needed
                    if session_date.tzinfo is None:
                        session_date = session_date.replace(tzinfo=timezone.utc)
                        
                    if session_date < cutoff_time and not session.get('queued'):
                        eligible_sessions.append(session)
                except Exception as e:
                    logger.error(f"Error processing session date in _prepare_client_queue: {str(e)}")
                    continue

            if not eligible_sessions:
                return None

            # Collect all eligible files
            all_files = []
            for session in eligible_sessions:
                files = [
                    {
                        "file_name": f['file_name'],
                        "cdn_url": f['CDN_link'],
                        "file_type": f['file_type'],
                        "snap_id": client.get('snap_ID', ''),
                        "timezone": client.get('timezone', 'America/New_York'),
                        "snapchat_publish_as": session.get('content_type', 'STORY'),
                        "session_id": session['session_id'],
                        "seq_number": f.get('seq_number', 0)
                    }
                    for f in session['files']
                    if not f.get('queued') and not f.get('is_thumbnail', False)
                ]
                all_files.extend(files)

            # Sort files by sequence number before distribution
            all_files.sort(key=lambda x: x['seq_number'])

            # Remove seq_number before creating scheduled stories
            for file in all_files:
                file.pop('seq_number', None)

            if not all_files:
                return None

            # Distribute files across time blocks
            time_blocks = [
                queue_date.replace(hour=self.MORNING_POST_TIME, minute=0, second=0),
                queue_date.replace(hour=self.AFTERNOON_POST_TIME, minute=0, second=0),
                queue_date.replace(hour=self.EVENING_POST_TIME, minute=0, second=0)
            ]

            posts_per_block = len(all_files) // 3
            extra_posts = len(all_files) % 3

            scheduled_stories = []
            file_index = 0

            for block_index, base_time in enumerate(time_blocks):
                block_size = posts_per_block + (1 if extra_posts > block_index else 0)
                
                for i in range(block_size):
                    if file_index < len(all_files):
                        file = all_files[file_index]
                        scheduled_time = base_time + timedelta(minutes=i*2)
                        
                        scheduled_stories.append({
                            **file,
                            "scheduled_time": scheduled_time.isoformat()
                        })
                        
                        file_index += 1

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

async def build_queue(queue_date: datetime = None):
    """Convenience function to build queue"""
    builder = QueueBuilder()
    return await builder.build_daily_queue(queue_date)

router = APIRouter(prefix="/api/queue", tags=["posting"])

@router.post("/test")
async def test_queue():
    try:
        test_data = {
            "queue_date": datetime.now().date().isoformat(),
            "created_at": datetime.now(),
            "status": "pending",
            "client_queues": {
                "th10021994": {
                    "stories": [
                        {
                            "file_name": "0001-2712-1639-6435.jpg",
                            "cdn_url": "https://snapped2.b-cdn.net/SNAPPED_CLIENTS/th10021994/STORIES_th10021994/F(12-27-2024)_th10021994/0001-2712-1639-6435.jpg",
                            "scheduled_time": datetime.now().isoformat(),
                            "snap_id": "th10021994",
                            "timezone": "America/New_York",
                            "snapchat_publish_as": "STORY",
                            "session_id": "F(12-27-2024)_th10021994"
                        }
                    ],
                    "processed": False
                }
            },
            "total_posts": 1
        }

        builder = QueueBuilder()
        result = await builder.queues.insert_one(test_data)
        
        return {"status": "success", "id": str(result.inserted_id)}
    except Exception as e:
        logger.error(f"Error in test queue: {str(e)}")
        raise

@router.post("/build")
async def build_queue_endpoint():
    try:
        builder = QueueBuilder()
        result = await builder.build_daily_queue()
        return {
            "status": "success",
            "total_posts": result['total_posts'],
            "queue_date": result['queue_date'],
            "client_queues": result['client_queues']
        }
    except Exception as e:
        logger.error(f"Error building queue: {str(e)}")
        raise

@router.post("/test/uploads")
async def test_uploads():
    """Test endpoint to read from UploadDB.Uploads"""
    try:
        builder = QueueBuilder()
        
        # Get all documents
        documents = []
        async for doc in builder.uploads.find({}):
            documents.append({
                "client_ID": doc.get("client_ID"),
                "sessions": [{
                    "upload_date": session.get("upload_date"),
                    "queued": session.get("queued", False),
                    "session_id": session.get("session_id")
                } for session in doc.get("sessions", [])]
            })
        
        return {
            "status": "success",
            "total_documents": len(documents),
            "documents": documents
        }
    except Exception as e:
        logger.error(f"Error reading uploads: {str(e)}")
        raise

@router.post("/test/posts")
async def test_posts():
    """Test endpoint to read posts from UploadDB.Uploads"""
    try:
        builder = QueueBuilder()
        
        posts = []
        async for doc in builder.uploads.find({}):
            client_id = doc.get("client_ID")
            for session in doc.get("sessions", []):
                session_files = [
                    {
                        "client_id": client_id,
                        "session_id": session.get("session_id"),
                        "file_name": file.get("file_name"),
                        "cdn_url": file.get("CDN_link"),
                        "file_type": file.get("file_type"),
                        "seq_number": file.get("seq_number"),
                        "upload_date": session.get("upload_date"),
                        "queued": file.get("queued", False)
                    }
                    for file in session.get("files", [])
                    if not file.get("is_thumbnail", False)  # Skip thumbnails
                ]
                posts.extend(session_files)
        
        return {
            "status": "success",
            "total_posts": len(posts),
            "posts": posts
        }
    except Exception as e:
        logger.error(f"Error reading posts: {str(e)}")
        raise

@router.post("/build/{client_name}")
async def build_client_queue(client_name: str, posts: List[Dict]):
    """
    Build a post queue for a specific client.
    
    Args:
        client_name (str): Client identifier
        posts (List[Dict]): Post data to queue
        
    Returns:
        dict: Operation status
        
    Raises:
        HTTPException: For processing errors
        
    Notes:
        - Validates input
        - Builds queue
        - Updates status
        - Handles errors
    """
    builder = QueueBuilder()
    success = await builder.build_queue(posts, client_name)
    
    if not success:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to build queue for {client_name}"
        )
    
    return {"status": "success"}

@router.get("/status")
async def get_queue_status(client_name: Optional[str] = None):
    """
    Get the current status of the post queue.
    
    Args:
        client_name (Optional[str]): Client to check status for
        
    Returns:
        dict: Queue status information
        
    Raises:
        HTTPException: For processing errors
        
    Notes:
        - Retrieves status
        - Validates data
        - Reports state
        - Handles errors
    """
    builder = QueueBuilder()
    status = await builder.get_queue_status(client_name)
    return status

if __name__ == "__main__":
    import asyncio
    asyncio.run(build_queue()) 