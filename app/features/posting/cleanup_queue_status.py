"""
Queue Status Cleanup Module

This module provides functionality for cleaning up queue status flags in the
Spotlights collection. It handles both regular and TikTok sessions, ensuring
that queued flags and timestamps are properly reset.

Features:
--------
1. Status Management:
   - Queue flag cleanup
   - Timestamp removal
   - Session tracking
   - Batch processing

2. Collection Handling:
   - Regular sessions
   - TikTok sessions
   - File status updates
   - Document verification

3. Error Handling:
   - Client validation
   - Session verification
   - Update tracking
   - Error logging

Data Model:
----------
Spotlights Structure:
- Client identification
- Session management
- File tracking
- Queue status

Dependencies:
-----------
- MongoDB: Data storage
- asyncio: Async operations
- logging: Debug tracking

Author: Snapped Development Team
"""

from app.shared.database import async_client
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

async def cleanup_queue_status():
    """
    Remove queue status from all files in the Spotlights collection.
    
    Processes all documents in the Spotlights collection, resetting
    queue status flags and removing queue timestamps for both regular
    and TikTok sessions.
    
    Returns:
        None
        
    Notes:
        - Handles regular sessions
        - Processes TikTok sessions
        - Updates file status
        - Maintains audit trail
    """
    try:
        spotlights = async_client['UploadDB']['Spotlights']
        
        # Get all spotlight documents
        async for doc in spotlights.find({}):
            client_id = doc.get('client_ID') or doc.get('client_id')
            if not client_id:
                continue
                
            logger.info(f"Processing cleanup for client: {client_id}")
            
            # Process regular sessions
            if 'sessions' in doc:
                for session in doc['sessions']:
                    if 'files' in session:
                        logger.info(f"Cleaning regular session {session.get('session_id')}")
                        await spotlights.update_one(
                            {
                                "client_ID": client_id,
                                "sessions.session_id": session['session_id']
                            },
                            {
                                "$set": {
                                    "sessions.$[outer].files.$[inner].queued": False
                                },
                                "$unset": {
                                    "sessions.$[outer].files.$[inner].queue_time": ""
                                }
                            },
                            array_filters=[
                                {"outer.session_id": session['session_id']},
                                {"inner.queued": True}  # Only update files that are currently queued
                            ]
                        )
            
            # Process TikTok sessions
            if 'tt_sessions' in doc:
                for session in doc['tt_sessions']:
                    if 'files' in session:
                        logger.info(f"Cleaning TikTok session {session.get('session_id')}")
                        await spotlights.update_one(
                            {
                                "client_ID": client_id,
                                "tt_sessions.session_id": session['session_id']
                            },
                            {
                                "$set": {
                                    "tt_sessions.$[outer].files.$[inner].queued": False
                                },
                                "$unset": {
                                    "tt_sessions.$[outer].files.$[inner].queue_time": ""
                                }
                            },
                            array_filters=[
                                {"outer.session_id": session['session_id']},
                                {"inner.queued": True}  # Only update files that are currently queued
                            ]
                        )
                
        logger.info("Queue status cleanup completed")
        
    except Exception as e:
        logger.error(f"Error in cleanup_queue_status: {str(e)}")
        raise

async def main():
    """
    Main function to run the cleanup script.
    
    Initializes and executes the queue status cleanup process,
    handling logging and error reporting.
    
    Notes:
        - Entry point for script
        - Manages logging setup
        - Handles process errors
        - Reports completion
    """
    logger.info("Starting queue status cleanup script")
    await cleanup_queue_status()
    logger.info("Cleanup script completed")

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run the cleanup script
    asyncio.run(main()) 