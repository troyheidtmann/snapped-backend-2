"""
Daily Queue Runner Module

This module manages the execution of daily content queue building
operations.

Features:
- Queue building
- Schedule management
- Error handling
- Logging
- Status tracking

Data Model:
- Queue entries
- Schedule data
- Processing status
- Error logs
- Timestamps

Dependencies:
- QueueBuilder for processing
- logging for tracking
- asyncio for async ops

Author: Snapped Development Team
"""

import asyncio
import logging
from app.features.posting.queue_builder import QueueBuilder

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """
    Run daily queue building process.
    
    Returns:
        None
        
    Notes:
        - Creates builder
        - Builds queue
        - Handles errors
        - Logs status
    """
    logger.info("Starting queue builder script")
    try:
        builder = QueueBuilder()
        logger.info("Created QueueBuilder instance")
        await builder.build_daily_queue()
        logger.info("Completed building queue")
    except Exception as e:
        logger.error(f"Error running queue builder: {str(e)}")
        raise

if __name__ == "__main__":
    logger.info("Script started")
    asyncio.run(main())
    logger.info("Script completed")