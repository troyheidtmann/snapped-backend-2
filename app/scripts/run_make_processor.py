"""
Make Processor Runner Module

This module manages the execution of Make integration processing
for content queues.

Features:
- Queue processing
- Make integration
- Error handling
- Logging
- Status tracking

Data Model:
- Queue entries
- Processing status
- Error logs
- Timestamps
- Integration data

Dependencies:
- MakeProcessor for processing
- logging for tracking
- asyncio for async ops

Author: Snapped Development Team
"""

import asyncio
import logging
from app.features.posting.make_processor import MakeProcessor

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """
    Run Make processor for all queues.
    
    Returns:
        None
        
    Notes:
        - Creates processor
        - Processes queues
        - Handles errors
        - Logs status
    """
    logger.info("Starting Make processor script")
    try:
        processor = MakeProcessor()
        logger.info("Created MakeProcessor instance")
        await processor.process_all_queues()
        logger.info("Completed processing queues")
    except Exception as e:
        logger.error(f"Error running Make processor: {str(e)}")
        raise

if __name__ == "__main__":
    logger.info("Script started")
    asyncio.run(main())
    logger.info("Script completed") 