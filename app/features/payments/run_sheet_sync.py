"""
Payout Sheet Synchronization Runner

This module provides the entry point for synchronizing payout data between
Google Sheets and MongoDB. It handles the initialization and execution of
the synchronization process with comprehensive logging.

Features:
--------
1. Synchronization:
   - Automated sheet-to-MongoDB sync
   - Status tracking
   - Error handling
   - Logging

2. Process Management:
   - Async execution
   - Status reporting
   - Exception handling
   - Debug logging

Dependencies:
-----------
- asyncio: Asynchronous I/O
- PayoutSheetSync: Core sync functionality
- logging: Debug tracking

Usage:
-----
Run this script directly to initiate a sync:
```
python run_sheet_sync.py
```

Author: Snapped Development Team
"""

import asyncio
from app.features.payments.sheet_sync import PayoutSheetSync
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """
    Main execution function for payout sheet synchronization.
    
    Initializes the PayoutSheetSync system and runs the synchronization
    process, including status checks and error handling.
    
    Returns:
        None
        
    Raises:
        Exception: Any errors during sync process
        
    Notes:
        - Initializes PayoutSheetSync instance
        - Runs sync_to_mongo process
        - Retrieves and logs sync status
        - Handles and logs any errors
        - Uses INFO level logging for progress
        - Uses ERROR level for exceptions
    """
    try:
        logger.info("Starting payout sheet sync...")
        
        # Initialize syncer
        syncer = PayoutSheetSync()
        
        # Run sync
        logger.info("Running sync process...")
        result = await syncer.sync_to_mongo()
        logger.info("Sync Result: %s", result)
        
        # Get status
        status = await syncer.get_sync_status()
        logger.info("Current Status: %s", status)
        
    except Exception as e:
        logger.error("Error running sync: %s", str(e))
        raise

if __name__ == "__main__":
    asyncio.run(main()) 