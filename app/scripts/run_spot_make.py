"""
Spotlight Make Runner Module

This module manages the execution of Make integration processing
for Spotlight content queues.

Features:
- Spotlight processing
- API integration
- Error handling
- Logging
- Status tracking

Data Model:
- Queue entries
- Processing status
- Error logs
- Timestamps
- API responses

Dependencies:
- requests for API calls
- logging for tracking
- datetime for timestamps

Author: Snapped Development Team
"""

import requests
import os
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Run Spotlight queue processing.
    
    Returns:
        None
        
    Notes:
        - Triggers API
        - Handles responses
        - Logs status
        - Error handling
    """
    url = "https://track.snapped.cc/api/spot-queue/process-make"
    
    try:
        response = requests.post(url)
        response.raise_for_status()
        logger.info("Successfully triggered spotlight queue processing")
        
    except Exception as e:
        logger.error(f"Failed to trigger spotlight queue processing: {e}")
        raise

if __name__ == "__main__":
    main() 