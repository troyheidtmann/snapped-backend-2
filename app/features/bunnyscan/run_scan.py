"""
Bunny Scanner Orchestration Module

This module serves as the main orchestrator for the Snapchat content scanning process,
coordinating video processing and database synchronization operations.

System Architecture:
    1. Components:
        - VideoClipper (BunnyScanner):
            * Video content processing
            * Duration validation
            * Orientation checks
            * Format standardization
        
        - DatabaseScanner (BunnyScanner):
            * Content discovery
            * Metadata extraction
            * Database synchronization
            * Session management
    
    2. Processing Pipeline:
        a) Video Processing Phase:
            - Folder traversal
            - Video validation
            - Format conversion
            - Thumbnail generation
        
        b) Database Sync Phase:
            - Content discovery
            - Metadata extraction
            - Collection updates
            - Session management
    
    3. Execution Flow:
        1. Initialize scanners
        2. Process video content
        3. Update database records
        4. Handle errors and cleanup

Dependencies:
    - VideoClipper: Video processing and standardization
    - DatabaseScanner: Content management and storage
    - AsyncIO: Asynchronous execution handling

Error Handling:
    - Component failures
    - Processing errors
    - Database conflicts
    - Resource cleanup
"""

from datetime import datetime
import asyncio
from app.features.bunnyscan.bunny_scanner_videocut import BunnyScanner as VideoClipper
from app.features.bunnyscan.bunny_scanner import BunnyScanner as DatabaseScanner

async def main():
    """
    Main execution function for the Bunny Scanner pipeline.
    
    Processing Flow:
        1. Video Processing:
            - Initialize VideoClipper
            - Process all content folders
            - Handle video standardization
            - Generate thumbnails
        
        2. Database Synchronization:
            - Initialize DatabaseScanner
            - Discover content changes
            - Update collection records
            - Manage content sessions
        
    Error Handling:
        - Component initialization failures
        - Processing pipeline errors
        - Database synchronization issues
        - Resource management
    
    Execution Order:
        1. Run VideoClipper to process and standardize video content
        2. Run DatabaseScanner to update database records
    """
    # Run VideoClipper first
    await VideoClipper().scan_all_folders()
    # Then run DatabaseScanner
    await DatabaseScanner().scan_uploads()

if __name__ == "__main__":
    asyncio.run(main())

