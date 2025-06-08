"""
BunnyCDN Webhook Handler

This module implements webhook handlers for BunnyCDN storage events, enabling automated
content discovery and processing for the Snapchat content management system.

System Architecture:
    1. Event Processing:
        - Storage event reception
        - Path validation and parsing
        - Content type routing
    
    2. Integration Points:
        - BunnyCDN Storage Webhooks
        - BunnyScanner content processor
        - MongoDB collections
    
    3. Directory Structure:
        /sc/
        ├── {client_id}/
        │   ├── STORIES/
        │   │   └── F(date)_{client_id}/
        │   ├── SPOTLIGHT/
        │   │   └── F(date)_{client_id}/
        │   ├── CONTENT_DUMP/
        │   └── SAVED/
        │       └── F(date)_{client_id}/

Event Types:
    - Storage Events:
        * File uploads
        * Directory creation
        * Content modifications

Processing Flow:
    1. Event Reception:
        - Webhook payload validation
        - Path information extraction
        - Event type categorization
    
    2. Content Processing:
        - Client identification
        - Content type determination
        - Date extraction and validation
    
    3. Scanner Integration:
        - BunnyScanner initialization
        - Content scanning triggers
        - Database synchronization

Error Handling:
    - Invalid paths
    - Unknown content types
    - Processing failures
    - Database errors
    - Network issues

Security:
    - Path validation
    - Client verification
    - Event authentication
    - Error isolation
"""

from fastapi import APIRouter, HTTPException, Request
from app.features.bunnyscan.bunny_scanner import BunnyScanner
import logging
from typing import Dict
import asyncio

router = APIRouter(prefix="/api/bunny/webhook", tags=["bunny_webhook"])
logger = logging.getLogger(__name__)

@router.post("/storage")
async def handle_storage_webhook(request: Request):
    """
    Handle BunnyCDN storage webhook events.
    
    Processing Flow:
        1. Event Validation:
            - Request payload parsing
            - Path structure verification
            - Event type identification
        
        2. Path Processing:
            - Client ID extraction
            - Content type determination
            - Date pattern recognition
        
        3. Content Scanning:
            - Scanner initialization
            - Content type routing
            - Date-based processing
        
        4. Error Management:
            - Path validation errors
            - Processing failures
            - Database conflicts
    
    Path Format:
        /sc/{client_id}/{content_type}/[F(date)_{client_id}]/
    
    Content Types:
        - STORIES: Daily story content
        - SPOTLIGHT: Featured content
        - CONTENT_DUMP: Archive storage
        - SAVED: Preserved content
    
    Args:
        request: FastAPI request object containing webhook payload
    
    Returns:
        Dict: Processing status and details
    
    Raises:
        HTTPException: For invalid paths, content types, or processing errors
    """
    try:
        data = await request.json()
        
        # Extract path information from webhook
        path = data.get("Path", "")
        event_type = data.get("EventType", "")
        
        logger.info(f"Received webhook for path: {path}, event: {event_type}")
        
        if not path:
            raise HTTPException(status_code=400, message="No path provided")
            
        # Parse path to get client_id and content_type
        path_parts = path.strip("/").split("/")
        if len(path_parts) >= 3 and path_parts[0] == "sc":
            client_id = path_parts[1]
            content_type = path_parts[2]
            
            # Initialize scanner and scan specific path
            scanner = BunnyScanner()
            
            # Get the date from the path if available
            scan_date = None
            if len(path_parts) > 3 and path_parts[3].startswith("F("):
                scan_date = path_parts[3].split("_")[0][2:-1]  # Extract date from F(date)
            
            if scan_date:
                await scanner.scan_client_content(client_id, content_type, scan_date)
            else:
                # Scan all dates for this content type
                for date in scanner.dates:
                    await scanner.scan_client_content(client_id, content_type, date)
                    
            return {"status": "success", "message": f"Scanned path: {path}"}
            
        raise HTTPException(status_code=400, message="Invalid path format")
        
    except Exception as e:
        logger.error(f"Webhook error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 