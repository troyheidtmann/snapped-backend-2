"""
Webhooks Module

This module provides webhook endpoints for handling external service
callbacks and notifications.

Features:
- HelloSign webhooks
- Event verification
- Signature handling
- Status tracking
- Error handling

Data Model:
- Event data
- Signature data
- Status updates
- Processing results

Security:
- Signature verification
- API key validation
- Event validation
- Error handling

Dependencies:
- FastAPI for routing
- hashlib for signatures
- hmac for verification
- logging for tracking

Author: Snapped Development Team
"""

from fastapi import APIRouter, Request, HTTPException
from app.shared.services.signing import SigningService
from app.shared.config import HELLOSIGN_WEBHOOK_KEY
import hashlib
import hmac
import logging

router = APIRouter(
    prefix="/api/webhooks",
    tags=["webhooks"]
)

logger = logging.getLogger(__name__)
signing_service = SigningService()

@router.post("/hellosign")
async def hellosign_webhook(request: Request):
    """
    Handle HelloSign webhook events.
    
    Args:
        request: FastAPI request object
        
    Returns:
        dict: Event processing status
        
    Raises:
        HTTPException: For invalid or failed events
        
    Notes:
        - Verifies signature
        - Processes events
        - Updates status
        - Handles errors
    """
    try:
        # Get the raw JSON data
        event_data = await request.json()
        
        # Verify webhook authenticity using HelloSign's event hash
        event_hash = request.headers.get('X-HelloSign-Signature')
        if not verify_hellosign_webhook(event_hash, event_data, HELLOSIGN_WEBHOOK_KEY):
            raise HTTPException(status_code=401, detail="Invalid webhook signature")

        # Handle different event types
        event_type = event_data.get('event').get('event_type')
        
        logger.info(f"Received HelloSign webhook event: {event_type}")
        
        if event_type == 'signature_request_signed':
            # Process signed document
            await signing_service.handle_signed_callback(event_data['signature_request'])
            return {"status": "success", "message": "Signature processed"}
            
        elif event_type == 'signature_request_viewed':
            # Optionally track when contract is viewed
            return {"status": "success", "message": "View tracked"}
            
        elif event_type == 'signature_request_declined':
            # Handle declined signatures
            return {"status": "success", "message": "Decline processed"}
            
        return {"status": "success", "message": f"Unhandled event type: {event_type}"}

    except Exception as e:
        logger.error(f"Error processing HelloSign webhook: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

def verify_hellosign_webhook(event_hash: str, event_data: dict, api_key: str) -> bool:
    """
    Verify HelloSign webhook authenticity.
    
    Args:
        event_hash: Event signature hash
        event_data: Event payload
        api_key: API key for verification
        
    Returns:
        bool: Verification status
        
    Notes:
        - Creates HMAC
        - Verifies hash
        - Secure comparison
        - UTF-8 encoding
    """
    # Create the HMAC-SHA256 hash
    raw = str(event_data).encode('utf-8')
    signature = hmac.new(
        api_key.encode('utf-8'),
        raw,
        hashlib.sha256
    ).hexdigest()
    
    return hmac.compare_digest(signature, event_hash) 