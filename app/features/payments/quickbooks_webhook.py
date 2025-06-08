"""
QuickBooks Integration API - Payee Synchronization System

This module provides FastAPI routes and utilities for synchronizing payee data
between the Snapped platform and QuickBooks, handling both individual and bulk
synchronization processes through Make.com webhooks.

Features:
--------
1. Payee Synchronization:
   - Individual payee sync
   - Bulk synchronization
   - Sync status tracking
   - Error handling

2. Integration Management:
   - Make.com webhook integration
   - QuickBooks ID management
   - Callback processing
   - Status monitoring

3. Data Tracking:
   - Sync history
   - Status updates
   - Error logging
   - Audit trail

Data Model:
----------
Payee Structure:
- Basic: Email, name, status
- QuickBooks: ID, sync status, sync date
- History: Sync attempts, errors, timestamps

Security:
--------
- Error handling
- Data validation
- Sync status tracking
- Audit logging
- Webhook security

Dependencies:
-----------
- FastAPI: Web framework
- MongoDB: Data storage
- aiohttp: Async HTTP client
- Pydantic: Data validation
- logging: Debug tracking

Author: Snapped Development Team
"""

import aiohttp
import logging
from datetime import datetime
from typing import Dict, Any
from app.shared.database import async_client
from fastapi import APIRouter, HTTPException
from bson.objectid import ObjectId
from pydantic import BaseModel

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the collections
payees = async_client['Payments']['Payees']
qb_sync_history = async_client['Payments']['QuickbooksSync']

router = APIRouter(prefix="/api/payments/quickbooks")

WEBHOOK_URL = 'https://hook.us2.make.com/0lwvz9fqcu6hhdvp65hkqq7yt69ypx9k'

class PayeeEmail(BaseModel):
    """
    Pydantic model for payee email validation.
    
    Attributes:
        payee_email (str): Email address of the payee
    """
    payee_email: str

async def send_to_quickbooks(payee_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Send payee data to QuickBooks through Make.com webhook.
    
    Processes payee data, sends it to QuickBooks, and handles
    the response including ID storage and status tracking.
    
    Args:
        payee_data (Dict[str, Any]): Complete payee information
        
    Returns:
        Dict[str, Any]: Operation result containing:
            - status: Success/error indicator
            - quickbooks_id: Assigned ID (if successful)
            - message: Error details (if failed)
            
    Notes:
        - Converts ObjectId and datetime to serializable format
        - Tracks sync history in database
        - Updates payee record with QuickBooks ID
        - Handles errors with detailed logging
    """
    try:
        # Helper function to make data JSON serializable
        def make_json_serializable(data):
            if isinstance(data, dict):
                return {k: make_json_serializable(v) for k, v in data.items()}
            elif isinstance(data, (list, tuple)):
                return [make_json_serializable(x) for x in data]
            elif isinstance(data, ObjectId):
                return str(data)
            elif isinstance(data, datetime):
                return data.isoformat()
            return data

        payee_data = make_json_serializable(payee_data)
            
        logger.info(f"Sending payee to QuickBooks: {payee_data.get('primary_email')}")
        
        sync_record = {
            'primary_email': payee_data.get('primary_email'),
            'attempt_date': datetime.utcnow(),
            'status': 'pending'
        }
        sync_id = await qb_sync_history.insert_one(sync_record)
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                WEBHOOK_URL,
                json=payee_data,
                headers={'Content-Type': 'application/json'}
            ) as response:
                response_text = await response.text()
                logger.info(f"Received QuickBooks ID: {response_text}")  # This is actually the ID!

                # The response text IS the QuickBooks ID
                quickbooks_id = response_text.strip()

                if quickbooks_id:
                    # Update payee record with the ID
                    await payees.update_one(
                        {'primary_email': payee_data.get('primary_email')},
                        {'$set': {
                            'quickbooks_id': quickbooks_id,
                            'quickbooks_sync_status': 'success',
                            'quickbooks_sync_date': datetime.utcnow()
                        }}
                    )
                    
                    # Update sync history
                    await qb_sync_history.update_one(
                        {'primary_email': payee_data.get('primary_email')},
                        {'$set': {
                            'status': 'success',
                            'quickbooks_id': quickbooks_id,
                            'completed_date': datetime.utcnow()
                        }}
                    )

                    return {
                        'status': 'success',
                        'quickbooks_id': quickbooks_id
                    }
                
                # If we got here, something went wrong
                error_message = response_text
                await qb_sync_history.update_one(
                    {'_id': sync_record['_id']},
                    {'$set': {
                        'status': 'error',
                        'error': error_message,
                        'completed_date': datetime.utcnow()
                    }}
                )
                
                return {
                    'status': 'error',
                    'message': error_message
                }
                
    except Exception as e:
        logger.error(f"Error in QuickBooks sync: {str(e)}")
        if 'sync_record' in locals():
            await qb_sync_history.update_one(
                {'_id': sync_record['_id']},
                {'$set': {
                    'status': 'error',
                    'error': str(e),
                    'completed_date': datetime.utcnow()
                }}
            )
        return {'status': 'error', 'message': str(e)}

@router.post("/sync")
async def sync_to_quickbooks(data: PayeeEmail):
    """
    Manually trigger QuickBooks synchronization for a payee.
    
    Endpoint to initiate QuickBooks sync for a specific payee
    identified by their email address.
    
    Args:
        data (PayeeEmail): Payee email model
        
    Returns:
        Dict: Sync operation result
        
    Raises:
        HTTPException: For missing payee or sync errors
        
    Notes:
        - Verifies payee existence
        - Triggers sync process
        - Returns sync status
    """
    try:
        # Get payee data
        payee = await payees.find_one({'primary_email': data.payee_email})
        if not payee:
            raise HTTPException(status_code=404, detail="Payee not found")
            
        # Send to QuickBooks
        result = await send_to_quickbooks(payee)
        return result
        
    except Exception as e:
        logger.error(f"Error in sync endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/sync-status/{email}")
async def get_sync_status(email: str):
    """
    Retrieve QuickBooks synchronization status for a payee.
    
    Gets current sync status and historical sync attempts for
    a payee identified by their email address.
    
    Args:
        email (str): Payee's email address
        
    Returns:
        Dict: Status information containing:
            - quickbooks_id: Current QuickBooks ID
            - last_sync_date: Latest sync timestamp
            - sync_history: List of sync attempts with:
                - attempt_date: Sync initiation time
                - status: Operation result
                - completed_date: Completion timestamp
                - error: Error details if failed
                
    Raises:
        HTTPException: For database errors
    """
    try:
        # Get sync history
        history = await qb_sync_history.find(
            {'primary_email': email}
        ).sort('attempt_date', -1).to_list(10)
        
        # Get current QuickBooks status from payee record
        payee = await payees.find_one({'primary_email': email})
        
        return {
            'quickbooks_id': payee.get('quickbooks_id') if payee else None,
            'last_sync_date': payee.get('quickbooks_sync_date') if payee else None,
            'sync_history': [
                {
                    'attempt_date': h['attempt_date'],
                    'status': h['status'],
                    'completed_date': h.get('completed_date'),
                    'error': h.get('error')
                } for h in history
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting sync status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bulk-sync")
async def bulk_sync_to_quickbooks():
    """
    Synchronize all unsynchronized payees with QuickBooks.
    
    Identifies and processes all payees that don't have a
    QuickBooks ID assigned.
    
    Returns:
        Dict: Bulk operation results containing:
            - total_processed: Number of payees processed
            - results: List of sync results with:
                - email: Payee email
                - status: Sync status
                
    Raises:
        HTTPException: For sync process errors
        
    Notes:
        - Processes payees without QuickBooks ID
        - Tracks individual sync results
        - Returns aggregate statistics
    """
    try:
        # Find payees without QuickBooks ID
        pending_payees = await payees.find(
            {'quickbooks_id': {'$exists': False}}
        ).to_list(None)
        
        results = []
        for payee in pending_payees:
            result = await send_to_quickbooks(payee)
            results.append({
                'email': payee.get('primary_email'),
                'status': result['status']
            })
            
        return {
            'total_processed': len(results),
            'results': results
        }
        
    except Exception as e:
        logger.error(f"Error in bulk sync: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/callback/{payee_id}")
async def quickbooks_callback(payee_id: str, response: Dict[str, Any]):
    """
    Handle QuickBooks integration callback from Make.com.
    
    Processes the callback response from Make.com after QuickBooks
    integration, updating payee records with results.
    
    Args:
        payee_id (str): Payee identifier
        response (Dict[str, Any]): Callback data containing:
            - quickbooks_id: Assigned QuickBooks ID
            
    Returns:
        Dict: Operation status
        
    Raises:
        HTTPException: For invalid responses or database errors
        
    Notes:
        - Validates QuickBooks ID
        - Updates payee record
        - Handles errors with status updates
        - Maintains audit trail
    """
    try:
        logger.info(f"Received QuickBooks callback for payee {payee_id}: {response}")
        
        quickbooks_id = response.get('quickbooks_id')
        if not quickbooks_id:
            raise HTTPException(status_code=400, detail="No QuickBooks ID in response")
            
        # Update payee record
        result = await payees.update_one(
            {'_id': ObjectId(payee_id)},
            {
                '$set': {
                    'quickbooks_id': quickbooks_id,
                    'quickbooks_sync_status': 'success',
                    'quickbooks_sync_date': datetime.utcnow()
                }
            }
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Payee not found")
            
        return {"status": "success", "message": "QuickBooks ID updated"}
        
    except Exception as e:
        logger.error(f"Error in callback: {str(e)}")
        # Update status to failed
        await payees.update_one(
            {'_id': ObjectId(payee_id)},
            {
                '$set': {
                    'quickbooks_sync_status': 'failed',
                    'quickbooks_sync_error': str(e)
                }
            }
        )
        raise HTTPException(status_code=500, detail=str(e)) 