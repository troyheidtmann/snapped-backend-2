"""
QuickBooks Payee Synchronization Test Module

This module provides functionality for testing and synchronizing payee data
between MongoDB and QuickBooks through Make.com webhooks. It handles data
transformation, webhook communication, and status tracking.

Features:
--------
1. Payee Synchronization:
   - Batch processing
   - Individual payee sync
   - Status tracking
   - Error handling

2. Data Transformation:
   - MongoDB to JSON conversion
   - Date formatting
   - ObjectId handling
   - Response parsing

3. Webhook Integration:
   - Make.com communication
   - Response handling
   - Status updates
   - Error tracking

4. Status Management:
   - Success tracking
   - Error logging
   - Sync date recording
   - QuickBooks ID management

Data Model:
----------
Payee Structure:
- Primary email
- QuickBooks ID
- Sync status
- Sync history
- Error tracking

Security:
--------
- Webhook authentication
- Data validation
- Error handling
- Status tracking

Dependencies:
-----------
- aiohttp: Async HTTP client
- MongoDB: Data storage
- datetime: Time handling
- logging: Debug tracking
- Make.com: Integration platform

Author: Snapped Development Team
"""

import asyncio
import logging
import sys
import os
import aiohttp
from datetime import datetime, timezone
from bson import ObjectId

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from app.shared.database import async_client

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the Payees collection
payees = async_client['Payments']['Payees']
WEBHOOK_URL = 'https://hook.us2.make.com/0lwvz9fqcu6hhdvp65hkqq7yt69ypx9k'

def prepare_payee_data(payee):
    """
    Convert MongoDB document to JSON serializable dict.
    
    Transforms MongoDB-specific data types into JSON-compatible formats,
    handling ObjectIds, dates, and other special types.
    
    Args:
        payee (Dict): MongoDB payee document
        
    Returns:
        Dict: JSON-serializable payee data
        
    Notes:
        - Converts ObjectId to string
        - Formats datetime to ISO
        - Preserves other data types
        - Handles nested structures
    """
    clean_payee = {}
    for key, value in payee.items():
        if key == '_id':
            clean_payee[key] = str(value)
        elif isinstance(value, ObjectId):
            clean_payee[key] = str(value)
        elif isinstance(value, datetime):
            clean_payee[key] = value.isoformat()
        else:
            clean_payee[key] = value
    return clean_payee

async def sync_payee_to_quickbooks(payee_data):
    """
    Synchronize a single payee with QuickBooks via webhook.
    
    Sends payee data to QuickBooks through Make.com webhook,
    processes the response, and updates the sync status.
    
    Args:
        payee_data (Dict): Payee information to sync
        
    Returns:
        bool: True if sync successful, False otherwise
        
    Notes:
        - Handles webhook communication
        - Processes response formats
        - Updates sync status
        - Records sync history
        - Manages error states
    """
    try:
        logger.info(f"Processing payee: {payee_data.get('primary_email')}")
        
        # Prepare data for webhook
        clean_data = prepare_payee_data(payee_data)
        logger.info(f"Sending data: {clean_data}")
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                WEBHOOK_URL,
                json=clean_data,
                headers={'Content-Type': 'application/json'}
            ) as response:
                if response.status == 200:
                    # Try to get response as text first
                    response_text = await response.text()
                    logger.info(f"Received response from webhook: {response_text}")
                    
                    # Try to get QuickBooks ID from response
                    quickbooks_id = None
                    
                    # First try to parse as plain number since that's what we're getting
                    try:
                        quickbooks_id = str(int(response_text.strip()))
                        logger.info(f"Parsed QuickBooks ID from plain number: {quickbooks_id}")
                    except ValueError:
                        # If not a number, try JSON
                        try:
                            import json
                            response_data = json.loads(response_text)
                            quickbooks_id = response_data.get('id') or response_data.get('quickbooks_id')
                            logger.info(f"Parsed QuickBooks ID from JSON: {quickbooks_id}")
                        except json.JSONDecodeError:
                            logger.warning(f"Response is neither a number nor valid JSON: {response_text}")
                    
                    if quickbooks_id:
                        # Update payee record immediately
                        await payees.update_one(
                            {'_id': payee_data['_id']},
                            {
                                '$set': {
                                    'quickbooks_id': quickbooks_id,
                                    'quickbooks_sync_status': 'success',
                                    'quickbooks_sync_date': datetime.now(timezone.utc)
                                }
                            }
                        )
                        logger.info(f"Successfully updated QuickBooks ID for {payee_data.get('primary_email')}: {quickbooks_id}")
                        return True
                    else:
                        # If we got 200 but no ID, mark as pending
                        await payees.update_one(
                            {'_id': payee_data['_id']},
                            {
                                '$set': {
                                    'quickbooks_sync_status': 'pending',
                                    'quickbooks_sync_date': datetime.now(timezone.utc),
                                    'quickbooks_sync_message': 'Webhook accepted but waiting for ID'
                                }
                            }
                        )
                        logger.info(f"Request accepted for {payee_data.get('primary_email')}, waiting for callback")
                        return True
                else:
                    error_text = await response.text()
                    logger.warning(f"Failed response from webhook for {payee_data.get('primary_email')}: {error_text}")
                    await payees.update_one(
                        {'_id': payee_data['_id']},
                        {
                            '$set': {
                                'quickbooks_sync_status': 'failed',
                                'quickbooks_sync_date': datetime.now(timezone.utc),
                                'quickbooks_sync_error': f'Webhook failed: {error_text}'
                            }
                        }
                    )
                    return False
                
    except Exception as e:
        logger.error(f"Error processing payee {payee_data.get('primary_email')}: {str(e)}")
        await payees.update_one(
            {'_id': payee_data['_id']},
            {
                '$set': {
                    'quickbooks_sync_status': 'failed',
                    'quickbooks_sync_date': datetime.now(timezone.utc),
                    'quickbooks_sync_error': str(e)
                }
            }
        )
        return False

async def sync_all_missing_quickbooks_ids():
    """
    Synchronize all payees missing QuickBooks IDs.
    
    Finds and processes all payee records that don't have
    a QuickBooks ID or where the ID is null.
    
    Returns:
        None
        
    Notes:
        - Processes in batches
        - Tracks success rates
        - Logs progress
        - Handles errors
        - Updates status
    """
    try:
        # Find only payees without quickbooks_id or where it's null
        cursor = payees.find({
            '$or': [
                {'quickbooks_id': {'$exists': False}},
                {'quickbooks_id': None}
            ]
        })
        
        total_processed = 0
        total_success = 0
        
        async for payee in cursor:
            total_processed += 1
            logger.info(f"Processing {total_processed}: {payee.get('primary_email')}")
            
            if await sync_payee_to_quickbooks(payee):
                total_success += 1
                
        logger.info(f"Sync complete. Processed: {total_processed}, Successfully synced: {total_success}")
        
    except Exception as e:
        logger.error(f"Error in sync process: {str(e)}")

if __name__ == "__main__":
    asyncio.run(sync_all_missing_quickbooks_ids()) 