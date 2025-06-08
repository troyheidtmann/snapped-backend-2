"""
Payment Statement Builder - Monthly Payment Processing System

This module handles the generation of monthly payment statements for payees in the
Snapped platform. It processes creator pulls, calculates splits, and generates
comprehensive payment statements for each payee.

Features:
--------
1. Payment Processing:
   - Monthly statement generation
   - Creator pull aggregation
   - Split calculations
   - Payee statement compilation

2. Data Integration:
   - Client payout tracking
   - Payee information management
   - Split rule application
   - QuickBooks integration

3. Validation:
   - Date range verification
   - Split rule validation
   - Payment calculation
   - Data integrity checks

Data Model:
----------
Statement Structure:
- Month identifier
- Generation timestamp
- Processing status
- Payee statements:
  - Payee information
  - Client splits
  - Total earnings
  - Pull details

Security:
--------
- Error handling
- Data validation
- Logging system
- Transaction tracking

Dependencies:
-----------
- MongoDB: Data storage
- datetime: Timestamp handling
- logging: Debug tracking
- argparse: CLI interface
- BSON: ObjectId handling

Author: Snapped Development Team
"""

import asyncio
from datetime import datetime, timezone
import logging
from typing import List, Dict, Any
import sys
import os
from bson import ObjectId
import argparse

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from app.shared.database import (
    async_client,
    client_payouts,
    payee_info,
    payment_statements
)

# Get collections
splits_collection = async_client["Payments"]["Splits"]
payees_collection = async_client["Payments"]["Payees"]

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def get_month_payouts(month: str) -> Dict[str, List[Dict]]:
    """
    Retrieve and process all payouts for a specific month.
    
    Fetches all client payouts for the specified month, processes the
    pull dates, and groups them by client ID.
    
    Args:
        month (str): Target month in YYYY-MM format
        
    Returns:
        Dict[str, List[Dict]]: Payouts grouped by client_id containing:
            - client_id: Client identifier
            - payout_email: Client's payout email
            - pulls: List of pull transactions
            
    Raises:
        Exception: For date parsing or database errors
        
    Notes:
        - Handles various date string formats
        - Adds UTC timezone if missing
        - Filters pulls within month range
        - Logs processing details and errors
    """
    try:
        # Parse month string (YYYY-MM)
        year, month = map(int, month.split('-'))
        
        # Create datetime objects for start and end dates
        start_date = datetime(year, month, 1, tzinfo=timezone.utc)
        
        # Calculate end date (start of next month)
        if month == 12:
            end_date = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end_date = datetime(year, month + 1, 1, tzinfo=timezone.utc)
            
        logger.info(f"Fetching payouts between {start_date} and {end_date}")
            
        # Find all pulls within date range using datetime objects
        cursor = client_payouts.find({})
        
        # Group pulls by client_id
        client_pulls = {}
        async for payout in cursor:
            logger.info(f"Processing payout: {payout.get('payout_email')} with client_id: {payout.get('client_id')}")
            
            client_id = payout.get('client_id')
            if not client_id:
                logger.warning(f"Skipping payout without client_id: {payout.get('payout_email')}")
                continue
                
            # Filter pulls for this month
            month_pulls = []
            for pull in payout.get('creator_pulls', []):
                try:
                    pull_date = pull['pull_date']
                    
                    # Convert string date to datetime if needed
                    if isinstance(pull_date, str):
                        # Handle various date string formats
                        if pull_date.endswith('Z'):
                            pull_date = pull_date.replace('Z', '+00:00')
                        if '+' not in pull_date and '-' not in pull_date[-6:]:
                            pull_date = pull_date + '+00:00'
                        pull_date = datetime.fromisoformat(pull_date)
                    
                    # Ensure timezone info exists
                    if pull_date.tzinfo is None:
                        pull_date = pull_date.replace(tzinfo=timezone.utc)
                    
                    logger.debug(f"Checking pull date: {pull_date} against range {start_date} to {end_date}")
                    
                    if start_date <= pull_date < end_date:
                        month_pulls.append(pull)
                except Exception as e:
                    logger.warning(f"Error processing pull date: {e}")
                    continue
            
            if month_pulls:
                logger.info(f"Found {len(month_pulls)} pulls for client {client_id} with email {payout.get('payout_email')}")
                client_pulls[client_id] = {
                    'client_id': client_id,
                    'payout_email': payout.get('payout_email'),
                    'pulls': month_pulls
                }
                
        return client_pulls
    except Exception as e:
        logger.error(f"Error getting month payouts: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise

async def get_active_splits() -> List[Dict]:
    """
    Retrieve all active split rules with payee information.
    
    Fetches split rules from the database and enriches them with
    payee details from the payees collection.
    
    Returns:
        List[Dict]: Active splits containing:
            - client_id: Client identifier
            - payee_id: Payee identifier
            - payee_name: Payee's name
            - payee_email: Payee's email
            - quickbooks_id: QuickBooks identifier
            - percentage: Split percentage
            
    Notes:
        - Validates split rule structure
        - Verifies payee existence
        - Logs processing details
        - Handles missing data gracefully
    """
    try:
        # Log the collections we're using
        logger.info("Using collections:")
        logger.info(f"Splits: {splits_collection.name}")
        logger.info(f"Payees: {payees_collection.name}")
        
        # Count total documents in Splits collection
        count = await splits_collection.count_documents({})
        logger.info(f"Total documents in Splits collection: {count}")
        
        # Get all documents from Splits collection to inspect
        all_splits = await splits_collection.find({}).to_list(length=None)
        logger.info("All split documents:")
        for doc in all_splits:
            logger.info(f"Split doc: {doc}")
        
        cursor = splits_collection.find({})
        splits = []
        
        async for split_doc in cursor:
            logger.info(f"Processing split document for client_id: {split_doc.get('client_id')}")
            logger.info(f"Full split document: {split_doc}")
            
            if not split_doc.get('splits'):
                logger.warning(f"No splits array in document for client_id: {split_doc.get('client_id')}")
                continue
                
            client_id = split_doc.get('client_id')
            if not client_id:
                logger.warning("Missing client_id in split document")
                continue
                
            for split in split_doc['splits']:
                logger.info(f"Processing split: {split}")
                payee_id = split.get('payeeId')
                if not payee_id:
                    logger.warning(f"Missing payeeId in split for client {client_id}")
                    continue
                
                try:
                    # Convert string ID to ObjectId
                    payee_object_id = ObjectId(payee_id)
                    
                    # Get payee details
                    payee = await payees_collection.find_one({'_id': payee_object_id})
                    logger.info(f"Looking up payee: {payee_id} -> Found: {payee is not None}")
                    
                    if not payee:
                        logger.warning(f"Payee not found for ID: {payee_id}")
                        continue
                        
                    splits.append({
                        'client_id': client_id,
                        'payee_id': str(payee_object_id),
                        'payee_name': payee.get('name'),
                        'payee_email': payee.get('primary_email'),
                        'quickbooks_id': payee.get('quickbooks_id'),
                        'percentage': split.get('percentage', 0)
                    })
                    logger.info(f"Added split for payee {payee.get('name')} ({payee.get('primary_email')}) - {split.get('percentage')}% on client {client_id}")
                    
                except Exception as e:
                    logger.warning(f"Error processing payee {payee_id}: {str(e)}")
                    continue
                
        logger.info(f"Found {len(splits)} total active splits")
        return splits
    except Exception as e:
        logger.error(f"Error getting active splits: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return []

async def build_monthly_statement(month: str):
    """
    Generate a comprehensive monthly payment statement.
    
    Processes all payouts and splits for the month, calculates
    earnings, and generates detailed statements for each payee.
    
    Args:
        month (str): Target month in YYYY-MM format
        
    Returns:
        dict: Statement document containing:
            - month: Statement month
            - generated_at: Generation timestamp
            - status: Processing status
            - payee_statements: List of payee statements with:
                - payee details
                - client splits
                - total earnings
                - pull transactions
                
    Notes:
        - Aggregates all monthly pulls
        - Applies split rules
        - Calculates earnings per payee
        - Saves statement to database
        - Handles errors gracefully
    """
    try:
        logger.info(f"Building statement for month: {month}")
        
        # Get all payouts for the month
        client_pulls = await get_month_payouts(month)
        logger.info(f"Found pulls for {len(client_pulls)} clients")
        
        # Get all active splits
        splits = await get_active_splits()
        logger.info(f"Found {len(splits)} active splits")
        
        # Group splits by payee
        payee_splits = {}
        for split in splits:
            payee_id = split['payee_id']
            if payee_id not in payee_splits:
                payee_splits[payee_id] = {
                    'payee_id': payee_id,
                    'payee_name': split['payee_name'],
                    'payee_email': split['payee_email'],
                    'quickbooks_id': split['quickbooks_id'],
                    'total_earnings': 0.00,
                    'client_splits': []
                }
                
            client_id = split['client_id']
            client_data = client_pulls.get(client_id)
            
            if client_data:
                # Calculate total amount for client
                total_amount = sum(pull['pull_amount'] for pull in client_data['pulls'])
                split_amount = (total_amount * split['percentage']) / 100
                
                payee_splits[payee_id]['client_splits'].append({
                    'client_id': client_id,
                    'client_email': client_data['payout_email'],
                    'total_amount': total_amount,
                    'split_percentage': split['percentage'],
                    'split_amount': split_amount,
                    'pulls': client_data['pulls']
                })
                
                payee_splits[payee_id]['total_earnings'] += split_amount
        
        # Create statement document
        statement_doc = {
            'month': month,
            'generated_at': datetime.now(timezone.utc),
            'status': 'draft',
            'payee_statements': list(payee_splits.values())
        }
        
        # Save to database
        result = await payment_statements.update_one(
            {'month': month},
            {'$set': statement_doc},
            upsert=True
        )
        
        logger.info(f"Statement saved for month {month}. "
                   f"Modified: {result.modified_count}, "
                   f"Upserted: {result.upserted_id is not None}")
        
        return statement_doc
        
    except Exception as e:
        logger.error(f"Error building monthly statement: {str(e)}")
        return None

async def main():
    """
    Command-line interface for statement generation.
    
    Provides a CLI interface for generating monthly statements,
    with argument parsing and detailed logging.
    
    Arguments:
        month: Target month in YYYY-MM format
        
    Notes:
        - Sets logging to DEBUG level
        - Validates month format
        - Reports generation status
        - Handles errors gracefully
    """
    parser = argparse.ArgumentParser(description="Build payment statements for a specific month")
    parser.add_argument("month", help="Month to build statements for (format: YYYY-MM)")
    args = parser.parse_args()
    
    # Set logging to DEBUG level for more detailed output
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    
    logger.info(f"Starting statement build for month: {args.month}")
    statement = await build_monthly_statement(args.month)
    
    if statement:
        logger.info(f"Successfully built statement for {args.month}")
        logger.info(f"Generated statements for {len(statement['payee_statements'])} payees")
    else:
        logger.error(f"Failed to build statement for {args.month}")

if __name__ == "__main__":
    asyncio.run(main()) 