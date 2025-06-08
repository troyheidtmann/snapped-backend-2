"""
Google Sheets Payment Synchronization System

This module provides functionality for synchronizing payment data between Google
Sheets and MongoDB. It handles data validation, client lookup, pull tracking,
and maintains detailed sync history with comprehensive error handling.

Features:
--------
1. Sheet Integration:
   - Google Sheets connection
   - Worksheet management
   - Data extraction
   - Format validation

2. Data Processing:
   - Email normalization
   - Client ID lookup
   - Pull record tracking
   - Amount validation

3. MongoDB Integration:
   - Upsert operations
   - Pull deduplication
   - Status tracking
   - History maintenance

4. Analytics:
   - Total calculations
   - Year-to-date tracking
   - Quarter-to-date metrics
   - Sync statistics

Data Model:
----------
Payout Structure:
- Email identification
- Client association
- Pull history
- Payment totals
- Sync metadata

Security:
--------
- Service account auth
- Email normalization
- Data validation
- Error tracking

Dependencies:
-----------
- gspread: Google Sheets API
- pandas: Data processing
- MongoDB: Data storage
- FastAPI: API endpoints
- logging: Debug tracking

Author: Snapped Development Team
"""

import gspread
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional
from app.shared.database import async_client, client_info
import logging
import os
from dotenv import load_dotenv
from fastapi import Request, HTTPException, APIRouter

router = APIRouter()

# Load environment variables
load_dotenv()

# Google Sheets configuration - hardcoded for now
GOOGLE_SHEETS_WORKBOOK = "payments"  # The name of your spreadsheet
GOOGLE_SHEETS_WORKSHEET = "filtered"   # The name of your worksheet/tab

# Set up detailed logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Get the ACTUAL CORRECT collection - Payments.Payouts (capital P)
payouts = async_client['Payments']['Payouts']

class PayoutSheetSync:
    """
    Google Sheets to MongoDB Payment Synchronization Manager.
    
    Handles the synchronization of payment data between Google Sheets
    and MongoDB, including data validation, client lookup, and
    detailed tracking of payment pulls.
    
    Attributes:
        workbook_name (str): Google Sheets workbook identifier
        worksheet_name (str): Target worksheet name
        
    Notes:
        - Uses service account authentication
        - Maintains sync history
        - Handles duplicate detection
        - Tracks sync statistics
    """
    
    def __init__(self, 
                 workbook_name: str = GOOGLE_SHEETS_WORKBOOK, 
                 worksheet_name: Optional[str] = GOOGLE_SHEETS_WORKSHEET):
        self.workbook_name = workbook_name
        self.worksheet_name = worksheet_name
        
    def connect_to_sheets(self) -> tuple[gspread.Client, gspread.Worksheet]:
        """
        Establish connection to Google Sheets and return specific worksheet.
        
        Returns:
            tuple: (gspread.Client, gspread.Worksheet)
                - Client: Authenticated gspread client
                - Worksheet: Target worksheet object
                
        Raises:
            Exception: For connection or worksheet access errors
            
        Notes:
            - Uses service account credentials
            - Validates worksheet existence
            - Falls back to first sheet if none specified
        """
        try:
            # Use gspread's built-in authentication
            gc = gspread.service_account(filename='client_secret.json')
            
            # Open the workbook
            workbook = gc.open(self.workbook_name)
            
            # Get specific worksheet
            if self.worksheet_name:
                try:
                    worksheet = workbook.worksheet(self.worksheet_name)
                except gspread.WorksheetNotFound:
                    raise Exception(f"Worksheet '{self.worksheet_name}' not found in workbook '{self.workbook_name}'")
            else:
                # Default to first sheet if no worksheet specified
                worksheet = workbook.sheet1
                
            return gc, worksheet

        except Exception as e:
            raise Exception(f"Failed to connect to Google Sheets: {str(e)}")

    async def get_client_id(self, email: str) -> Optional[str]:
        """
        Look up client ID from email in ClientInfo collection.
        
        Performs multiple matching attempts to find the correct client ID,
        including exact match, case-insensitive match, and base email match.
        
        Args:
            email (str): Client's email address
            
        Returns:
            Optional[str]: Client ID if found, None otherwise
            
        Notes:
            - Tries exact match first
            - Falls back to case-insensitive
            - Handles @snapped.cc variations
            - Logs all attempts
        """
        try:
            # Log the email we're looking up
            logger.info(f"Looking up client ID for email: {email}")
            
            # Try exact match first
            client = await client_info.find_one({"Email_Address": email})
            if client:
                client_id = client.get('client_id')
                logger.info(f"Found client ID with exact match: {client_id} for email: {email}")
                return client_id
                
            # Try case-insensitive match
            client = await client_info.find_one({"Email_Address": {"$regex": f"^{email}$", "$options": "i"}})
            if client:
                client_id = client.get('client_id')
                logger.info(f"Found client ID with case-insensitive match: {client_id} for email: {email}")
                return client_id

            # Try without @snapped.cc
            base_email = email.replace('@snapped.cc', '')
            client = await client_info.find_one({"Email_Address": {"$regex": f"^{base_email}", "$options": "i"}})
            if client:
                client_id = client.get('client_id')
                logger.info(f"Found client ID with base email match: {client_id} for email: {email}")
                return client_id

            # Log all email addresses in database for debugging
            cursor = client_info.find({"Email_Address": {"$exists": True}}, {"Email_Address": 1, "client_id": 1})
            all_emails = await cursor.to_list(length=None)
            logger.info(f"All emails in database: {[{'email': doc.get('Email_Address'), 'client_id': doc.get('client_id')} for doc in all_emails]}")
                
            logger.warning(f"No client found for email: {email}")
            return None
        except Exception as e:
            logger.error(f"Error looking up client ID for {email}: {str(e)}")
            return None

    async def process_sheet_data(self, data: List[List[str]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Process and validate sheet data before insertion.
        
        Converts raw sheet data into structured records, validating
        and normalizing data along the way.
        
        Args:
            data (List[List[str]]): Raw sheet data rows
            
        Returns:
            Dict[str, List[Dict[str, Any]]]: Processed records grouped by email
            
        Notes:
            - Validates required fields
            - Normalizes email addresses
            - Converts dates and amounts
            - Groups by email
            - Includes client IDs
        """
        # Group records by email
        email_groups = {}
        
        # Skip empty rows
        data = [row for row in data if any(row)]
        
        logger.info(f"Processing {len(data)} rows from sheet")
        
        for row in data:
            if len(row) < 3:  # Skip rows that don't have all required fields
                logger.warning(f"Skipping row with insufficient fields: {row}")
                continue
                
            # Extract data from columns (no headers)
            email = row[0].strip().lower()  # Column A
            date_str = row[1]  # Column B
            amount_str = row[2]  # Column C
            
            logger.info(f"Processing row - Email: {email}, Date: {date_str}, Amount: {amount_str}")
            
            if not email:
                logger.warning("Skipping row with empty email")
                continue
                
            # Look up client ID
            client_id = await self.get_client_id(email)
            
            # Convert date string to datetime
            try:
                pull_date = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            except ValueError as e:
                logger.error(f"Error parsing date {date_str}: {e}")
                continue
                
            # Convert amount string to float
            try:
                # Remove $ and convert to float
                amount = float(str(amount_str).replace('$', '').replace(',', ''))
            except (ValueError, TypeError) as e:
                logger.error(f"Error converting amount {amount_str}: {e}")
                continue
                
            # Initialize email group if it doesn't exist
            if email not in email_groups:
                email_groups[email] = {
                    'payout_email': email,
                    'client_id': client_id,  # Add client ID to record
                    'last_synced': datetime.utcnow(),
                    'sync_source': 'google_sheets',
                    'workbook': self.workbook_name,
                    'worksheet': self.worksheet_name or 'sheet1',
                    'creator_pulls': []
                }
            
            # Add pull record
            pull_record = {
                'pull_date': pull_date,
                'pull_amount': amount
            }
            
            email_groups[email]['creator_pulls'].append(pull_record)
            logger.info(f"Added pull record for {email}: {amount} on {pull_date}")
            
        return email_groups

    async def sync_to_mongo(self) -> Dict[str, Any]:
        """
        Sync sheet data to MongoDB.
        
        Main synchronization function that handles the complete process
        of reading from Google Sheets and updating MongoDB records.
        
        Returns:
            Dict[str, Any]: Sync results containing:
                - status: Operation status
                - sync_results: Detailed statistics
                - workbook: Source workbook
                - worksheet: Source worksheet
                - timestamp: Operation time
                
        Notes:
            - Handles duplicate detection
            - Updates totals
            - Tracks sync history
            - Maintains audit trail
        """
        try:
            # Connect to Google Sheets
            _, worksheet = self.connect_to_sheets()
            
            # Get all values (not records since we don't have headers)
            data = worksheet.get_all_values()
            
            logger.info(f"Retrieved {len(data)} rows from Google Sheet")
            
            # Process the data
            email_groups = await self.process_sheet_data(data)
            
            logger.info(f"Processed data into {len(email_groups)} email groups")
            
            sync_results = {
                'processed': 0,
                'updated': 0,
                'errors': 0,
                'missing_client_ids': 0,
                'details': []  # Add details about each sync
            }
            
            # Update MongoDB for each email group
            for email, record in email_groups.items():
                try:
                    logger.info(f"Processing email group: {email}")
                    
                    # Normalize email case and remove any whitespace
                    email = email.lower().strip()
                    
                    # Check if record already exists - try case insensitive match
                    existing_record = await payouts.find_one({
                        'payout_email': {'$regex': f'^{email}$', '$options': 'i'}
                    })
                    logger.info(f"Existing record found for {email}: {existing_record is not None}")
                    
                    # Sort pulls by date
                    record['creator_pulls'].sort(key=lambda x: x['pull_date'])
                    
                    # If record exists, only add new pulls
                    new_pulls = []
                    if existing_record and 'creator_pulls' in existing_record:
                        # Create a set of existing pull identifiers (date + amount)
                        existing_pulls = {
                            (pull['pull_date'], pull['pull_amount']) 
                            for pull in existing_record['creator_pulls']
                        }
                        
                        # Only add pulls that don't exist
                        for pull in record['creator_pulls']:
                            pull_key = (pull['pull_date'], pull['pull_amount'])
                            if pull_key not in existing_pulls:
                                new_pulls.append(pull)
                                logger.info(f"New pull found for {email}: {pull}")
                            else:
                                logger.info(f"Skipping duplicate pull for {email}: {pull}")
                    else:
                        new_pulls = record['creator_pulls']
                    
                    # Calculate totals
                    all_pulls = (existing_record.get('creator_pulls', []) if existing_record else []) + new_pulls
                    total_paid = sum(pull['pull_amount'] for pull in all_pulls)
                    logger.info(f"Calculated total_paid for {email}: {total_paid}")
                    
                    # Get current year and quarter
                    now = datetime.utcnow()
                    current_year = now.year
                    current_quarter = (now.month - 1) // 3 + 1
                    
                    # Calculate year and quarter totals
                    year_to_date = sum(
                        pull['pull_amount'] 
                        for pull in all_pulls 
                        if pull['pull_date'].year == current_year
                    )
                    
                    quarter_to_date = sum(
                        pull['pull_amount'] 
                        for pull in all_pulls 
                        if pull['pull_date'].year == current_year 
                        and ((pull['pull_date'].month - 1) // 3 + 1) == current_quarter
                    )
                    
                    # Update totals in record
                    record['total_paid_to_date'] = total_paid
                    record['total_paid_year_to_date'] = year_to_date
                    record['total_paid_quarter_to_date'] = quarter_to_date

                    if not record.get('client_id'):
                        sync_results['missing_client_ids'] += 1
                        logger.warning(f"No client ID found for email: {email}")
                    
                    # Only update if we have new pulls or the record doesn't exist
                    if new_pulls or not existing_record:
                        # Update or insert record using payouts - use case insensitive match
                        update_doc = {
                            '$set': {
                                'last_synced': datetime.utcnow(),
                                'sync_source': 'google_sheets',
                                'workbook': self.workbook_name,
                                'worksheet': self.worksheet_name or 'sheet1',
                                'total_paid_to_date': total_paid,
                                'total_paid_year_to_date': year_to_date,
                                'total_paid_quarter_to_date': quarter_to_date
                            },
                            '$setOnInsert': {
                                'payout_email': email,
                                'client_id': record['client_id']
                            },
                            '$currentDate': {'last_updated': True}
                        }
                        
                        # Only add new pulls if we have any
                        if new_pulls:
                            update_doc['$push'] = {
                                'creator_pulls': {
                                    '$each': new_pulls
                                }
                            }
                        
                        result = await payouts.update_one(
                            {'payout_email': {'$regex': f'^{email}$', '$options': 'i'}},
                            update_doc,
                            upsert=True
                        )
                        
                        logger.info(f"Update result for {email}: modified={result.modified_count}, upserted={result.upserted_id is not None}")
                        
                        # Verify the update
                        updated_record = await payouts.find_one({'payout_email': email})
                        logger.info(f"Verification - Record after update for {email}: {updated_record is not None}")
                        if updated_record:
                            logger.info(f"Updated record details: client_id={updated_record.get('client_id')}, total_paid={updated_record.get('total_paid_to_date')}")
                        
                        sync_detail = {
                            'email': email,
                            'client_id': record.get('client_id'),
                            'total_paid': total_paid,
                            'new_pulls_count': len(new_pulls),
                            'total_pulls_count': len(all_pulls),
                            'status': 'updated' if result.modified_count else 'created' if result.upserted_id else 'unchanged'
                        }
                        sync_results['details'].append(sync_detail)
                        
                        if result.modified_count > 0 or result.upserted_id:
                            sync_results['updated'] += 1
                            logger.info(f"Successfully updated/inserted record for {email}")
                    else:
                        logger.info(f"No new pulls for {email}, skipping update")
                        sync_detail = {
                            'email': email,
                            'client_id': record.get('client_id'),
                            'total_paid': total_paid,
                            'new_pulls_count': 0,
                            'total_pulls_count': len(all_pulls),
                            'status': 'no_changes'
                        }
                        sync_results['details'].append(sync_detail)
                    
                    sync_results['processed'] += 1
                    
                except Exception as e:
                    logger.error(f"Error processing email {email}: {str(e)}")
                    sync_results['errors'] += 1
                    sync_results['details'].append({
                        'email': email,
                        'status': 'error',
                        'error': str(e)
                    })
            
            return {
                'status': 'success',
                'sync_results': sync_results,
                'workbook': self.workbook_name,
                'worksheet': self.worksheet_name or 'sheet1',
                'timestamp': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Sync failed: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'workbook': self.workbook_name,
                'worksheet': self.worksheet_name or 'sheet1',
                'timestamp': datetime.utcnow()
            }

    async def get_sync_status(self) -> Dict[str, Any]:
        """
        Get the current sync status.
        
        Retrieves synchronization statistics and status information
        for the current workbook and worksheet.
        
        Returns:
            Dict[str, Any]: Status information containing:
                - total_records: Count of synced records
                - last_sync: Latest sync timestamp
                - workbook: Source workbook
                - worksheet: Source worksheet
                - status: Current sync status
                
        Notes:
            - Counts total records
            - Finds latest sync
            - Reports sync status
            - Includes source info
        """
        try:
            total_records = await payouts.count_documents({
                'sync_source': 'google_sheets',
                'workbook': self.workbook_name,
                'worksheet': self.worksheet_name or 'sheet1'
            })
            
            latest_sync = await payouts.find_one(
                {
                    'sync_source': 'google_sheets',
                    'workbook': self.workbook_name,
                    'worksheet': self.worksheet_name or 'sheet1'
                },
                sort=[('last_synced', -1)]
            )
            
            return {
                'total_records': total_records,
                'last_sync': latest_sync['last_synced'] if latest_sync else None,
                'workbook': self.workbook_name,
                'worksheet': self.worksheet_name or 'sheet1',
                'status': 'active' if total_records > 0 else 'no_records'
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e),
                'workbook': self.workbook_name,
                'worksheet': self.worksheet_name or 'sheet1'
            }

@router.post("/sync-payout-email")
async def sync_payout_email(request: Request):
    """
    API endpoint to sync payout email with client ID.
    
    Updates or creates a payout record with the specified
    client ID and email association.
    
    Args:
        request (Request): FastAPI request object containing:
            - client_id: Client identifier
            - payout_email: Email address
            
    Returns:
        Dict[str, Any]: Operation result containing:
            - status: Success indicator
            - message: Status description
            - updated: Change indicator
            
    Raises:
        HTTPException: For validation or processing errors
        
    Notes:
        - Normalizes email
        - Uses case-insensitive matching
        - Verifies updates
        - Logs operations
    """
    try:
        data = await request.json()
        client_id = data.get('client_id')
        payout_email = data.get('payout_email')
        
        if not client_id or not payout_email:
            raise HTTPException(status_code=400, detail="Both client_id and payout_email are required")
        
        # Normalize email
        payout_email = payout_email.lower().strip()
        logger.info(f"Attempting to update client_id {client_id} for email {payout_email}")
        
        # Use case-insensitive regex match like vista_service.py
        result = await payouts.update_one(
            {
                "payout_email": {
                    "$regex": f"^{payout_email}$",
                    "$options": "i"
                }
            },
            {
                "$set": {
                    "client_id": client_id,
                    "last_updated": datetime.utcnow()
                }
            },
            upsert=True
        )
        
        # Verify the update
        updated_record = await payouts.find_one({"payout_email": {"$regex": f"^{payout_email}$", "$options": "i"}})
        logger.info(f"Verification - Updated record: {updated_record}")
        
        return {
            "status": "success",
            "message": "Client ID updated",
            "updated": result.modified_count > 0 or result.upserted_id is not None
        }
        
    except Exception as e:
        logger.error(f"Error updating client_id: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))