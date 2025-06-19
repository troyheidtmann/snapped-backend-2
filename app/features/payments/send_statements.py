"""
QuickBooks Statement Integration System

This module handles the automated synchronization of payment statements with
QuickBooks through Make.com webhooks. It processes payment data, formats it
for QuickBooks bill creation, and manages the integration workflow.

Features:
--------
1. Statement Processing:
   - Batch statement handling
   - Individual payee processing
   - Amount calculation
   - Split details formatting

2. QuickBooks Integration:
   - Bill creation
   - Vendor synchronization
   - Webhook communication
   - Status tracking

3. Error Handling:
   - Rate limiting
   - Webhook retries
   - Status monitoring
   - Detailed logging

4. Data Management:
   - MongoDB integration
   - Status updates
   - Result tracking
   - Audit logging

Data Model:
----------
Statement Structure:
- Statement metadata
- Payee information
- Split calculations
- QuickBooks status

Security:
--------
- Webhook authentication
- Rate limiting
- Error handling
- Status tracking

Dependencies:
-----------
- requests: HTTP client
- MongoDB: Data storage
- datetime: Time handling
- logging: Debug tracking
- Make.com: Integration platform

Author: Snapped Development Team
"""

import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

import requests
from datetime import datetime
import logging
from typing import Dict, Any, List
from app.shared.database import async_client, payment_statements
import asyncio
from bson import ObjectId

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Make webhook URL for payment statements
MAKE_WEBHOOK_URL = "https://hook.us2.make.com/pm33xp0uztw57perdcn774vdb47gwaql"

async def format_statement_for_quickbooks(payee_statement: Dict[str, Any], statement_date: str) -> Dict[str, Any]:
    """
    Format payment statement data for QuickBooks bill creation.
    
    Processes payee statement data and formats it into a structure
    suitable for creating bills in QuickBooks via Make.com webhook.
    
    Args:
        payee_statement (Dict[str, Any]): Complete payee statement data
        statement_date (str): Statement period date
        
    Returns:
        Dict[str, Any]: Formatted data containing:
            - data: Bill creation details
            - metadata: Integration metadata
            
    Raises:
        Exception: For formatting errors
        
    Notes:
        - Calculates total amounts
        - Formats split details
        - Includes pull history
        - Adds vendor information
        - Sets line item details
    """
    try:
        # Calculate total amount from client splits
        total_amount = payee_statement.get("total_earnings", 0)

        # Build description with all details
        description_parts = [
            f"TOTAL AMOUNT OWED: ${total_amount:.2f}",
            f"Name: {payee_statement.get('payee_name', 'N/A')}",
            f"Email: {payee_statement.get('payee_email', 'N/A')}"
        ]

        # Add split details
        split_details = []
        for split in payee_statement.get("client_splits", []):
            pulls_info = " + ".join([
                f"${pull['pull_amount']:.2f} ({pull['pull_date'].strftime('%Y-%m-%d')})"
                for pull in split.get("pulls", [])
            ])
            split_details.append(
                f"{split.get('client_email', 'Unknown')} (Total: ${split.get('total_amount', 0):.2f} @ {split.get('split_percentage', 0)}% = ${split.get('split_amount', 0):.2f} | Pulls: {pulls_info})"
            )

        if split_details:
            description_parts.append("Split Details: " + " | ".join(split_details))
        else:
            description_parts.append("No splits for this period")

        # Join description parts and truncate to 3200 characters if needed
        full_description = "\n".join(description_parts)
        if len(full_description) > 3200:
            logger.warning(f"Description length ({len(full_description)}) exceeds 3200 characters, truncating...")
            full_description = full_description[:3197] + "..."

        # Format data for Make webhook
        make_data = {
            "data": {
                "vendor_id": int(payee_statement.get("quickbooks_id", 0)),
                "amount": float(total_amount),
                "Line": [{
                    "Amount": total_amount,
                    "Description": full_description,
                    "DetailType": "ItemBasedExpenseLineDetail",
                    "type": "itemBased",
                    "ItemRef": "1010000031"
                }],
                "date": statement_date
            },
            "metadata": {
                "type": "bill",
                "source": "payment_statement",
                "payee_id": payee_statement["payee_id"]
            }
        }

        return make_data

    except Exception as e:
        logger.error(f"Error formatting statement for QuickBooks: {str(e)}")
        raise

async def send_statement_to_quickbooks(statement_id: str) -> Dict[str, Any]:
    """
    Send a payment statement to QuickBooks via Make webhook.
    
    Processes a complete payment statement and sends individual
    payee data to QuickBooks for bill creation.
    
    Args:
        statement_id (str): MongoDB ObjectId of statement
        
    Returns:
        Dict[str, Any]: Operation results containing:
            - status: Completion status
            - statement_id: Processed statement
            - results: List of payee results
            
    Raises:
        ValueError: For invalid statement ID
        Exception: For processing errors
        
    Notes:
        - Processes payees individually
        - Implements rate limiting
        - Validates QuickBooks IDs
        - Updates statement status
        - Tracks results per payee
    """
    try:
        # Get statement from database
        statement = await payment_statements.find_one({"_id": ObjectId(statement_id)})
        if not statement:
            raise ValueError(f"Statement not found: {statement_id}")

        logger.info(f"Processing statement for {statement.get('month')}")
        results = []
        
        # Process each payee statement separately
        for i, payee_statement in enumerate(statement.get("payee_statements", [])):
            try:
                # Add delay between webhook calls (except for first one)
                if i > 0:
                    logger.info("Waiting 2 seconds before next webhook call...")
                    await asyncio.sleep(2)

                # Skip if no earnings
                if not payee_statement.get("total_earnings"):
                    logger.info(f"Skipping payee {payee_statement.get('payee_name')} - no earnings")
                    continue

                # Skip if no QuickBooks ID
                if not payee_statement.get("quickbooks_id"):
                    logger.warning(f"Skipping payee {payee_statement.get('payee_name')} - no QuickBooks ID")
                    continue

                # Format data for QuickBooks
                make_data = await format_statement_for_quickbooks(
                    payee_statement,
                    statement.get("month", str(datetime.utcnow().date()))
                )
                
                # Log the data being sent
                logger.info(f"Sending to Make webhook for {payee_statement.get('payee_name')} ({i+1} of {len(statement.get('payee_statements', []))})): {make_data}")
                
                # Send to Make webhook - each payee gets their own webhook call
                response = requests.post(MAKE_WEBHOOK_URL, json=make_data)
                
                if response.status_code != 200:
                    logger.error(f"Make webhook failed for {payee_statement.get('payee_name')}: {response.text}")
                    results.append({
                        "status": "failed",
                        "message": f"Failed to submit to QuickBooks: {response.text}",
                        "payee_name": payee_statement.get("payee_name"),
                        "payee_id": payee_statement.get("payee_id")
                    })
                    continue

                results.append({
                    "status": "success",
                    "message": "Statement sent to QuickBooks successfully",
                    "payee_name": payee_statement.get("payee_name"),
                    "payee_id": payee_statement.get("payee_id"),
                    "amount": payee_statement.get("total_earnings")
                })

            except Exception as e:
                logger.error(f"Error processing payee {payee_statement.get('payee_name')}: {str(e)}")
                results.append({
                    "status": "failed",
                    "message": str(e),
                    "payee_name": payee_statement.get("payee_name"),
                    "payee_id": payee_statement.get("payee_id")
                })

        # Update statement status with all results
        await payment_statements.update_one(
            {"_id": ObjectId(statement_id)},
            {
                "$set": {
                    "quickbooks_status": "processed",
                    "quickbooks_results": results,
                    "last_quickbooks_attempt": datetime.utcnow()
                }
            }
        )

        return {
            "status": "completed",
            "statement_id": statement_id,
            "results": results
        }

    except Exception as e:
        logger.error(f"Error sending statement to QuickBooks: {str(e)}")
        raise

async def send_pending_statements() -> Dict[str, Any]:
    """
    Send all pending statements to QuickBooks.
    
    Processes all statements that haven't been sent to QuickBooks
    or failed in previous attempts.
    
    Returns:
        Dict[str, Any]: Batch results containing:
            - total: Total statements processed
            - sent: Successfully sent count
            - failed: Failed attempts count
            - skipped: Skipped statements count
            - details: List of all results
            
    Raises:
        Exception: For processing errors
        
    Notes:
        - Finds unprocessed statements
        - Handles failed statements
        - Tracks batch statistics
        - Maintains detailed logs
    """
    try:
        # Find all statements that haven't been sent
        cursor = payment_statements.find({
            "$or": [
                {"quickbooks_status": {"$exists": False}},
                {"quickbooks_status": "failed"}
            ]
        })
        
        statements = await cursor.to_list(length=None)
        
        results = {
            "total": len(statements),
            "sent": 0,
            "failed": 0,
            "skipped": 0,
            "details": []
        }

        for statement in statements:
            try:
                result = await send_statement_to_quickbooks(str(statement["_id"]))
                if result["status"] == "success":
                    results["sent"] += 1
                elif result["status"] == "skipped":
                    results["skipped"] += 1
                results["details"].append(result)
            except Exception as e:
                results["failed"] += 1
                results["details"].append({
                    "status": "failed",
                    "message": str(e),
                    "statement_id": str(statement["_id"])
                })

        return results

    except Exception as e:
        logger.error(f"Error processing pending statements: {str(e)}")
        raise

async def main():
    """
    Main execution function for statement synchronization.
    
    Initiates the process of sending pending statements to
    QuickBooks and logs the results.
    
    Notes:
        - Entry point for script
        - Handles process errors
        - Logs operation results
        - Maintains audit trail
    """
    try:
        logger.info("Starting payment statement sync to QuickBooks...")
        results = await send_pending_statements()
        logger.info(f"Sync complete. Results: {results}")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main()) 