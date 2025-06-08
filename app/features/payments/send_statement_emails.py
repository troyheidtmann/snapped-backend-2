"""
Payment Statement Email Distribution System

This module handles the automated distribution of payment statements to payees
via email. It provides functionality for formatting HTML emails with payment
details, managing SMTP connections, and tracking email delivery status.

Features:
--------
1. Email Management:
   - HTML email formatting
   - SMTP connection handling
   - Async email sending
   - Delivery tracking

2. Statement Processing:
   - Payment details formatting
   - Client split calculations
   - Pull history inclusion
   - Batch processing

3. Error Handling:
   - SMTP error management
   - Database validation
   - Logging system
   - Status tracking

4. Test Mode:
   - Dry run capability
   - Content verification
   - No actual sending
   - Result simulation

Data Model:
----------
Statement Structure:
- Statement metadata
- Payee information
- Payment details
- Client splits
- Email status

Security:
--------
- SMTP authentication
- TLS encryption
- Environment variables
- Error logging

Dependencies:
-----------
- smtplib: SMTP client
- email.mime: Email formatting
- asyncio: Async operations
- MongoDB: Data storage
- dotenv: Configuration
- logging: Debug tracking

Author: Snapped Development Team
"""

import asyncio
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from typing import Dict, List, Any, Optional
from bson import ObjectId
import logging
import os
import argparse
from dotenv import load_dotenv
import sys
from pathlib import Path

# Add the root directory to the path so we can import app modules
root_dir = str(Path(__file__).parent.parent.parent)
sys.path.append(root_dir)

# Load environment variables
load_dotenv()

# Import database connection
from app.shared.database import async_client, client_info

# Import email configuration from shared config
from app.shared.config import (
    SMTP_SERVER,
    SMTP_PORT,
    SMTP_USERNAME,
    SMTP_PASSWORD,
    FROM_EMAIL
)

# Get collections
payment_statements = async_client["Payments"]["Statements"]

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Company details for the email
COMPANY_NAME = os.getenv("COMPANY_NAME", "Snapped")
COMPANY_ADDRESS = os.getenv("COMPANY_ADDRESS", "Snapped, Inc.")
COMPANY_WEBSITE = os.getenv("COMPANY_WEBSITE", "https://snapped.cc")
EMAIL_REPLY_TO = os.getenv("EMAIL_REPLY_TO", "accounting@snapped.cc")

async def format_statement_email(payee_statement: Dict[str, Any], statement_date: str) -> str:
    """
    Format payment statement data into HTML email content.
    
    Creates a professionally formatted HTML email containing payment details,
    client splits, and pull history.
    
    Args:
        payee_statement (Dict[str, Any]): Complete payee statement data
        statement_date (str): Statement period in YYYY-MM format
        
    Returns:
        str: Formatted HTML email content
        
    Raises:
        Exception: For formatting errors
        
    Notes:
        - Includes company branding
        - Shows payment summary
        - Details client splits
        - Lists pull history
        - Adds footer with contact info
    """
    try:
        # Extract payee information
        payee_name = payee_statement.get("payee_name", "Payee")
        payee_email = payee_statement.get("payee_email", "")
        total_earnings = payee_statement.get("total_earnings", 0)
        
        # Format the date for display (convert from "2025-01" to "January 2025")
        display_date = None
        try:
            date_parts = statement_date.split("-")
            year = date_parts[0]
            month = datetime.strptime(date_parts[1], "%m").strftime("%B")
            display_date = f"{month} {year}"
        except Exception:
            display_date = statement_date
        
        # Start building the HTML email
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 800px; margin: 0 auto; }}
                .header {{ background-color: #0066cc; color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 20px; }}
                .footer {{ background-color: #f5f5f5; padding: 15px; text-align: center; font-size: 0.8em; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .total-row {{ font-weight: bold; background-color: #f9f9f9; }}
                .note {{ font-style: italic; color: #666; margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>{COMPANY_NAME} Payment Statement</h1>
                <h2>{display_date}</h2>
            </div>
            <div class="content">
                <p>Dear {payee_name},</p>
                
                <p>Please find below your payment statement for {display_date}.</p>
                
                <h3>Payment Summary</h3>
                <table>
                    <tr>
                        <th>Payee</th>
                        <th>Email</th>
                        <th>Total Earnings</th>
                    </tr>
                    <tr>
                        <td>{payee_name}</td>
                        <td>{payee_email}</td>
                        <td>${total_earnings:.2f}</td>
                    </tr>
                </table>
        """
        
        # Add client splits if available
        client_splits = payee_statement.get("client_splits", [])
        if client_splits:
            html += f"""
                <h3>Earnings Breakdown</h3>
                <table>
                    <tr>
                        <th>Client</th>
                        <th>Client Total</th>
                        <th>Split %</th>
                        <th>Your Share</th>
                    </tr>
            """
            
            for split in client_splits:
                client_email = split.get("client_email", "Unknown")
                client_id = split.get("client_id", "")
                
                # Look up client name from database
                client_name = "Unknown Client"
                if client_id:
                    client_data = await client_info.find_one({"client_id": client_id})
                    if client_data:
                        first_name = client_data.get("First_Legal_Name", "")
                        last_name = client_data.get("Last_Legal_Name", "")
                        if first_name and last_name:
                            client_name = f"{first_name} {last_name}"
                
                total_amount = split.get("total_amount", 0)
                split_percentage = split.get("split_percentage", 0)
                split_amount = split.get("split_amount", 0)
                
                html += f"""
                    <tr>
                        <td>{client_name}</td>
                        <td>${total_amount:.2f}</td>
                        <td>{split_percentage}%</td>
                        <td>${split_amount:.2f}</td>
                    </tr>
                """
                
                # Add pull details if available
                pulls = split.get("pulls", [])
                if pulls:
                    html += f"""
                    <tr>
                        <td colspan="4" style="border-top: none; padding-left: 20px;">
                            <strong>Pull Details:</strong><br>
                    """
                    
                    for pull in pulls:
                        pull_date = pull.get("pull_date")
                        if pull_date and not isinstance(pull_date, str):
                            pull_date = pull_date.strftime("%Y-%m-%d")
                        pull_amount = pull.get("pull_amount", 0)
                        
                        html += f"&nbsp;&nbsp;${pull_amount:.2f} ({pull_date})<br>"
                    
                    html += """
                        </td>
                    </tr>
                    """
            
            # Add total row
            html += f"""
                <tr class="total-row">
                    <td colspan="3">Total</td>
                    <td>${total_earnings:.2f}</td>
                </tr>
            </table>
            """
        else:
            html += """
                <p class="note">No client splits available for this period.</p>
            """
        
        # Add footer and close HTML
        html += f"""
                <p class="note">Payment will be processed according to our payment terms. If you have any questions regarding this statement, please reply to this email or contact {EMAIL_REPLY_TO}.</p>
            </div>
            <div class="footer">
                <p>{COMPANY_NAME} | {COMPANY_ADDRESS} | <a href="{COMPANY_WEBSITE}">{COMPANY_WEBSITE}</a></p>
                <p>This is an automated email. Please do not reply directly to this message.</p>
            </div>
        </body>
        </html>
        """
        
        return html
    except Exception as e:
        logger.error(f"Error formatting statement email: {str(e)}")
        raise

def send_email_sync(msg):
    """
    Synchronous function to send email via SMTP.
    
    Establishes SMTP connection, handles authentication, and
    sends the email message.
    
    Args:
        msg: Email message object (MIMEMultipart)
        
    Returns:
        bool: True if sent successfully, False otherwise
        
    Notes:
        - Uses TLS encryption
        - Handles SMTP authentication
        - Logs connection steps
        - Auto-closes connection
    """
    try:
        logger.info("Connecting to SMTP server...")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            logger.info("Starting TLS...")
            server.starttls()
            logger.info(f"Logging in as {SMTP_USERNAME}")
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            logger.info("Sending message...")
            server.send_message(msg)
            logger.info("Message sent!")
            return True
    except Exception as e:
        logger.error(f"SMTP Error: {str(e)}")
        return False

async def send_email(to_email: str, subject: str, html_content: str) -> bool:
    """
    Send HTML email asynchronously.
    
    Creates email message with HTML content and sends it
    using async executor to prevent blocking.
    
    Args:
        to_email (str): Recipient email address
        subject (str): Email subject line
        html_content (str): HTML-formatted email body
        
    Returns:
        bool: True if sent successfully, False otherwise
        
    Notes:
        - Uses MIMEMultipart for HTML
        - Sets reply-to address
        - Runs SMTP in executor
        - Logs send status
    """
    try:
        logger.info(f"Preparing email to {to_email}")
        
        # Create message
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = FROM_EMAIL
        msg["To"] = to_email
        msg["Reply-To"] = EMAIL_REPLY_TO
        
        # Add HTML content
        html_part = MIMEText(html_content, "html")
        msg.attach(html_part)
        
        # Send email - use asyncio to prevent blocking
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: send_email_sync(msg))
        
        if result:
            logger.info(f"Email sent successfully to {to_email}")
            return True
        else:
            logger.error(f"Failed to send email to {to_email}")
            return False
            
    except Exception as e:
        logger.error(f"Error sending email to {to_email}: {str(e)}")
        return False

async def send_statement_emails(statement_id: str, test_mode: bool = False) -> Dict[str, Any]:
    """
    Send payment statement emails to all payees.
    
    Processes a payment statement and sends formatted emails
    to each payee with their payment details.
    
    Args:
        statement_id (str): MongoDB ObjectId of statement
        test_mode (bool): If True, simulates sending without actual delivery
        
    Returns:
        Dict[str, Any]: Operation results containing:
            - status: Completion status
            - statement_id: Processed statement
            - results: List of send attempts
            - test_mode: Mode indicator
            
    Raises:
        ValueError: For invalid statement ID
        Exception: For processing errors
        
    Notes:
        - Skips payees with no earnings
        - Validates email addresses
        - Updates statement status
        - Tracks delivery results
        - Supports test mode
    """
    try:
        # Get statement from database
        statement = await payment_statements.find_one({"_id": ObjectId(statement_id)})
        if not statement:
            raise ValueError(f"Statement not found: {statement_id}")

        statement_month = statement.get("month", "")
        logger.info(f"Processing statement emails for {statement_month}")
        
        results = []
        for payee_statement in statement.get("payee_statements", []):
            try:
                # Skip if no earnings
                if not payee_statement.get("total_earnings"):
                    logger.info(f"Skipping payee {payee_statement.get('payee_name')} - no earnings")
                    continue

                payee_name = payee_statement.get("payee_name", "Payee")
                payee_email = payee_statement.get("payee_email")
                
                # Skip if no email
                if not payee_email:
                    logger.warning(f"Skipping payee {payee_name} - no email address")
                    results.append({
                        "status": "skipped",
                        "message": "No email address available",
                        "payee_name": payee_name,
                        "payee_id": payee_statement.get("payee_id")
                    })
                    continue
                    
                # Format email content
                html_content = await format_statement_email(
                    payee_statement,
                    statement.get("month", "")
                )
                
                # Set up email subject
                subject = f"{COMPANY_NAME} Payment Statement - {statement_month}"
                
                if test_mode:
                    # In test mode, just log the email content
                    logger.info(f"TEST MODE: Would send email to {payee_name} <{payee_email}>")
                    logger.info(f"Subject: {subject}")
                    logger.info(f"Content (truncated): {html_content[:100]}...")
                    
                    results.append({
                        "status": "test_success",
                        "message": "Email prepared but not sent (test mode)",
                        "payee_name": payee_name,
                        "payee_email": payee_email,
                        "payee_id": payee_statement.get("payee_id")
                    })
                else:
                    # Send the actual email
                    success = await send_email(
                        payee_email,
                        subject,
                        html_content
                    )
                    
                    if success:
                        results.append({
                            "status": "success",
                            "message": "Email sent successfully",
                            "payee_name": payee_name,
                            "payee_email": payee_email,
                            "payee_id": payee_statement.get("payee_id")
                        })
                    else:
                        results.append({
                            "status": "failed",
                            "message": "Failed to send email",
                            "payee_name": payee_name,
                            "payee_email": payee_email,
                            "payee_id": payee_statement.get("payee_id")
                        })

            except Exception as e:
                logger.error(f"Error processing payee {payee_statement.get('payee_name')}: {str(e)}")
                results.append({
                    "status": "failed",
                    "message": str(e),
                    "payee_name": payee_statement.get("payee_name"),
                    "payee_id": payee_statement.get("payee_id")
                })

        # Update statement with email results
        await payment_statements.update_one(
            {"_id": ObjectId(statement_id)},
            {
                "$set": {
                    "email_status": "sent" if not test_mode else "test",
                    "email_results": results,
                    "last_email_attempt": datetime.utcnow()
                }
            }
        )

        return {
            "status": "completed",
            "statement_id": statement_id,
            "results": results,
            "test_mode": test_mode
        }

    except Exception as e:
        logger.error(f"Error sending statement emails: {str(e)}")
        raise

async def main():
    """
    Main execution function for statement email distribution.
    
    Parses command line arguments and initiates the email
    sending process for a specified month.
    
    Args:
        None (uses argparse)
        
    Returns:
        None
        
    Notes:
        - Requires month argument (YYYY-MM)
        - Supports --test flag
        - Validates statement existence
        - Handles process errors
    """
    parser = argparse.ArgumentParser(description="Send payment statement emails")
    parser.add_argument("month", help="Month to send emails for (format: YYYY-MM)")
    parser.add_argument("--test", action="store_true", help="Run in test mode without sending actual emails")
    args = parser.parse_args()
    
    try:
        logger.info(f"Finding statement for month {args.month}")
        # Find statement by month
        statement = await payment_statements.find_one({"month": args.month})
        if not statement:
            logger.error(f"No statement found for month {args.month}")
            return
            
        statement_id = str(statement["_id"])
        logger.info(f"Found statement {statement_id} for {args.month}")
        
        logger.info(f"Starting email process for statement {statement_id} (test mode: {args.test})")
        result = await send_statement_emails(statement_id, args.test)
        logger.info(f"Email process completed: {result}")
    except Exception as e:
        logger.error(f"Error in send_statement_emails script: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())