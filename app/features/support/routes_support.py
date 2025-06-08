"""
Support Request Management Module

This module handles customer support request processing and email notifications
for the BlackMatter Support Portal.

Features:
- Support request submission
- Email notification system
- Async email processing
- Error handling and logging

Data Model:
- Support request structure
- Email templates
- Error tracking
- Request status

Security:
- SMTP authentication
- Email validation
- Error handling
- Request validation

Dependencies:
- FastAPI for routing
- smtplib for email
- Pydantic for validation
- logging for debug tracking

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from ...shared.config import (
    SMTP_SERVER,
    SMTP_PORT,
    SMTP_USERNAME,
    SMTP_PASSWORD,
    FROM_EMAIL
)
import asyncio

router = APIRouter()
logger = logging.getLogger(__name__)

class SupportRequest(BaseModel):
    """
    Support request data model.
    
    Validates and structures incoming support request data.
    
    Attributes:
        name (str): Requester's name
        email (str): Requester's email address
        subject (str): Support request subject
        message (str): Support request message body
    """
    name: str
    email: str
    subject: str
    message: str

async def send_support_email(request: SupportRequest):
    """
    Send support request email asynchronously.
    
    Args:
        request (SupportRequest): Validated support request data
        
    Returns:
        bool: True if email sent successfully, False otherwise
        
    Notes:
        - Uses SMTP for email delivery
        - Formats message with request details
        - Handles errors asynchronously
        - Logs email status
    """
    try:
        msg = MIMEMultipart()
        msg['From'] = FROM_EMAIL
        msg['To'] = FROM_EMAIL  # Send to support email
        msg['Subject'] = f"BlackMatter Support Request: {request.subject}"

        body = f"""
        New BlackMatter Support Request
        =============================

        From: {request.name}
        Email: {request.email}
        Subject: {request.subject}

        Message:
        {request.message}

        =============================
        This message was sent from the BlackMatter Support Portal
        """

        msg.attach(MIMEText(body, 'plain'))

        # Send email using asyncio to prevent blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: send_email_sync(msg))
        
        return True
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        return False

def send_email_sync(msg):
    """
    Synchronous email sending function.
    
    Args:
        msg (MIMEMultipart): Formatted email message
        
    Raises:
        Exception: If SMTP connection or sending fails
        
    Notes:
        - Uses TLS for security
        - Authenticates with SMTP server
        - Logs SMTP errors
    """
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
    except Exception as e:
        logger.error(f"SMTP Error: {str(e)}")
        raise

@router.post("/api/support/submit")
async def submit_support_request(request: SupportRequest):
    """
    Handle support request submission and email notification.
    
    Args:
        request (SupportRequest): Validated support request data
        
    Returns:
        dict: Success status and message
        
    Raises:
        HTTPException: If request submission fails
        
    Notes:
        - Validates request data
        - Sends email notification
        - Logs submission status
        - Handles errors gracefully
    """
    try:
        email_sent = await send_support_email(request)
        if not email_sent:
            raise Exception("Failed to send support email")

        return {"status": "success", "message": "Support request submitted successfully"}
    except Exception as e:
        logger.error(f"Error submitting support request: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to submit support request")
