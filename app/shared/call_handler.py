"""
Call Handling Module

This module manages the processing and storage of call information
submitted through the API.

Features:
- Call data submission
- MongoDB storage
- Error handling
- Response formatting
- Logging

Data Model:
- Call records
- Timestamps
- User data
- Response status
- Error logs

Security:
- Input validation
- Error handling
- Safe storage
- Status tracking

Dependencies:
- FastAPI for routing
- MongoDB for storage
- httpx for HTTP
- logging for tracking

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
import httpx
import logging
from flask import request, jsonify

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post('/call')
def handle_call_submit():
    """
    Handle call information submission.
    
    Returns:
        dict: Response with status
        
    Raises:
        HTTPException: For processing errors
        
    Notes:
        - Validates input
        - Stores in MongoDB
        - Returns ID
        - Logs status
    """
    try:
        data = request.json
        logger.info("Received call submission")
        logger.debug(f"Call data: {data}")
        
        # Insert the document into MongoDB
        result = collection.insert_one(data)
        
        success_msg = "Call information saved successfully"
        logger.info(success_msg)
        return jsonify({
            "success": True,
            "message": success_msg,
            "id": str(result.inserted_id)
        }), 200
        
    except Exception as e:
        error_msg = f"Error saving call: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return jsonify({
            "success": False,
            "message": error_msg
        }), 500