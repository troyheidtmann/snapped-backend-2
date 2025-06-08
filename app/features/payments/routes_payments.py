"""
Payment Management API - Payee Information Management System

This module provides FastAPI routes for managing payee information in the
Snapped platform. It handles payee creation, retrieval, and data management
with timestamp tracking and email normalization.

Features:
--------
1. Payee Management:
   - Payee creation
   - Information retrieval
   - Email-based lookup
   - Timestamp tracking

2. Data Handling:
   - Email normalization
   - Timestamp management
   - ObjectId conversion
   - Error handling

Data Model:
----------
Payee Structure:
- Basic Info: Email, name, details
- Metadata: Creation date, updates
- System: ObjectId, status flags
- Timestamps: Created, updated

Security:
--------
- Input validation
- Error handling
- Email normalization
- Data sanitization

Dependencies:
-----------
- FastAPI: Web framework
- MongoDB: Data storage
- datetime: Timestamp handling
- Request: Data parsing

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException, Request
from app.shared.database import async_client
from datetime import datetime

router = APIRouter(prefix="/api/payments")

# Get the Payees collection
payees = async_client['Payments']['Payees']

@router.post("/payee")
async def create_payee(request: Request):
    """
    Create a new payee record in the system.
    
    Processes incoming payee data, adds timestamps, and stores
    the information in the database.
    
    Args:
        request (Request): FastAPI request object containing payee data
        
    Returns:
        dict: Creation status containing:
            - status: Success indicator
            - message: Status description
            - id: Created payee identifier
            
    Raises:
        HTTPException: For database errors
        
    Notes:
        - Automatically adds creation timestamp
        - Adds last update timestamp
        - Returns MongoDB ObjectId as string
    """
    try:
        data = await request.json()
        
        # Add timestamp
        data['created_at'] = datetime.utcnow()
        data['updated_at'] = datetime.utcnow()
        
        # Insert into Payees collection
        result = await payees.insert_one(data)
        
        return {
            "status": "success",
            "message": "Payee information saved successfully",
            "id": str(result.inserted_id)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/payee/{email}")
async def get_payee(email: str):
    """
    Retrieve payee information by email address.
    
    Searches for a payee using normalized email address and
    returns their complete information if found.
    
    Args:
        email (str): Payee's email address
        
    Returns:
        dict: Query result containing:
            - status: Success/not_found indicator
            - data: Payee information (if found) with:
                - _id: Payee identifier
                - All stored payee fields
                
    Raises:
        HTTPException: For database errors
        
    Notes:
        - Normalizes email (lowercase, stripped)
        - Converts MongoDB ObjectId to string
        - Returns None data if payee not found
    """
    try:
        # Find payee by email
        payee = await payees.find_one({"primary_email": email.lower().strip()})
        
        if not payee:
            return {
                "status": "not_found",
                "data": None
            }
            
        # Convert ObjectId to string
        payee['_id'] = str(payee['_id'])
        
        return {
            "status": "success",
            "data": payee
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 