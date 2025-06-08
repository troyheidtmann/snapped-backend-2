"""
Call Form API Routes Module

This module implements the API endpoints for handling call form submissions,
managing creator information, and integrating with MongoDB for data persistence.

System Architecture:
    1. API Layer:
        - FastAPI router implementation
        - Request validation
        - Error handling
        - Response formatting
    
    2. Data Model:
        - Pydantic schema validation
        - Required fields enforcement
        - Optional field handling
        - Type validation
    
    3. Database Integration:
        - MongoDB async client
        - Document storage
        - Timestamp management
        - Error recovery

Data Schema:
    Creator Information:
        - Basic Details:
            * Legal names
            * Contact information
            * Demographics
        
        - Social Media Metrics:
            * Instagram stats
            * TikTok presence
            * YouTube metrics
            * Snapchat data
        
        - Business Information:
            * Contract status
            * Group chat status
            * Notes and comments
            * Rankings and metrics

Security:
    - Input validation
    - Data sanitization
    - Error isolation
    - Logging controls

Dependencies:
    - FastAPI: Web framework
    - Pydantic: Data validation
    - Motor: Async MongoDB
    - Python-dotenv: Environment management
"""

from fastapi import HTTPException, Request
from fastapi import APIRouter
from typing import Dict, Optional, Any
from pydantic import BaseModel
from datetime import datetime, timezone
from app.shared.database import async_client, DB_NAME
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Create router WITHOUT prefix (it's handled in the route paths)
router = APIRouter()

# Initialize database collection using async client
client_info = async_client[DB_NAME]['ClientInfo']  # Use async client

class CallFormData(BaseModel):
    """
    Call Form Data Model
    
    Validates and structures creator information submitted through the call form.
    
    Fields:
        1. Basic Information:
            - client_id: Unique identifier
            - snap_id: Snapchat identifier
            - First/Last Legal Name: Official names
            - Email/Phone: Contact details
            - DOB: Date of birth
            - Timezone: Creator's timezone
        
        2. Social Media Presence:
            a) Instagram:
                - Username
                - Follower count
                - Verification status
                - Engagement metrics
                - Rankings
            
            b) TikTok:
                - Username
                - Follower count
                - Verification status
                - Rankings
            
            c) YouTube:
                - Username
                - Subscriber count
                - Verification status
                - Rankings
            
            d) Snapchat:
                - Username
                - Follower metrics
                - Star status
                - Monetization status
        
        3. Business Details:
            - Notes
            - Group chat status
            - Contract status
            - Signing status
    
    Validation:
        - Required fields enforcement
        - Type checking
        - Optional field handling
        - Extra field allowance
    """
    client_id: str
    snap_id: Optional[str] = ""
    First_Legal_Name: str
    Last_Legal_Name: str
    Email_Address: str
    Stage_Name: Optional[str] = ""
    DOB: str
    Timezone: str
    Phone: str
    IG_Username: Optional[str] = None
    IG_Followers: Optional[int] = None
    IG_Verified: Optional[bool] = False
    IG_Engagement: Optional[float] = None
    TT_Username: Optional[str] = None
    TT_Followers: Optional[int] = None
    TT_Verified: Optional[bool] = False
    YT_Username: Optional[str] = None
    YT_Followers: Optional[int] = None
    YT_Verified: Optional[bool] = False
    Snap_Username: Optional[str] = None
    Snap_Followers: Optional[str] = None
    Snap_Star: Optional[bool] = False
    Snap_Monetized: Optional[bool] = False
    is_note: Optional[str] = ""
    is_groupchat: Optional[str] = ""
    is_contractout: Optional[str] = ""
    is_signed: Optional[str] = ""
    IG_Rank: Optional[int] = None
    IG_Views_Rank: Optional[int] = None
    TT_Rank: Optional[int] = None
    TT_Views_Rank: Optional[int] = None
    YT_Rank: Optional[int] = None
    YT_Views_Rank: Optional[int] = None

    class Config:
        extra = "allow"  # Allow extra fields
        arbitrary_types_allowed = True
        validate_assignment = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

@router.post("/call")
async def submit_call_form(form_data: CallFormData, request: Request):
    """
    Handle call form submission and store creator information.
    
    Processing Flow:
        1. Request Handling:
            - Raw data logging
            - Pydantic validation
            - Data transformation
        
        2. Data Storage:
            - Timestamp addition
            - MongoDB insertion
            - Result verification
        
        3. Response Generation:
            - Success confirmation
            - Error handling
            - Status reporting
    
    Parameters:
        form_data: Validated CallFormData instance
        request: Raw FastAPI request object
    
    Returns:
        Dict: Operation status and message
    
    Raises:
        HTTPException: For validation or storage failures
    
    Error Handling:
        - Input validation errors
        - Database connection issues
        - Storage failures
        - General exceptions
    """
    try:
        # Log raw request body
        raw_body = await request.json()
        logger.error("DEBUG - Raw request body:")  # Changed to error for visibility
        logger.error(raw_body)  # Changed to error for visibility
        
        # Log validation attempt
        logger.error("DEBUG - Attempting validation with model")
        
        form_dict = form_data.dict()
        logger.error(f"DEBUG - Validated data: {form_dict}")  # Changed to error for visibility
        
        form_dict["created_at"] = datetime.now(timezone.utc)
        
        result = await client_info.insert_one(form_dict)
        
        if result.inserted_id:
            return {"status": "success", "message": "Form submitted successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to save form")
            
    except Exception as e:
        logger.error(f"Error submitting form: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 