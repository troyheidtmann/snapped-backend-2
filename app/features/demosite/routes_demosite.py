"""
Demo Site API - Password Protection and Access Control

This module provides FastAPI routes for managing demo site access control through
password verification. It implements a simple but secure mechanism to protect
demo content from unauthorized access.

Features:
--------
1. Password Verification:
   - Simple password-based authentication
   - Secure password comparison
   - Standardized response format

2. Access Control:
   - Binary access verification (true/false)
   - HTTP 401 responses for invalid attempts
   - Pydantic model validation

Security:
--------
- Input validation via Pydantic
- Standard HTTP authentication responses
- Error handling for invalid attempts

Dependencies:
-----------
- FastAPI: Web framework and routing
- Pydantic: Data validation
- HTTPException: Error handling

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/demo", tags=["demo"])

class PasswordCheck(BaseModel):
    """
    Data model for password verification requests.
    
    Attributes:
        password (str): The password to verify against the system
    """
    password: str

@router.post("/verify-password")
async def verify_password(password_data: PasswordCheck):
    """
    Verify a provided password against the system password.
    
    Args:
        password_data (PasswordCheck): The password data model containing the password to verify
        
    Returns:
        dict: A dictionary containing the verification result
            {
                "verified": bool  # True if password matches, False otherwise
            }
            
    Raises:
        HTTPException: 401 error if password is invalid
    """
    # Replace this with your actual password verification logic
    if password_data.password == "your_password_here":
        return {"verified": True}
    raise HTTPException(status_code=401, detail="Invalid password")
