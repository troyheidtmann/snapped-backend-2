"""
Client Routes - API Endpoints for Client Management

This module provides FastAPI routes for managing client information in the Snapped platform.
It handles client data retrieval with proper access control and filtering based on user
permissions and group memberships.

API Endpoints:
------------
GET /api/clients
- List clients with permission-based filtering
- Supports partner-specific access control
- Returns filtered client list

GET /api/clients/signed
- List basic information for signed clients
- Returns only essential fields
- Supports contract status tracking

Security:
--------
- Partner-based filtering
- Group-based access control
- Field-level projection
- Error handling

Data Models:
----------
Client Document:
{
    "First_Legal_Name": str,
    "Last_Legal_Name": str,
    "client_id": str,
    "is_signed": bool,
    ...additional fields
}

Dependencies:
-----------
- FastAPI: Web framework
- MongoDB Motor: Database operations
- Shared auth: Access control
- Logging: Operation tracking

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException, Depends
from app.shared.database import client_info
from typing import List, Dict
import logging
from app.shared.auth import get_filtered_query

router = APIRouter(
    prefix="/api/clients",
    tags=["clients"]
)
logger = logging.getLogger(__name__)

@router.get("")
async def get_clients(filter_query: dict = Depends(get_filtered_query)):
    """
    Retrieve a filtered list of clients based on user permissions.
    
    This endpoint returns client information filtered by the user's group
    membership and access level. The filtering is automatically applied
    through the get_filtered_query dependency.
    
    Args:
        filter_query (dict): Automatically injected query filter based on
                           user's permissions and group membership
    
    Returns:
        List[dict]: List of client documents with fields:
            - First_Legal_Name (str)
            - Last_Legal_Name (str)
            - client_id (str)
            - Additional fields based on access level
    
    Security:
    --------
    - Requires authentication
    - Filtered by user's group
    - Partner-specific access
    
    Example Response:
    ---------------
    [
        {
            "First_Legal_Name": "John",
            "Last_Legal_Name": "Doe",
            "client_id": "jd12345678"
        },
        ...
    ]
    """
    clients = await client_info.find(filter_query).to_list(length=None)
    return clients

@router.get("/signed")
async def get_signed_clients():
    """
    Retrieve basic information for all signed clients.
    
    This endpoint returns a list of clients who have signed contracts,
    including only essential identification fields. It's optimized for
    quick lookups and basic client verification.
    
    Returns:
        dict: Response object with structure:
            {
                "status": "success",
                "clients": [
                    {
                        "First_Legal_Name": str,
                        "Last_Legal_Name": str,
                        "client_id": str
                    },
                    ...
                ]
            }
    
    Fields:
    ------
    - First_Legal_Name: Client's legal first name
    - Last_Legal_Name: Client's legal last name
    - client_id: Unique client identifier
    
    Query Optimization:
    -----------------
    - Filtered by is_signed=True
    - Projected fields for performance
    - Excluded MongoDB _id
    
    Error Handling:
    -------------
    - 500: Internal server error with details
    - Logged errors for debugging
    
    Example Response:
    ---------------
    {
        "status": "success",
        "clients": [
            {
                "First_Legal_Name": "Jane",
                "Last_Legal_Name": "Smith",
                "client_id": "js98765432"
            }
        ]
    }
    """
    try:
        # Query for signed clients and project only needed fields
        cursor = client_info.find(
            {"is_signed": True},
            {
                "First_Legal_Name": 1,
                "Last_Legal_Name": 1,
                "client_id": 1,
                "_id": 0  # Exclude MongoDB _id
            }
        )
        
        # Convert cursor to list
        clients = await cursor.to_list(length=None)
        
        return {
            "status": "success",
            "clients": clients
        }
        
    except Exception as e:
        logger.error(f"Error fetching signed clients: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 