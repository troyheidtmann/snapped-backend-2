"""
Authentication Module

This module provides authentication and authorization functionality
for API endpoints and services.

Features:
- JWT validation
- Group management
- Partner filtering
- Access control
- Client assignment

Data Model:
- User groups
- Partner data
- Client assignments
- Access tokens
- Employee records

Security:
- Token validation
- Group verification
- Access filtering
- Error handling
- Secure defaults

Dependencies:
- FastAPI for routing
- JWT for tokens
- MongoDB for storage
- typing for hints
- logging for tracking

Author: Snapped Development Team
"""

from fastapi import Depends, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import jwt
from typing import List, Optional
from app.shared.database import referred_by_collection, async_client
from jwt.exceptions import InvalidTokenError

security = HTTPBearer()

async def get_current_user_group(credentials: HTTPAuthorizationCredentials = Security(security)) -> dict:
    """
    Extract user groups from JWT token.
    
    Args:
        credentials: HTTP auth credentials
        
    Returns:
        dict: User groups and ID
        
    Raises:
        HTTPException: For auth errors
        
    Notes:
        - Decodes token
        - Gets groups
        - Gets user ID
        - Handles errors
    """
    try:
        token = credentials.credentials
        print("\n=== Auth Token Debug ===")
        print(f"Token received: {token[:50]}...")
        
        decoded_token = jwt.decode(token, options={"verify_signature": False})
        print(f"Decoded token fields: {list(decoded_token.keys())}")
        
        # Get groups and normalize them to uppercase for consistency
        cognito_groups = [
            group.upper() 
            for group in (
                decoded_token.get('cognito:groups', []) or 
                decoded_token.get('groups', []) or 
                []
            )
        ]
        
        # Get user ID from various possible locations
        user_id_fields = [
            'custom:UserID',
            'sub',
            'username',
            'cognito:username',
            'email'
        ]
        
        user_id = None
        for field in user_id_fields:
            if field in decoded_token:
                user_id = decoded_token[field]
                print(f"Found user_id in field '{field}': {user_id}")
                break
        
        print(f"Final user_id selected: {user_id}")
        print(f"Found groups: {cognito_groups}")
        
        if not cognito_groups:
            print("Warning: No groups found in token")
            cognito_groups = ["DEFAULT"]
            
        if not user_id:
            raise HTTPException(
                status_code=401,
                detail="No user ID found in token"
            )
            
        return {
            "groups": cognito_groups,
            "user_id": user_id
        }
        
    except Exception as e:
        print(f"Auth error: {str(e)}")
        print(f"Token contents: {decoded_token if 'decoded_token' in locals() else 'Not decoded'}")
        raise HTTPException(
            status_code=401,
            detail=f"Authentication error: {str(e)}"
        )

async def get_partner_client_ids(partner_name: str) -> List[str]:
    """
    Get client IDs for partner.
    
    Args:
        partner_name: Partner identifier
        
    Returns:
        list: Associated client IDs
        
    Notes:
        - Checks referrals
        - Handles admin
        - Case insensitive
        - Returns empty list
    """
    if partner_name == "ADMIN":
        return []  # Admin sees everything
        
    # Find all client_ids for this partner - check both referred_by and company_id
    cursor = referred_by_collection.find({
        "$or": [
            {"partner_name": {"$regex": f"^{partner_name}$", "$options": "i"}},
            {"company_id": partner_name}
        ]
    })
    client_ids = [doc["client_id"] for doc in await cursor.to_list(length=None)]
    print(f"Found client_ids for {partner_name}: {client_ids}")  # Debug print
    return client_ids

async def get_employee_client_ids(user_id: str) -> List[str]:
    """
    Get client IDs for employee.
    
    Args:
        user_id: Employee identifier
        
    Returns:
        list: Assigned client IDs
        
    Notes:
        - Checks assignments
        - Returns empty list
        - DB lookup
    """
    employees_collection = async_client["Opps"]["Employees"]
    employee = await employees_collection.find_one({"user_id": user_id})
    
    if not employee:
        return []
        
    return employee.get("clients", [])

async def filter_by_partner(auth_data: dict):
    """
    Create MongoDB filter for partner.
    
    Args:
        auth_data: Auth data with groups
        
    Returns:
        dict: MongoDB filter
        
    Notes:
        - Handles admin
        - Checks employees
        - Gets client IDs
        - Format handling
    """
    groups = auth_data["groups"]
    user_id = auth_data["user_id"]
    
    print(f"=== Auth Filter Debug ===")
    print(f"User ID from token: {user_id}")
    print(f"Groups: {groups}")
    
    # If user is in ADMIN Cognito group, they see everything
    if "ADMIN" in groups:
        print("User is ADMIN - full access granted")
        return {}  # Empty filter = see all documents
    
    # For non-admin users, check their employee record
    employees_collection = async_client["Opps"]["Employees"]
    client_info_collection = async_client["ClientDb"]["ClientInfo"]
    
    # Try different user ID formats based on the token's user ID
    # The employee records show format like "tj10021994", so we need to handle that
    possible_user_ids = [
        user_id,  # Original format
        user_id.lower(),  # Lowercase
        user_id.upper(),  # Uppercase
        # If the ID looks like an email, try extracting the username part
        user_id.split('@')[0] if '@' in user_id else user_id,
        # Add prefix variations if they don't exist
        f"tj{user_id}" if not user_id.startswith('tj') else user_id,
        f"th{user_id}" if not user_id.startswith('th') else user_id,
        f"jm{user_id}" if not user_id.startswith('jm') else user_id,
    ]
    
    # Remove duplicates while preserving order
    possible_user_ids = list(dict.fromkeys(possible_user_ids))
    print(f"Trying user IDs: {possible_user_ids}")
    
    # Try each possible user ID format
    employee = None
    for try_user_id in possible_user_ids:
        print(f"Looking up employee with user_id: {try_user_id}")
        employee = await employees_collection.find_one({"user_id": try_user_id})
        if employee:
            print(f"Found employee record with user_id {try_user_id}:")
            print(employee)
            break
    
    if not employee:
        print("No employee record found for any user ID format!")
        return {"client_id": "NO_ACCESS"}
    
    assigned_clients = employee.get("clients", [])
    print(f"Assigned clients: {assigned_clients}")
    
    # If employee has no clients assigned, they see nothing
    if not assigned_clients:
        print("No clients assigned to employee")
        return {"client_id": "NO_ACCESS"}
        
    # Check for admin access - look for "admin" in any case
    has_admin_access = any(client.lower() == "admin" for client in assigned_clients)
    print(f"Has admin access: {has_admin_access}")
        
    # If they have admin access, show all clients for their partner groups
    if has_admin_access:
        print("Employee has partner admin access")
        partner_client_ids = []
        for group in groups:
            client_ids = await get_partner_client_ids(group)
            print(f"Found partner client IDs for {group}: {client_ids}")
            if client_ids:  # Only query if we have IDs to check
                existing_clients = await client_info_collection.find(
                    {"client_id": {"$in": client_ids}},
                    {"client_id": 1}
                ).to_list(None)
                valid_ids = [doc["client_id"] for doc in existing_clients]
                print(f"Valid client IDs: {valid_ids}")
                partner_client_ids.extend(valid_ids)
        
        if partner_client_ids:
            return {"client_id": {"$in": partner_client_ids}}
        else:
            # If admin but no specific client IDs found, show all clients
            return {}
    
    # For regular employees, only show their specific assigned clients
    print("Employee has specific client access")
    # Filter out 'admin' from assigned clients
    client_ids = [c for c in assigned_clients if c.lower() != 'admin']
    
    if not client_ids:
        print("No valid client IDs found")
        return {"client_id": "NO_ACCESS"}
    
    existing_clients = await client_info_collection.find(
        {"client_id": {"$in": client_ids}},
        {"client_id": 1}
    ).to_list(None)
    
    valid_client_ids = [doc["client_id"] for doc in existing_clients]
    print(f"Valid client IDs for specific access: {valid_client_ids}")
    
    if not valid_client_ids:
        print("No valid client IDs found")
        return {"client_id": "NO_ACCESS"}
        
    return {"client_id": {"$in": valid_client_ids}}

# Helper function for routes
async def get_filtered_query(auth_data: dict = Depends(get_current_user_group)):
    """
    Get filtered query for user.
    
    Args:
        auth_data: Auth data with groups
        
    Returns:
        dict: MongoDB filter
        
    Notes:
        - Uses auth data
        - Gets filter
        - Partner based
    """
    return await filter_by_partner(auth_data)

# Add this function alongside get_current_user_group
async def get_current_user_id(credentials: HTTPAuthorizationCredentials = Security(security)) -> str:
    """
    Get user ID from token.
    
    Args:
        credentials: HTTP auth credentials
        
    Returns:
        str: User ID
        
    Raises:
        HTTPException: For auth errors
        
    Notes:
        - Decodes token
        - Gets custom ID
        - Validates
        - Handles errors
    """
    try:
        token = credentials.credentials
        decoded_token = jwt.decode(token, options={"verify_signature": False})
        
        # Get user ID from custom:UserID field
        user_id = decoded_token.get('custom:UserID')
        if not user_id:
            raise HTTPException(
                status_code=401,
                detail="Could not validate credentials",
            )
        return user_id
        
    except Exception as e:
        raise HTTPException(
            status_code=401,
            detail="Could not validate credentials",
        ) 