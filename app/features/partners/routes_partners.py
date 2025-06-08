"""
Partner Management API - Partner and Client Relationship Management

This module provides FastAPI routes for managing partners and their relationships
with clients in the Snapped platform. It handles partner creation, assignment,
and relationship tracking for both monetization and referral connections.

Features:
--------
1. Partner Management:
   - Partner creation and listing
   - Partner assignment to clients
   - Relationship tracking
   - Status updates

2. Relationship Types:
   - Monetization tracking
   - Referral management
   - Partner reassignment
   - Relationship removal

Data Model:
----------
Partner Structure:
- Basic: ID, name
- Relationships:
  - Monetization: partner_id, partner_name, client_id
  - Referral: partner_id, partner_name, client_id

Security:
--------
- Input validation
- Error handling
- Relationship integrity
- Data consistency checks

Dependencies:
-----------
- FastAPI: Web framework
- MongoDB: Data storage
- BSON: ObjectId handling
- Error Handling: HTTPException

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException
from app.shared.database import partners_collection, monetized_by_collection, referred_by_collection, client_info
from bson import ObjectId

router = APIRouter(prefix="/api/partners", tags=["partners"])

@router.get("")
async def get_partners():
    """
    Retrieve all partners in the system.
    
    Returns a list of all partners with their IDs and names,
    transforming ObjectIds to strings for JSON compatibility.
    
    Returns:
        list: Partner objects containing:
            - id: Partner identifier (string)
            - name: Partner name
            
    Raises:
        HTTPException: For database errors
    """
    try:
        partners = await partners_collection.find({}).to_list(None)
        return [{"id": str(p["_id"]), "name": p["name"]} for p in partners]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("")
async def add_partner(partner: dict):
    """
    Add a new partner to the system.
    
    Creates a new partner record with the provided name.
    
    Args:
        partner (dict): Partner data containing:
            - name: Partner name
            
    Returns:
        dict: Created partner with:
            - id: Generated partner ID
            - name: Partner name
            
    Raises:
        HTTPException: For database errors
    """
    try:
        result = await partners_collection.insert_one({"name": partner["name"]})
        return {"id": str(result.inserted_id), "name": partner["name"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/monetized")
async def update_monetized_by(data: dict):
    """
    Update the monetization relationship between a client and partner.
    
    Handles both assignment and removal of monetization relationships.
    Supports null assignments to remove relationships.
    
    Args:
        data (dict): Relationship data containing:
            - client_id: Client identifier
            - partner_id: Partner identifier (or null)
            
    Returns:
        dict: Operation status
        
    Raises:
        HTTPException: For invalid partners or database errors
        
    Notes:
        - Removes forward slashes from client IDs
        - Verifies partner existence before assignment
        - Supports relationship removal via null partner_id
    """
    try:
        client_id = data['client_id'].replace("/", "")
        
        # Case 1: Changing to "Select Partner" (null)
        if not data["partner_id"]:
            # Remove the document entirely
            await monetized_by_collection.delete_one({"client_id": client_id})
            return {"status": "success"}
            
        # Case 2: New assignment or updating existing
        try:
            partner = await partners_collection.find_one({"_id": ObjectId(data["partner_id"])})
            if not partner:
                raise HTTPException(status_code=404, detail="Partner not found")
                
            # Update or create document
            await monetized_by_collection.update_one(
                {"client_id": client_id},
                {"$set": {
                    "partner_id": data["partner_id"],
                    "partner_name": partner["name"]
                }},
                upsert=True
            )
            return {"status": "success"}
            
        except Exception as e:
            print(f"Error processing partner: {str(e)}")
            raise HTTPException(status_code=400, detail="Invalid partner ID")
            
    except Exception as e:
        print(f"Error in update_monetized_by: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/referred")
async def update_referred_by(data: dict):
    """
    Update the referral relationship between a client and partner.
    
    Handles both assignment and removal of referral relationships.
    Supports null assignments to remove relationships.
    
    Args:
        data (dict): Relationship data containing:
            - client_id: Client identifier
            - partner_id: Partner identifier (or null)
            
    Returns:
        dict: Operation status
        
    Raises:
        HTTPException: For invalid partners or database errors
        
    Notes:
        - Removes forward slashes from client IDs
        - Verifies partner existence before assignment
        - Supports relationship removal via null partner_id
    """
    try:
        client_id = data['client_id'].replace("/", "")
        
        # Case 1: Changing to "Select Partner" (null)
        if not data["partner_id"]:
            # Remove the document entirely
            await referred_by_collection.delete_one({"client_id": client_id})
            return {"status": "success"}
            
        # Case 2: New assignment or updating existing
        try:
            partner = await partners_collection.find_one({"_id": ObjectId(data["partner_id"])})
            if not partner:
                raise HTTPException(status_code=404, detail="Partner not found")
                
            # Update or create document
            await referred_by_collection.update_one(
                {"client_id": client_id},
                {"$set": {
                    "partner_id": data["partner_id"],
                    "partner_name": partner["name"]
                }},
                upsert=True
            )
            return {"status": "success"}
            
        except Exception as e:
            print(f"Error processing partner: {str(e)}")
            raise HTTPException(status_code=400, detail="Invalid partner ID")
            
    except Exception as e:
        print(f"Error in update_referred_by: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/monetized/{client_id}")
async def get_monetized_by(client_id: str):
    """
    Retrieve the monetization partner for a client.
    
    Args:
        client_id (str): Client identifier
        
    Returns:
        dict: Partner information containing:
            - partner_id: Partner identifier (empty if none)
            - partner_name: Partner name (empty if none)
            
    Raises:
        HTTPException: For database errors
        
    Notes:
        - Returns empty strings if no relationship exists
    """
    try:
        result = await monetized_by_collection.find_one({"client_id": client_id})
        if not result:
            return {"partner_id": "", "partner_name": ""}
        return {
            "partner_id": result["partner_id"],
            "partner_name": result["partner_name"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/referred/{client_id}")
async def get_referred_by(client_id: str):
    """
    Retrieve the referral partner for a client.
    
    Args:
        client_id (str): Client identifier
        
    Returns:
        dict: Partner information containing:
            - partner_id: Partner identifier (empty if none)
            - partner_name: Partner name (empty if none)
            
    Raises:
        HTTPException: For database errors
        
    Notes:
        - Returns empty strings if no relationship exists
    """
    try:
        result = await referred_by_collection.find_one({"client_id": client_id})
        if not result:
            return {"partner_id": "", "partner_name": ""}
        return {
            "partner_id": result["partner_id"],
            "partner_name": result["partner_name"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 