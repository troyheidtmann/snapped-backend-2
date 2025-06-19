"""
Lead Management API - Comprehensive Lead and Analytics Management

This module provides FastAPI routes for managing leads, analytics, and associated
operations in the Snapped platform. It handles lead data, notes, employee assignments,
analytics, and various administrative functions.

Features:
--------
1. Lead Management:
   - Lead creation and updates
   - Grid view with filtering
   - Search functionality
   - Status tracking
   - Notes management
   - Employee assignments

2. Analytics:
   - Platform-specific metrics
   - Mobile analytics
   - Snapchat analytics
   - Performance comparisons
   - Historical data analysis

3. Administrative:
   - Algorithm score calculation
   - Payout management
   - Approval workflows
   - Employee search and assignment

Data Model:
----------
Lead Document Structure:
- Basic Info: Names, email, DOB, timezone
- Platform Data: Instagram, TikTok, YouTube, Snapchat metrics
- Status Flags: Monetization, verification, contracts
- Analytics: Views, engagement, rankings
- Administrative: Notes, assignments, approvals

Security:
--------
- Role-based access control
- Query filtering
- Input validation
- Error handling
- Audit logging

Dependencies:
-----------
- FastAPI: Web framework
- MongoDB: Data storage
- Logging: Debug and error tracking
- Authentication: User group management
- Datetime: Timestamp handling

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException, Request, Depends, Query
from app.shared.database import (
    client_info, 
    async_client,
    partners_collection,
    monetized_by_collection,
    referred_by_collection,
    client_payouts
)
from bson import ObjectId
from datetime import datetime, timedelta
from fastapi.responses import JSONResponse
import logging
from bson import json_util
import json
from pymongo import UpdateOne
from app.shared.auth import get_filtered_query, get_current_user_group  # Import our auth helper
import asyncio
from typing import List, Optional

router = APIRouter(prefix="/leads", tags=["leads"])

# Add a second router for backward compatibility with singular "lead"
router_singular = APIRouter(prefix="/lead", tags=["leads"])

# Setup logging
logger = logging.getLogger(__name__)

# Collections
content_data_collection = async_client['ClientDb']['content_data']

# Change this line to use the existing database connection
client_notes = async_client['ClientDb']['ClientNote']  # Using the same database as client_info

# Add this at the top of the file with other imports
field_mappings = {
    'First_Legal_Name': 'First_Legal_Name',
    'Last_Legal_Name': 'Last_Legal_Name',
    'Email_Address': 'Email_Address',
    'Stage_Name': 'Stage_Name',
    'DOB': 'DOB',
    'Timezone': 'Timezone',
    'IG_Username': 'IG_Username',
    'IG_Followers': 'IG_Followers',
    'IG_Verified': 'IG_Verified',
    'IG_Engagement': 'IG_Engagement',
    'IG_Rank': 'IG_Rank',
    'IG_Views_Rank': 'IG_Views_Rank',
    'TT_Username': 'TT_Username',
    'TT_Followers': 'TT_Followers',
    'TT_Verified': 'TT_Verified',
    'TT_Rank': 'TT_Rank',
    'TT_Views_Rank': 'TT_Views_Rank',
    'YT_Username': 'YT_Username',
    'YT_Followers': 'YT_Followers',
    'YT_Verified': 'YT_Verified',
    'YT_Rank': 'YT_Rank',
    'YT_Views_Rank': 'YT_Views_Rank',
    'Snap_Username': 'Snap_Username',
    'Snap_Followers': 'Snap_Followers',
    'Snap_Star': 'Snap_Star',
    'Snap_Monetized': 'Snap_Monetized',
    'is_contractout': 'is_contractout',
    'is_signed': 'is_signed',
    'is_groupchat': 'is_groupchat',
    'is_dead': 'is_dead',
    'client_id': 'client_id',
    'snap_id': 'snap_id',
    'assigned_employees': 'assigned_employees',  # Add this line
}

# Add new collection for algorithm settings
algorithm_settings = async_client['Settings']['AlgorithmSettings']

async def get_algorithm_settings():
    settings = await algorithm_settings.find_one({})
    if not settings:
        # Load default settings from a config file
        with open('app/config/algorithm_settings.json') as f:
            settings = json.load(f)
        await algorithm_settings.insert_one(settings)
    return settings

@router.get("/grid")
async def get_leads_grid(filter_query: dict = Depends(get_filtered_query)):
    """
    Retrieve a filtered grid view of all leads with associated data.
    
    Fetches leads based on user permissions and combines data from multiple
    collections including partner information and analytics metrics.
    
    Args:
        filter_query (dict): Filtering criteria from auth middleware
        
    Returns:
        list: Transformed lead objects containing:
            - Basic information (names, email, etc.)
            - Platform metrics (followers, verification)
            - Contract status
            - Partner information
            - Analytics data
            
    Raises:
        HTTPException: For database errors
    """
    try:
        logger.info(f"Starting get_leads_grid with filter: {filter_query}")
        
        # Get all leads in a single query
        cursor = client_info.find(filter_query)
        leads = await cursor.to_list(length=None)
        logger.info(f"Found {len(leads)} leads after filtering")
        
        # Extract all client IDs for batch queries
        client_ids = [lead.get('client_id') or f"jm{lead.get('DOB', '').replace('-', '')}" 
                     for lead in leads if lead.get('client_id') or lead.get('DOB')]
        
        # Batch fetch all partner data in parallel
        partner_tasks = [
            referred_by_collection.find({"client_id": {"$in": client_ids}}).to_list(None),
            monetized_by_collection.find({"client_id": {"$in": client_ids}}).to_list(None)
        ]
        referred_list, monetized_list = await asyncio.gather(*partner_tasks)
        
        # Create lookup maps for O(1) access
        referred_map = {ref["client_id"]: ref for ref in referred_list}
        monetized_map = {mon["client_id"]: mon for mon in monetized_list}
        
        # Get metrics for both 7 and 30 days
        today = datetime.utcnow()
        seven_days_ago = today - timedelta(days=7)
        thirty_days_ago = today - timedelta(days=30)
        
        transformed_leads = []
        for lead in leads:
            client_id = lead.get('client_id') or f"jm{lead.get('DOB', '').replace('-', '')}"
            
            # Get partner info from maps instead of individual queries
            referred_by = referred_map.get(client_id, {})
            monetized_by = monetized_map.get(client_id, {})
            
            transformed_lead = {
                # Basic Info
                'id': str(lead.get('_id')),
                'First_Legal_Name': lead.get('First_Legal_Name'),
                'Last_Legal_Name': lead.get('Last_Legal_Name'),
                'Email_Address': lead.get('Email_Address'),
                'Stage_Name': lead.get('Stage_Name'),
                'DOB': lead.get('DOB'),
                'Timezone': lead.get('Timezone'),
                'client_id': lead.get('client_id'),
                'snap_id': lead.get('snap_id'),
                
                # Instagram
                'IG_Username': lead.get('IG_Username'),
                'IG_Followers': int(lead.get('IG_Followers', 0)),
                'IG_Verified': bool(lead.get('IG_Verified')),
                'IG_Engagement': float(lead.get('IG_Engagement', 0)),
                'IG_Rank': int(lead.get('IG_Rank', 0)),
                'IG_Views_Rank': int(lead.get('IG_Views_Rank', 0)),
                
                # TikTok
                'TT_Username': lead.get('TT_Username'),
                'TT_Followers': int(lead.get('TT_Followers', 0)),
                'TT_Verified': bool(lead.get('TT_Verified')),
                'TT_Rank': int(lead.get('TT_Rank', 0)),
                'TT_Views_Rank': int(lead.get('TT_Views_Rank', 0)),
                
                # YouTube
                'YT_Username': lead.get('YT_Username'),
                'YT_Followers': int(lead.get('YT_Followers', 0)),
                'YT_Verified': bool(lead.get('YT_Verified')),
                'YT_Rank': int(lead.get('YT_Rank', 0)),
                'YT_Views_Rank': int(lead.get('YT_Views_Rank', 0)),
                
                # Snapchat
                'Snap_Username': lead.get('Snap_Username'),
                'Snap_Followers': lead.get('Snap_Followers'),
                'Snap_Star': bool(lead.get('Snap_Star')),
                'Snap_Monetized': bool(lead.get('Snap_Monetized')),
                
                # Contract Status
                'is_contractout': bool(lead.get('is_contractout')),
                'is_signed': bool(lead.get('is_signed')),
                'is_groupchat': bool(lead.get('is_groupchat')),
                'is_note': lead.get('is_note'),
                'is_dead': bool(lead.get('is_dead')),
                
                # Partner fields - use lookup maps
                'referred_by': referred_by.get('partner_id', ''),
                'referred_by_name': referred_by.get('partner_name', ''),
                'monetized_by': monetized_by.get('partner_id', ''),
                'monetized_by_name': monetized_by.get('partner_name', ''),
                
                # Created At and Other Fields
                'created_at': lead.get('created_at'),
                'assigned_employees': lead.get('assigned_employees', []),
                'score': lead.get('algo_rank', 0),
                
                # Analytics data for both timeframes
                'seven_day_story_views': lead.get('seven_day_story_views', 0),
                'thirty_day_story_views': lead.get('thirty_day_story_views', 0),
                'snap_view_time': lead.get('snap_view_time', 0)
            }
            transformed_leads.append(transformed_lead)
            
        return transformed_leads
        
    except Exception as e:
        logger.error(f"Error in get_leads: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sync/{lead_id}")
async def sync_lead(lead_id: str):
    try:
        lead = client_info.find_one({'_id': ObjectId(lead_id)})
        if not lead:
            raise HTTPException(status_code=404, detail="Lead not found")
            
        result = sync_single_lead(lead_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{lead_id}")
async def update_lead(lead_id: str, request: Request, filter_query: dict = Depends(get_filtered_query)):
    """
    Update lead information with role-based access control.
    
    Updates specified fields for a lead while ensuring user has appropriate
    permissions. Supports partial updates and maintains data integrity.
    
    Args:
        lead_id (str): Lead identifier
        request (Request): Update data
        filter_query (dict): Access control filters
        
    Returns:
        JSONResponse: Update status and modified document
        
    Raises:
        HTTPException: For access, validation, or database errors
    """
    try:
        # Log the incoming request details
        logger.info(f"Received update request for lead_id: {lead_id}")
        logger.info(f"Raw filter_query from auth: {filter_query}")
        
        updates = await request.json()
        logger.info(f"Received updates: {updates}")
        
        update_fields = {}

        # First check if lead exists without any filters
        find_query = {'client_id': lead_id}
        logger.info(f"Checking if lead exists with query: {find_query}")
        
        lead_exists = await client_info.find_one(find_query)
        if not lead_exists:
            logger.error(f"Lead not found with client_id: {lead_id}")
            return JSONResponse(
                status_code=404,
                content={"detail": f"Lead not found with client_id: {lead_id}"}
            )
            
        # Then check if user has access
        access_query = {
            'client_id': lead_id,
            **filter_query
        }
        logger.info(f"Checking access with query: {access_query}")
        
        has_access = await client_info.find_one(access_query)
        if not has_access:
            logger.error(f"Access denied for lead {lead_id}. Query: {access_query}")
            return JSONResponse(
                status_code=403,
                content={"detail": "Access denied"}
            )

        # Process field updates
        for field, value in updates.items():
            if field in field_mappings:
                update_fields[field_mappings[field]] = value

        logger.info(f"Prepared update fields: {update_fields}")

        # Perform the update
        try:
            update_result = await client_info.update_one(
                {'client_id': lead_id},
                {'$set': update_fields}
            )
            logger.info(f"Update result: {update_result.modified_count} documents modified")
            
            if update_result.modified_count == 0:
                logger.warning(f"Update operation didn't modify any documents. Query: {{'client_id': {lead_id}}}")
            
            # Fetch and return updated document
            updated_doc = await client_info.find_one({'client_id': lead_id})
            if not updated_doc:
                return JSONResponse(
                    status_code=404,
                    content={"detail": "Lead not found after update"}
                )
                
            return JSONResponse(
                status_code=200,
                content={
                    "status": "success",
                    "message": "Lead updated successfully",
                    "data": json.loads(json_util.dumps(updated_doc))
                }
            )
            
        except Exception as e:
            logger.error(f"Error during update operation: {str(e)}", exc_info=True)
            return JSONResponse(
                status_code=500,
                content={"detail": f"Database update failed: {str(e)}"}
            )

    except Exception as e:
        logger.error(f"Unexpected error in update_lead: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)}
        )

@router.get("/notes/{client_id}")
async def get_client_notes(client_id: str):
    """
    Retrieve conversation and status notes for a client.
    
    Fetches both conversation history and status updates, creating
    empty structures if no notes exist.
    
    Args:
        client_id (str): Client identifier
        
    Returns:
        dict: Notes data containing:
            - conversation: List of conversation entries
            - status: List of status updates
            
    Raises:
        HTTPException: For database errors
    """
    try:
        print(f"Fetching notes for client: {client_id}")  # Debug log
        notes = await client_notes.find_one({"client_id": client_id})
        
        if not notes:
            # Return empty structure if no notes exist
            return {
                "status": "success",
                "data": {
                    "client_id": client_id,
                    "conversation": [],
                    "status": []
                }
            }
        
        # Ensure both arrays exist
        if "conversation" not in notes:
            notes["conversation"] = []
        if "status" not in notes:
            notes["status"] = []
        
        # Convert ObjectId to string for JSON serialization
        notes["_id"] = str(notes["_id"])
        return {"status": "success", "data": notes}
    
    except Exception as e:
        print(f"Error fetching notes: {str(e)}")  # Debug log
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/notes/{client_id}/{note_type}")
async def add_note(client_id: str, note_type: str, request: Request):
    """
    Add a new note to a client's record.
    
    Creates or updates note arrays, maintaining chronological order
    and author attribution.
    
    Args:
        client_id (str): Client identifier
        note_type (str): Note category (conversation/status)
        request (Request): Note content
        
    Returns:
        dict: Creation status and note data
        
    Raises:
        HTTPException: For validation or database errors
    """
    try:
        print(f"Adding note for client: {client_id}, type: {note_type}")  # Debug log
        note_data = await request.json()
        
        new_note = {
            "text": note_data["text"],
            "timestamp": datetime.utcnow().isoformat(),
            "author": note_data.get("author", "Unknown")
        }
        
        # First, ensure the document exists with both arrays initialized
        await client_notes.update_one(
            {"client_id": client_id},
            {
                "$setOnInsert": {
                    "conversation": [],
                    "status": []
                }
            },
            upsert=True
        )
        
        # Then add the note to the specified array
        result = await client_notes.update_one(
            {"client_id": client_id},
            {
                "$push": {
                    note_type: {
                        "$each": [new_note],
                        "$position": 0  # Add to start of array
                    }
                }
            }
        )
        
        print(f"Update result: {result.modified_count}")  # Debug log
        return {
            "status": "success",
            "message": "Note added successfully",
            "data": new_note
        }
        
    except Exception as e:
        print(f"Error adding note: {str(e)}")  # Debug log
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/notes/{client_id}/{note_type}/{note_timestamp}")
async def delete_note(client_id: str, note_type: str, note_timestamp: str):
    try:
        print(f"Deleting note for client: {client_id}, type: {note_type}, timestamp: {note_timestamp}")
        
        result = await client_notes.update_one(
            {"client_id": client_id},
            {
                "$pull": {
                    note_type: {
                        "timestamp": note_timestamp
                    }
                }
            }
        )
        
        if result.modified_count:
            return {"status": "success", "message": "Note deleted successfully"}
        return {"status": "error", "message": "Note not found"}
        
    except Exception as e:
        print(f"Error deleting note: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/leads/{lead_id}")
async def delete_lead(lead_id: str):
    try:
        result = await client_info.delete_one({'_id': ObjectId(lead_id)})
        if result.deleted_count:
            return {"status": "success", "message": "Lead deleted successfully"}
        return {"status": "error", "message": "Lead not found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/raw")
async def get_leads_raw():
    try:
        # Get all leads
        leads = await client_info.find({}).to_list(None)
        
        # Convert ObjectId to string for JSON serialization
        for lead in leads:
            lead['_id'] = str(lead['_id'])
            
        return leads
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/employees/search")
async def search_employees(
    partner_id: str = Query(...),
    search: str = Query(...)
):
    try:
        employees_collection = async_client["Opps"]["Employees"]
        
        # Log the search parameters
        logger.info(f"Searching employees with partner_id: {partner_id}, search: {search}")
        
        # Search by first name, last name, and user_id
        query = {
            "company_id": partner_id,
            "$or": [
                {"first_name": {"$regex": search, "$options": "i"}},
                {"last_name": {"$regex": search, "$options": "i"}},
                {"user_id": {"$regex": search, "$options": "i"}}  # Added user_id search
            ]
        }
        
        # Log the query
        logger.info(f"MongoDB query: {query}")
        
        cursor = employees_collection.find(query)
        employees = await cursor.to_list(length=None)
        
        # Log the results
        logger.info(f"Found {len(employees)} matching employees")
        
        # Make sure we return the user_id field
        return [
            {
                "user_id": emp["user_id"],  # This should be in format like "bm01211990"
                "first_name": emp["first_name"],
                "last_name": emp["last_name"],
                "email": emp["email"]
            }
            for emp in employees
        ]
        
    except Exception as e:
        logger.error(f"Error searching employees: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to search employees: {str(e)}"
        )

@router.get("/employees/assigned")
async def get_assigned_employees(user_ids: str = Query(..., description="Comma-separated list of user IDs")):
    try:
        # Split the comma-separated string into a list
        user_id_list = user_ids.split(',')
        
        employees_collection = async_client["Opps"]["Employees"]
        
        # Log the user IDs we're searching for
        logger.info(f"Searching for employees with user_ids: {user_id_list}")
        
        # First, find all clients that have any of these employees assigned
        client_cursor = client_info.find(
            {"assigned_employees": {"$in": user_id_list}}
        )
        clients = await client_cursor.to_list(length=None)
        
        # Get the list of actually assigned employee IDs
        assigned_ids = set()
        for client in clients:
            if client.get('assigned_employees'):
                assigned_ids.update(
                    emp_id for emp_id in client['assigned_employees'] 
                    if emp_id in user_id_list
                )
        
        # If no assignments found, return empty list
        if not assigned_ids:
            logger.info("No assigned employees found")
            return []
        
        # Find employees that are actually assigned
        cursor = employees_collection.find({"user_id": {"$in": list(assigned_ids)}})
        employees = await cursor.to_list(length=None)
        
        # Log what we found
        logger.info(f"Found {len(employees)} assigned employees")
        
        # Return only the assigned employees with their details
        return [
            {
                "user_id": emp["user_id"],
                "first_name": emp["first_name"],
                "last_name": emp["last_name"],
                "email": emp["email"]
            }
            for emp in employees
        ]
    except Exception as e:
        logger.error(f"Error getting assigned employees: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get assigned employees: {str(e)}"
        )

@router.get("/employees/debug")
async def debug_employees():
    employees_collection = async_client["Opps"]["Employees"]
    employees = await employees_collection.find({}).to_list(length=None)
    return {
        "employees": [
            {
                "user_id": emp["user_id"],
                "first_name": emp["first_name"],
                "last_name": emp["last_name"],
                "company_id": emp.get("company_id", "NOT_SET")
            }
            for emp in employees
        ]
    }

@router.post("/employees/assign")
async def assign_employee(request: Request):
    """
    Assign an employee to a client by updating both collections.
    Updates both the client's assigned_employees array and the employee's clients array.
    
    Args:
        request (Request): Contains client_id and user_id
        
    Returns:
        JSONResponse: Assignment status
        
    Raises:
        HTTPException: For validation or database errors
    """
    try:
        data = await request.json()
        client_id = data.get('client_id')
        user_id = data.get('user_id')
        
        logger.info(f"Attempting to assign employee {user_id} to client {client_id}")
        
        if not client_id or not user_id:
            raise HTTPException(
                status_code=400,
                detail="Both client_id and user_id are required"
            )

        # First verify the employee exists
        employees_collection = async_client["Opps"]["Employees"]
        employee = await employees_collection.find_one({"user_id": user_id})
        
        if not employee:
            logger.error(f"Employee not found: {user_id}")
            raise HTTPException(
                status_code=404,
                detail=f"Employee not found with ID: {user_id}"
            )

        # Then verify the client exists
        client = await client_info.find_one({'client_id': client_id})
        if not client:
            logger.error(f"Client not found: {client_id}")
            raise HTTPException(
                status_code=404,
                detail=f"Client not found with ID: {client_id}"
            )

        # Update both collections atomically
        async with await async_client.start_session() as session:
            async with session.start_transaction():
                # Update employee's clients array
                emp_result = await employees_collection.update_one(
                    {"user_id": user_id},
                    {"$addToSet": {"clients": client_id}},
                    session=session
                )
                
                # Update client's assigned_employees array
                client_result = await client_info.update_one(
                    {"client_id": client_id},
                    {"$addToSet": {"assigned_employees": user_id}},
                    session=session
                )

                logger.info(f"Assignment update results - Employee matched: {emp_result.matched_count}, "
                          f"modified: {emp_result.modified_count}, Client matched: {client_result.matched_count}, "
                          f"modified: {client_result.modified_count}")

                if emp_result.matched_count == 0 or client_result.matched_count == 0:
                    raise HTTPException(
                        status_code=500,
                        detail="Failed to update one or both documents"
                    )

                # If neither document was modified, the assignment already existed
                if emp_result.modified_count == 0 and client_result.modified_count == 0:
                    logger.info(f"Employee {user_id} already assigned to client {client_id}")
                    return JSONResponse(
                        status_code=200,
                        content={
                            "status": "success",
                            "message": "Employee already assigned",
                            "employee": {
                                "user_id": employee["user_id"],
                                "first_name": employee["first_name"],
                                "last_name": employee["last_name"],
                                "email": employee.get("email")
                            }
                        }
                    )

        logger.info(f"Successfully assigned employee {user_id} to client {client_id}")
        
        # Assignment was successful
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "Employee assigned successfully",
                "employee": {
                    "user_id": employee["user_id"],
                    "first_name": employee["first_name"],
                    "last_name": employee["last_name"],
                    "email": employee.get("email")
                }
            }
        )
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error assigning employee: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to assign employee: {str(e)}"
        )

@router.delete("/employees/unassign")
async def unassign_employee(request: Request):
    """
    Remove an employee assignment from both collections.
    Updates both the client's assigned_employees array and the employee's clients array.
    
    Args:
        request (Request): Contains client_id and user_id
        
    Returns:
        JSONResponse: Unassignment status
        
    Raises:
        HTTPException: For validation or database errors
    """
    try:
        data = await request.json()
        client_id = data.get('client_id')
        user_id = data.get('user_id')
        
        logger.info(f"Attempting to unassign employee {user_id} from client {client_id}")
        
        if not client_id or not user_id:
            raise HTTPException(
                status_code=400,
                detail="Both client_id and user_id are required"
            )

        # First verify the employee exists
        employees_collection = async_client["Opps"]["Employees"]
        employee = await employees_collection.find_one({"user_id": user_id})
        
        if not employee:
            logger.error(f"Employee not found: {user_id}")
            raise HTTPException(
                status_code=404,
                detail=f"Employee not found with ID: {user_id}"
            )

        # Then verify the client exists
        client = await client_info.find_one({'client_id': client_id})
        if not client:
            logger.error(f"Client not found: {client_id}")
            raise HTTPException(
                status_code=404,
                detail=f"Client not found with ID: {client_id}"
            )

        # Update both collections atomically
        async with await async_client.start_session() as session:
            async with session.start_transaction():
                # Remove client from employee's clients array
                emp_result = await employees_collection.update_one(
                    {"user_id": user_id},
                    {"$pull": {"clients": client_id}},
                    session=session
                )
                
                # Remove employee from client's assigned_employees array
                client_result = await client_info.update_one(
                    {"client_id": client_id},
                    {"$pull": {"assigned_employees": user_id}},
                    session=session
                )

                logger.info(f"Unassignment update results - Employee matched: {emp_result.matched_count}, "
                          f"modified: {emp_result.modified_count}, Client matched: {client_result.matched_count}, "
                          f"modified: {client_result.modified_count}")

                if emp_result.matched_count == 0 or client_result.matched_count == 0:
                    raise HTTPException(
                        status_code=500,
                        detail="Failed to update one or both documents"
                    )

                # If neither document was modified, the assignment didn't exist
                if emp_result.modified_count == 0 and client_result.modified_count == 0:
                    logger.info(f"Employee {user_id} was not assigned to client {client_id}")
                    return JSONResponse(
                        status_code=200,
                        content={
                            "status": "success",
                            "message": "Employee was not assigned"
                        }
                    )

        logger.info(f"Successfully unassigned employee {user_id} from client {client_id}")
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": "Employee unassigned successfully"
            }
        )
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error unassigning employee: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to unassign employee: {str(e)}"
        )

def calculate_algo_score(lead):
    """
    Calculate algorithmic score for lead ranking.
    
    Evaluates lead quality based on multiple factors:
    - Platform verification status
    - Follower counts across platforms
    - Engagement rates
    - Cross-platform presence
    
    Args:
        lead (dict): Lead document with platform metrics
        
    Returns:
        int: Calculated score (0-100)
        
    Notes:
        - Scores are weighted by platform importance
        - Cross-platform bonus for multiple active accounts
        - Penalty for empty profiles
    """
    # Load algorithm settings directly from file
    with open('app/shared/algorithmSettings.json') as f:
        settings = json.load(f)
    
    score = 0
    
    # Verification Points
    verification = settings['verification']
    if lead.get('Snap_Star'): score += verification['snapchat']
    if lead.get('TT_Verified'): score += verification['tiktok'] 
    if lead.get('IG_Verified'): score += verification['instagram']
    if lead.get('YT_Verified'): score += verification['youtube']

    # Follower Scores
    followers = settings['followers']
    
    # Instagram
    ig_followers = int(lead.get('IG_Followers', 0) or 0)
    if ig_followers >= followers['instagram']['high']:
        score += followers['instagram']['points']['high']
    elif ig_followers >= followers['instagram']['medium']:
        score += followers['instagram']['points']['medium']
    else:
        score += followers['instagram']['points']['low']

    # TikTok
    tt_followers = int(lead.get('TT_Followers', 0) or 0)
    if tt_followers >= followers['tiktok']['high']:
        score += followers['tiktok']['points']['high']
    elif tt_followers >= followers['tiktok']['medium']:
        score += followers['tiktok']['points']['medium']
    else:
        score += followers['tiktok']['points']['low']

    # YouTube
    yt_followers = int(lead.get('YT_Followers', 0) or 0)
    if yt_followers >= followers['youtube']['high']:
        score += followers['youtube']['points']['high']
    elif yt_followers >= followers['youtube']['medium']:
        score += followers['youtube']['points']['medium']
    else:
        score += followers['youtube']['points']['low']

    # Snapchat
    snap_followers = int(lead.get('Snap_Followers', 0) or 0)
    if snap_followers >= followers['snapchat']['high']:
        score += followers['snapchat']['points']['high']
    elif snap_followers >= followers['snapchat']['medium']:
        score += followers['snapchat']['points']['medium']
    else:
        score += followers['snapchat']['points']['low']

    # Rank Scores
    ranks = settings['ranks']
    
    # Instagram Rank
    ig_rank = int(lead.get('IG_Rank', 0) or 0)
    if ig_rank > 0:
        if ig_rank <= ranks['instagram']['high']:
            score += ranks['instagram']['points']['high']
        elif ig_rank <= ranks['instagram']['medium']:
            score += ranks['instagram']['points']['medium']
        else:
            score += ranks['instagram']['points']['low']

    # TikTok Rank
    tt_rank = int(lead.get('TT_Rank', 0) or 0)
    if tt_rank > 0:
        if tt_rank <= ranks['tiktok']['high']:
            score += ranks['tiktok']['points']['high']
        elif tt_rank <= ranks['tiktok']['medium']:
            score += ranks['tiktok']['points']['medium']
        else:
            score += ranks['tiktok']['points']['low']

    # YouTube Rank
    yt_rank = int(lead.get('YT_Rank', 0) or 0)
    if yt_rank > 0:
        if yt_rank <= ranks['youtube']['high']:
            score += ranks['youtube']['points']['high']
        elif yt_rank <= ranks['youtube']['medium']:
            score += ranks['youtube']['points']['medium']
        else:
            score += ranks['youtube']['points']['low']

    # Engagement Scores
    engagement = settings['engagement']
    
    # Instagram Engagement
    ig_engagement = float(lead.get('IG_Engagement', 0) or 0)
    if ig_engagement >= engagement['instagram']['high']:
        score += engagement['instagram']['points']['high']
    elif ig_engagement >= engagement['instagram']['medium']:
        score += engagement['instagram']['points']['medium']
    else:
        score += engagement['instagram']['points']['low']

    # Cross Platform Bonus
    platforms_present = [
        bool(lead.get('IG_Username')),
        bool(lead.get('TT_Username')),
        bool(lead.get('YT_Username')),
        bool(lead.get('Snap_Username'))
    ]
    if all(platforms_present):
        score += settings['crossPlatform']
    elif sum(platforms_present) > 1:  # Partial bonus for having multiple platforms
        score += (settings['crossPlatform'] * (sum(platforms_present) / 4))

    # Empty penalty for having no meaningful presence
    if not any(platforms_present):
        score += settings['emptyPenalty']

    return max(min(round(score), 100), 0)  # Ensure score is between 0-100

@router.post("/update-scores")
async def update_scores():
    try:
        current_time = datetime.utcnow()
        
        # Get ALL leads instead of just ones needing updates
        leads_to_update = await client_info.find({}).to_list(length=None)
        
        # Update scores
        updated_count = 0
        
        for lead in leads_to_update:
            score = calculate_algo_score(lead)
            result = await client_info.update_one(
                {"_id": lead["_id"]},
                {
                    "$set": {
                        "algo_rank": score,
                        "last_score_update": current_time
                    }
                }
            )
            if result.modified_count > 0:
                updated_count += 1
        
        return {
            "status": "success",
            "message": f"Updated scores for {updated_count} leads",
            "total_processed": len(leads_to_update)
        }
        
    except Exception as e:
        logger.error(f"Error updating scores: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/ranks")
async def get_lead_ranks():
    try:
        # Get all leads with their ranks
        leads = await client_info.find(
            {},
            {
                'client_id': 1,
                'First_Legal_Name': 1,
                'Last_Legal_Name': 1,
                'algo_rank': 1
            }
        ).to_list(None)
        
        # Format response
        formatted_leads = []
        for lead in leads:
            formatted_leads.append({
                'id': str(lead['_id']),
                'client_id': lead.get('client_id'),
                'name': f"{lead.get('First_Legal_Name', '')} {lead.get('Last_Legal_Name', '')}".strip(),
                'score': lead.get('algo_rank', 0)
            })
            
        return formatted_leads
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sync-payout-email")
async def sync_payout_email(request: Request):
    try:
        data = await request.json()
        client_id = data.get('client_id')
        payout_email = data.get('payout_email')
        
        if not client_id or not payout_email:
            raise HTTPException(
                status_code=400, 
                detail="Both client_id and payout_email are required"
            )
            
        # First, get the client info to verify it exists
        client = await client_info.find_one({"client_id": client_id})
        if not client:
            raise HTTPException(
                status_code=404,
                detail=f"Client with ID {client_id} not found"
            )
            
        # Update or create payout record with client_id
        result = await client_payouts.update_one(
            {"payout_email": payout_email.lower().strip()},
            {
                "$set": {
                    "client_id": client_id,
                    "last_synced": datetime.utcnow()
                }
            },
            upsert=True
        )
        
        return {
            "status": "success",
            "message": "Payout email synced with client ID",
            "updated": result.modified_count > 0,
            "created": result.upserted_id is not None,
            "client_id": client_id,
            "payout_email": payout_email
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error syncing payout email: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/payout-info/{client_id}")
async def get_payout_info(client_id: str):
    try:
        # Get payout record for this client
        payout_record = await client_payouts.find_one({"client_id": client_id})
        
        if not payout_record:
            return {
                "status": "not_found",
                "payout_email": None,
                "total_paid": 0,
                "last_sync": None,
                "creator_pulls": []
            }
            
        # Return the data including creator_pulls directly from the record
        return {
            "status": "success",
            "payout_email": payout_record.get("payout_email"),
            "total_paid": payout_record.get("total_paid_to_date", 0),
            "last_sync": payout_record.get("last_synced"),
            "year_to_date": payout_record.get("total_paid_year_to_date", 0),
            "quarter_to_date": payout_record.get("total_paid_quarter_to_date", 0),
            "creator_pulls": payout_record.get("creator_pulls", [])
        }
            
    except Exception as e:
        logger.error(f"Error getting payout info: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/update-approval-status")
async def update_approval_status(request: Request):
    try:
        data = await request.json()
        client_id = data.get('client_id')
        
        if not client_id:
            raise HTTPException(
                status_code=400, 
                detail="client_id is required"
            )
        
        # Access the UploadDB.Uploads collection
        uploads_collection = async_client['UploadDB']['Uploads']
        
        # Only include approved in update_data if it was explicitly provided
        update_data = {}
        if 'approved' in data:
            update_data['approved'] = data['approved']
            
            # Only add approver data if it's being approved
            if data['approved'] and data.get('approver_id'):
                update_data["approved_by"] = data["approver_id"]
                update_data["approved_at"] = datetime.utcnow()
        
        if not update_data:
            return {
                "status": "success",
                "message": "No changes requested"
            }
        
        # Update all documents for this client_id
        result = await uploads_collection.update_many(
            {"client_ID": client_id},
            {"$set": update_data}
        )
        
        return {
            "status": "success",
            "message": f"Updated approval status for client {client_id}",
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
            "approved": update_data.get('approved'),
            "approver_id": update_data.get('approved_by')
        }
        
    except Exception as e:
        logger.error(f"Error updating approval status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/approval-status/{client_id}")
async def get_approval_status(client_id: str):
    try:
        # Access the UploadDB.Uploads collection
        uploads_collection = async_client['UploadDB']['Uploads']
        
        # Find documents for this client
        document = await uploads_collection.find_one({"client_ID": client_id})
        
        if not document:
            return {
                "status": "success",
                "message": "No documents found for this client",
                "approved": False
            }
        
        # Return the approval status
        return {
            "status": "success",
            "approved": document.get("approved", False)
        }
        
    except Exception as e:
        logger.error(f"Error getting approval status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/settings")
async def get_algorithm_settings():
    """Get algorithm settings"""
    try:
        with open('app/shared/algorithmSettings.json') as f:
            settings = json.load(f)
        return settings
    except Exception as e:
        logger.error(f"Error getting algorithm settings: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/mobile")
async def get_mobile_analytics(
    user_ids: str = Query(None),
    user_id: str = Query(None),
    days: int = Query(7, description="Number of days to fetch (7 or 30)"),
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Retrieve mobile analytics for single or multiple users.
    
    Aggregates analytics data across specified timeframes, supporting
    both individual and batch queries.
    
    Args:
        user_ids (str, optional): Comma-separated list of user IDs
        user_id (str, optional): Single user ID
        days (int): Timeframe (7 or 30 days)
        auth_data (dict): User authentication data
        
    Returns:
        dict: Analytics metrics including:
            - Views and impressions
            - Reach and engagement
            - Platform-specific metrics
            
    Raises:
        HTTPException: For validation or database errors
    """
    try:
        if days not in [7, 30]:
            raise HTTPException(status_code=400, detail="Days parameter must be either 7 or 30")
            
        # Calculate date range
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)
        
        # Format dates for MongoDB query
        formatted_start = start_date.strftime("%m-%d-%Y")
        formatted_end = end_date.strftime("%m-%d-%Y")
        
        # Log the date range we're querying
        logger.info(f"Fetching analytics from {formatted_start} to {formatted_end}")
        
        # Handle multiple user IDs
        if user_ids:
            client_ids = [cid.strip() for cid in user_ids.split(',') if cid.strip()]
            if not client_ids:
                logger.warning("No valid client IDs provided")
                return {
                    "status": "success",
                    "total_views": 0,
                    "impressions": 0,
                    "reach": 0,
                    "story_view_time": 0,
                    "timeframe": f"{days}d"
                }
                
            logger.info(f"Fetching analytics for {len(client_ids)} clients: {client_ids}")
            
            # Build the aggregation pipeline
            pipeline = [
                # Match documents for the specified clients
                {"$match": {
                    "$or": [
                        {"client_id": {"$in": client_ids}},
                        {"user_id": {"$in": client_ids}}
                    ],
                    "platform": "snapchat"
                }},
                # Unwind the sessions array
                {"$unwind": "$sessions"},
                # Convert string date to date object for comparison
                {"$addFields": {
                    "session_date": "$sessions.date"
                }},
                # Match sessions within the date range
                {"$match": {
                    "session_date": {
                        "$gte": formatted_start,
                        "$lte": formatted_end
                    }
                }},
                # Group and sum all metrics
                {"$group": {
                    "_id": None,
                    "total_views": {"$sum": "$sessions.metrics.views.story_views"},
                    "impressions": {"$sum": "$sessions.metrics.views.impressions"},
                    "reach": {"$sum": "$sessions.metrics.views.reach"},
                    "snap_view_time": {"$sum": "$sessions.metrics.time_metrics.snap_view_time"},
                    "profile_views": {"$sum": "$sessions.metrics.views.profile_views"},
                    "spotlight_views": {"$sum": "$sessions.metrics.views.spotlight_views"},
                    "saved_story_views": {"$sum": "$sessions.metrics.views.saved_story_views"}
                }}
            ]
            
            # Log the pipeline for debugging
            logger.info(f"MongoDB pipeline: {pipeline}")
            
            # Execute the aggregation
            result = await content_data_collection.aggregate(pipeline).to_list(length=None)
            
            # Log the results for debugging
            logger.info(f"Aggregation result: {result}")
            
            if result:
                metrics = result[0]
                return {
                    "status": "success",
                    "total_views": metrics.get("total_views", 0),
                    "impressions": metrics.get("impressions", 0),
                    "reach": metrics.get("reach", 0),
                    "story_view_time": metrics.get("snap_view_time", 0),
                    "profile_views": metrics.get("profile_views", 0),
                    "spotlight_views": metrics.get("spotlight_views", 0),
                    "saved_story_views": metrics.get("saved_story_views", 0),
                    "timeframe": f"{days}d"
                }
            else:
                return {
                    "status": "success",
                    "total_views": 0,
                    "impressions": 0,
                    "reach": 0,
                    "story_view_time": 0,
                    "profile_views": 0,
                    "spotlight_views": 0,
                    "saved_story_views": 0,
                    "timeframe": f"{days}d"
                }
            
        # Handle single user ID
        elif user_id:
            logger.info(f"Fetching analytics for single client: {user_id}")
            
            pipeline = [
                # Match the specific client
                {"$match": {
                    "$or": [
                        {"client_id": user_id},
                        {"user_id": user_id}
                    ],
                    "platform": "snapchat"
                }},
                # Unwind the sessions array
                {"$unwind": "$sessions"},
                # Use the session date directly for comparison
                {"$addFields": {
                    "session_date": "$sessions.date"
                }},
                # Match sessions within the date range
                {"$match": {
                    "session_date": {
                        "$gte": formatted_start,
                        "$lte": formatted_end
                    }
                }},
                # Group and sum all metrics for single user
                {"$group": {
                    "_id": None,
                    "total_views": {"$sum": "$sessions.metrics.views.story_views"},
                    "impressions": {"$sum": "$sessions.metrics.views.impressions"},
                    "reach": {"$sum": "$sessions.metrics.views.reach"},
                    "snap_view_time": {"$sum": "$sessions.metrics.time_metrics.snap_view_time"},
                    "profile_views": {"$sum": "$sessions.metrics.views.profile_views"},
                    "spotlight_views": {"$sum": "$sessions.metrics.views.spotlight_views"},
                    "saved_story_views": {"$sum": "$sessions.metrics.views.saved_story_views"}
                }}
            ]
            
            # Log the pipeline for debugging
            logger.info(f"MongoDB pipeline for single client: {pipeline}")
            
            # Execute the aggregation
            result = await content_data_collection.aggregate(pipeline).to_list(length=None)
            
            # Log the result for debugging
            logger.info(f"Single client result: {result}")
            
            if result:
                metrics = result[0]
                return {
                    "status": "success",
                    "total_views": metrics.get("total_views", 0),
                    "impressions": metrics.get("impressions", 0),
                    "reach": metrics.get("reach", 0),
                    "story_view_time": metrics.get("snap_view_time", 0),
                    "profile_views": metrics.get("profile_views", 0),
                    "spotlight_views": metrics.get("spotlight_views", 0),
                    "saved_story_views": metrics.get("saved_story_views", 0),
                    "timeframe": f"{days}d"
                }
            else:
                return {
                    "status": "success",
                    "total_views": 0,
                    "impressions": 0,
                    "reach": 0,
                    "story_view_time": 0,
                    "profile_views": 0,
                    "spotlight_views": 0,
                    "saved_story_views": 0,
                    "timeframe": f"{days}d"
                }
        
        else:
            raise HTTPException(status_code=400, detail="Either user_id or user_ids must be provided")
            
    except Exception as e:
        logger.error(f"Error in get_mobile_analytics: {str(e)}")
        logger.exception("Full error details:")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/snapchat")
async def get_snapchat_analytics(
    client_id: str = Query(..., description="Client ID to fetch analytics for"),
    start_date: str = Query(..., description="Start date in MM-DD-YYYY format"),
    end_date: str = Query(..., description="End date in MM-DD-YYYY format")
):
    try:
        # Parse dates
        start = datetime.strptime(start_date, "%m-%d-%Y")
        end = datetime.strptime(end_date, "%m-%d-%Y")
        
        # Get analytics data
        analytics_collection = async_client['Analytics']['SnapchatAnalytics']
        cursor = analytics_collection.find({
            "client_id": client_id,
            "date": {
                "$gte": start,
                "$lte": end
            }
        })
        analytics_data = await cursor.to_list(length=None)
        
        return {
            "status": "success",
            "data": {
                "sessions": analytics_data
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting Snapchat analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/analytics/comparison")
async def get_analytics_comparison(
    user_ids: str = Query(None, description="Comma-separated list of user IDs"),
    days: int = Query(7, description="Number of days to fetch (7 or 30)")
):
    try:
        if days not in [7, 30]:
            raise HTTPException(status_code=400, detail="Days parameter must be either 7 or 30")
            
        # Calculate date ranges for current and previous periods
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)
        previous_end_date = start_date
        previous_start_date = previous_end_date - timedelta(days=days)
        
        # Format dates for queries
        current_start = start_date.strftime("%Y-%m-%d")
        current_end = end_date.strftime("%Y-%m-%d")
        previous_start = previous_start_date.strftime("%Y-%m-%d")
        previous_end = previous_end_date.strftime("%Y-%m-%d")
        
        # Split user_ids if provided
        user_id_list = user_ids.split(',') if user_ids else None
        
        # Build base query
        base_query = {}
        if user_id_list:
            base_query["user_id"] = {"$in": user_id_list}
            
        # Get analytics data for both periods
        analytics_collection = async_client['Analytics']['MobileAnalytics']
        
        # Current period query
        current_query = {
            **base_query,
            "date": {
                "$gte": current_start,
                "$lte": current_end
            }
        }
        
        # Previous period query
        previous_query = {
            **base_query,
            "date": {
                "$gte": previous_start,
                "$lte": previous_end
            }
        }
        
        # Execute both queries in parallel
        current_cursor = analytics_collection.find(current_query)
        previous_cursor = analytics_collection.find(previous_query)
        
        current_data, previous_data = await asyncio.gather(
            current_cursor.to_list(length=None),
            previous_cursor.to_list(length=None)
        )
        
        # Calculate totals for both periods
        current_total_views = sum(data.get('story_views', 0) for data in current_data)
        previous_total_views = sum(data.get('story_views', 0) for data in previous_data)
        
        # Calculate percentage change
        percent_change = 0
        if previous_total_views > 0:
            percent_change = ((current_total_views - previous_total_views) / previous_total_views) * 100
        
        return {
            "status": "success",
            "current_period": {
                "start_date": current_start,
                "end_date": current_end,
                "total_views": current_total_views
            },
            "previous_period": {
                "start_date": previous_start,
                "end_date": previous_end,
                "total_views": previous_total_views
            },
            "comparison": {
                "percent_change": round(percent_change, 2),
                "absolute_change": current_total_views - previous_total_views
            },
            "timeframe": f"{days}d"
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error getting analytics comparison: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Add routes for singular "lead" endpoint
@router_singular.post("/notes/{client_id}/{note_type}")
async def add_note_singular(client_id: str, note_type: str, request: Request):
    """Redirect to plural endpoint for backward compatibility"""
    return await add_note(client_id, note_type, request)

@router_singular.get("/notes/{client_id}")
async def get_client_notes_singular(client_id: str):
    """Redirect to plural endpoint for backward compatibility"""
    return await get_client_notes(client_id)

@router_singular.delete("/notes/{client_id}/{note_type}/{note_timestamp}")
async def delete_note_singular(client_id: str, note_type: str, note_timestamp: str):
    """Redirect to plural endpoint for backward compatibility"""
    return await delete_note(client_id, note_type, note_timestamp)

# Add this line at the end of the file
__all__ = ['router', 'router_singular']
