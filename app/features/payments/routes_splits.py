"""
Payment Splits API - Revenue Distribution Management System

This module provides FastAPI routes for managing payment splits and earnings
distribution in the Snapped platform. It handles split profiles, payouts tracking,
and earnings calculations with comprehensive logging and error handling.

Features:
--------
1. Split Profile Management:
   - Create/Update split profiles
   - Delete split rules
   - Profile retrieval
   - Multi-payee support

2. Payout Management:
   - Payee search
   - Payout tracking
   - Monthly earnings calculation
   - Client-specific analytics

3. Analytics & Reporting:
   - Monthly earnings aggregation
   - Client-specific reporting
   - Pull count tracking
   - Date-range filtering

Data Model:
----------
Split Structure:
- Client identification
- Payee assignments
- Split rules
- Timestamps

Security:
--------
- Bearer token authentication
- Input validation
- Error handling
- Audit logging

Dependencies:
-----------
- FastAPI: Web framework
- MongoDB: Data storage
- datetime: Date handling
- logging: Debug tracking
- bson: ObjectId handling

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException, Request
from typing import Dict, List, Optional
from datetime import datetime
from app.shared.database import async_client
import logging
from bson import ObjectId

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get collections from Payments database
payouts = async_client['Payments']['Payouts']
payees = async_client['Payments']['Payees']
splits = async_client['Payments']['Splits']

# Update the router to include the prefix
router = APIRouter(prefix="/api/payments", tags=["payments"])

@router.get("/search-payouts")
async def search_payouts(request: Request):
    """
    Search and retrieve all payout records.
    
    Fetches all payout records from the database with proper
    ObjectId conversion for JSON serialization.
    
    Args:
        request (Request): FastAPI request object with auth header
        
    Returns:
        dict: Query results containing:
            - payouts: List of payout records with:
                - _id: Converted to string
                - All payout fields
                
    Raises:
        HTTPException: For auth or database errors
        
    Notes:
        - Requires bearer token authentication
        - Converts MongoDB ObjectIds to strings
        - Logs operation details
    """
    try:
        # Get bearer token from Authorization header
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise HTTPException(status_code=401, detail="Missing or invalid token")
            
        logger.info("Fetching all payouts")
        cursor = payouts.find({})
        payout_list = await cursor.to_list(length=None)
        
        # Convert ObjectId to string for JSON serialization
        for payout in payout_list:
            if '_id' in payout:
                payout['_id'] = str(payout['_id'])
                
        logger.info(f"Found {len(payout_list)} payouts")
        return {"payouts": payout_list}
    except Exception as e:
        logger.error(f"Error in search_payouts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/search-payees")
async def search_payees(request: Request):
    """
    Retrieve all registered payee records.
    
    Fetches all payee records from the database with proper
    ObjectId conversion for JSON serialization.
    
    Args:
        request (Request): FastAPI request object with auth header
        
    Returns:
        dict: Query results containing:
            - payees: List of payee records with:
                - _id: Converted to string
                - All payee fields
                
    Raises:
        HTTPException: For auth or database errors
        
    Notes:
        - Requires bearer token authentication
        - Converts MongoDB ObjectIds to strings
        - Logs operation details
    """
    try:
        # Get bearer token from Authorization header
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise HTTPException(status_code=401, detail="Missing or invalid token")
            
        logger.info("Fetching all payees")
        cursor = payees.find({})
        payee_list = await cursor.to_list(length=None)
        
        # Convert ObjectId to string
        for payee in payee_list:
            if '_id' in payee:
                payee['_id'] = str(payee['_id'])
                
        logger.info(f"Found {len(payee_list)} payees")
        return {"payees": payee_list}
    except Exception as e:
        logger.error(f"Error in search_payees: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/split-profile/{client_id}")
async def get_split_profile(client_id: str, request: Request):
    """
    Retrieve split profile for a specific client.
    
    Fetches the revenue split configuration for a given client,
    including all payee assignments and split rules.
    
    Args:
        client_id (str): Client identifier
        request (Request): FastAPI request object with auth header
        
    Returns:
        dict: Split profile containing:
            - _id: Profile identifier (if exists)
            - splits: List of split rules (empty if none)
            - Other profile fields
            
    Raises:
        HTTPException: For auth or database errors
        
    Notes:
        - Requires bearer token authentication
        - Handles undefined client_id gracefully
        - Returns empty splits array if no profile exists
        - Converts MongoDB ObjectId to string
    """
    try:
        # Get bearer token from Authorization header
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise HTTPException(status_code=401, detail="Missing or invalid token")
            
        logger.info(f"Getting split profile for client_id: {client_id}")
        
        # Validate client_id
        if not client_id or client_id == 'undefined':
            logger.error("Invalid client_id provided")
            return {"splits": []}
            
        split_doc = await splits.find_one({"client_id": client_id})
        logger.info(f"Found split document: {split_doc}")
        
        if split_doc:
            # Convert ObjectId to string
            if '_id' in split_doc:
                split_doc['_id'] = str(split_doc['_id'])
            # Ensure splits array exists
            if 'splits' not in split_doc:
                split_doc['splits'] = []
            return split_doc
        
        logger.info(f"No splits found for client_id: {client_id}")
        return {"splits": []}
        
    except Exception as e:
        logger.error(f"Error in get_split_profile: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/split-profile")
async def save_split_profile(request: Request):
    """
    Create or update a client's split profile.
    
    Saves or updates the complete split configuration for a client,
    replacing any existing split rules.
    
    Args:
        request (Request): FastAPI request object containing:
            - clientId: Client identifier
            - splits: Array of split rules
            
    Returns:
        dict: Operation result with:
            - success: Boolean indicator
            - message: Status description
            
    Raises:
        HTTPException: For validation, auth, or database errors
        
    Notes:
        - Requires bearer token authentication
        - Validates required fields
        - Updates timestamp automatically
        - Replaces entire splits array
        - Uses upsert for atomic operation
    """
    try:
        # Get bearer token from Authorization header
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise HTTPException(status_code=401, detail="Missing or invalid token")
            
        split_data = await request.json()
        logger.info(f"Received split data: {split_data}")
        
        # Validate required fields
        if not split_data.get("clientId"):
            raise HTTPException(status_code=400, detail="clientId is required")
        if not split_data.get("splits") or not isinstance(split_data["splits"], list):
            raise HTTPException(status_code=400, detail="splits array is required")
            
        # Create or update the split document
        split_doc = {
            "client_id": split_data["clientId"],
            "splits": split_data["splits"],  # Replace entire splits array
            "updated_at": datetime.utcnow()
        }
        
        logger.info(f"Saving split document: {split_doc}")
        
        # Replace the entire document
        result = await splits.replace_one(
            {"client_id": split_data["clientId"]},
            split_doc,
            upsert=True
        )
        
        return {"success": True, "message": "Splits saved successfully"}
        
    except ValueError as e:
        logger.error(f"Invalid JSON in request body: {str(e)}")
        raise HTTPException(status_code=400, detail="Invalid JSON in request body")
    except Exception as e:
        logger.error(f"Error in save_split_profile: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/split-profile/{client_id}/{payee_id}")
async def delete_split_profile(client_id: str, payee_id: str, request: Request):
    """
    Delete a specific split rule from a client's profile.
    
    Removes a single payee's split configuration from a
    client's split profile.
    
    Args:
        client_id (str): Client identifier
        payee_id (str): Payee identifier to remove
        request (Request): FastAPI request object with auth header
        
    Returns:
        dict: Operation result with:
            - success: Boolean indicator
            
    Raises:
        HTTPException: For auth or database errors
        
    Notes:
        - Requires bearer token authentication
        - Uses atomic $pull operation
        - Maintains other split rules
        - Logs operation details
    """
    try:
        # Get bearer token from Authorization header
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise HTTPException(status_code=401, detail="Missing or invalid token")
            
        logger.info(f"Deleting split for client_id: {client_id}, payee_id: {payee_id}")
        result = await splits.update_one(
            {"client_id": client_id},
            {"$pull": {"splits": {"payeeId": payee_id}}}
        )
        return {"success": True}
    except Exception as e:
        logger.error(f"Error in delete_split_profile: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/monthly-earnings")
async def get_monthly_earnings(request: Request):
    """
    Calculate total monthly earnings across all clients.
    
    Aggregates earnings data for the current month across all
    clients, providing detailed breakdown and totals.
    
    Args:
        request (Request): FastAPI request object with auth header
        
    Returns:
        dict: Monthly earnings data containing:
            - status: Operation status
            - total_earnings: Aggregate earnings
            - total_pulls: Total pull count
            - month: Month identifier (YYYY-MM)
            - client_breakdown: Per-client details
            - start_date: Period start
            - end_date: Period end
            
    Raises:
        HTTPException: For auth or database errors
        
    Notes:
        - Requires bearer token authentication
        - Uses MongoDB aggregation pipeline
        - Handles month transitions
        - Provides detailed client breakdown
        - Includes date range metadata
    """
    try:
        # Get bearer token from Authorization header
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise HTTPException(status_code=401, detail="Missing or invalid token")
            
        logger.info("Fetching monthly earnings for all clients")
        
        # Get current month's start and end dates
        now = datetime.utcnow()
        logger.info(f"Current time: {now}")
        
        # Start from beginning of month
        start_date = datetime(now.year, now.month, 1)
        # End at beginning of next month
        if now.month == 12:
            end_date = datetime(now.year + 1, 1, 1)
        else:
            end_date = datetime(now.year, now.month + 1, 1)
        
        logger.info(f"Fetching earnings between {start_date} and {end_date}")
        
        # Find all payouts with pulls in the current month
        pipeline = [
            {
                "$match": {
                    "creator_pulls": {
                        "$elemMatch": {
                            "pull_date": {
                                "$gte": start_date,
                                "$lt": end_date
                            }
                        }
                    }
                }
            },
            {
                "$project": {
                    "client_id": 1,
                    "payout_email": 1,
                    "monthly_pulls": {
                        "$filter": {
                            "input": "$creator_pulls",
                            "as": "pull",
                            "cond": {
                                "$and": [
                                    {"$gte": ["$$pull.pull_date", start_date]},
                                    {"$lt": ["$$pull.pull_date", end_date]}
                                ]
                            }
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "monthly_earnings": {
                        "$sum": {
                            "$map": {
                                "input": "$monthly_pulls",
                                "as": "pull",
                                "in": {"$toDouble": "$$pull.pull_amount"}
                            }
                        }
                    },
                    "total_pulls": {"$size": "$monthly_pulls"}
                }
            }
        ]
        
        cursor = payouts.aggregate(pipeline)
        payout_list = await cursor.to_list(length=None)
        logger.info(f"Found {len(payout_list)} payout documents with pulls this month")
        
        # Calculate totals
        total_earnings = sum(doc.get('monthly_earnings', 0) for doc in payout_list)
        total_pulls = sum(doc.get('total_pulls', 0) for doc in payout_list)
        
        # Get detailed breakdown by client
        client_breakdown = {
            doc['client_id']: {
                'email': doc['payout_email'],
                'earnings': doc['monthly_earnings'],
                'pulls': doc['total_pulls']
            }
            for doc in payout_list
        }
        
        logger.info(f"Final totals: earnings={total_earnings}, pulls={total_pulls}")
        
        response_data = {
            "status": "success",
            "total_earnings": total_earnings,
            "total_pulls": total_pulls,
            "month": start_date.strftime('%Y-%m'),
            "client_breakdown": client_breakdown,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }
        
        logger.info(f"Returning response: {response_data}")
        return response_data
        
    except Exception as e:
        logger.error(f"Error in get_monthly_earnings: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/client-monthly-earnings/{client_id}")
async def get_client_monthly_earnings(client_id: str, request: Request):
    """
    Calculate monthly earnings for a specific client.
    
    Aggregates earnings data for the current month for a
    single client, providing detailed statistics.
    
    Args:
        client_id (str): Client identifier
        request (Request): FastAPI request object with auth header
        
    Returns:
        dict: Client earnings data containing:
            - status: Operation status
            - total_earnings: Monthly earnings
            - total_pulls: Pull count
            - month: Month identifier (YYYY-MM)
            - start_date: Period start
            - end_date: Period end
            
    Raises:
        HTTPException: For auth or database errors
        
    Notes:
        - Requires bearer token authentication
        - Uses MongoDB aggregation pipeline
        - Handles month transitions
        - Returns zero totals if no data
        - Includes date range metadata
    """
    try:
        # Get bearer token from Authorization header
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise HTTPException(status_code=401, detail="Missing or invalid token")
            
        logger.info(f"Fetching monthly earnings for client: {client_id}")
        
        # Get current month's start and end dates
        now = datetime.utcnow()
        logger.info(f"Current time: {now}")
        
        # Start from beginning of month
        start_date = datetime(now.year, now.month, 1)
        # End at beginning of next month
        if now.month == 12:
            end_date = datetime(now.year + 1, 1, 1)
        else:
            end_date = datetime(now.year, now.month + 1, 1)
            
        logger.info(f"Fetching earnings between {start_date} and {end_date}")
        
        # Find payout document with pulls in the current month for this client
        pipeline = [
            {
                "$match": {
                    "client_id": client_id,
                    "creator_pulls": {
                        "$elemMatch": {
                            "pull_date": {
                                "$gte": start_date,
                                "$lt": end_date
                            }
                        }
                    }
                }
            },
            {
                "$project": {
                    "client_id": 1,
                    "payout_email": 1,
                    "monthly_pulls": {
                        "$filter": {
                            "input": "$creator_pulls",
                            "as": "pull",
                            "cond": {
                                "$and": [
                                    {"$gte": ["$$pull.pull_date", start_date]},
                                    {"$lt": ["$$pull.pull_date", end_date]}
                                ]
                            }
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "monthly_earnings": {
                        "$sum": {
                            "$map": {
                                "input": "$monthly_pulls",
                                "as": "pull",
                                "in": {"$toDouble": "$$pull.pull_amount"}
                            }
                        }
                    },
                    "total_pulls": {"$size": "$monthly_pulls"}
                }
            }
        ]
        
        cursor = payouts.aggregate(pipeline)
        payout_doc = await cursor.to_list(length=1)
        
        if not payout_doc:
            logger.info(f"No payouts found for client {client_id} in current month")
            return {
                "status": "success",
                "total_earnings": 0,
                "total_pulls": 0,
                "month": start_date.strftime('%Y-%m'),
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            }
            
        payout_doc = payout_doc[0]
        logger.info(f"Found payout document with {payout_doc.get('total_pulls', 0)} pulls this month")
        
        response_data = {
            "status": "success",
            "total_earnings": payout_doc.get('monthly_earnings', 0),
            "total_pulls": payout_doc.get('total_pulls', 0),
            "month": start_date.strftime('%Y-%m'),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat()
        }
        
        logger.info(f"Returning response: {response_data}")
        return response_data
        
    except Exception as e:
        logger.error(f"Error in get_client_monthly_earnings: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 
