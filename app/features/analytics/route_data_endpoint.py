"""
Analytics Data Endpoint Router Module

This module provides specialized data retrieval endpoints for the analytics system,
focusing on granular data access and metric calculations for Snapchat analytics.

System Architecture:
    - Data Access Layer:
        * MongoDB Collections:
            - content_data: Analytics metrics and session data
            - ClientInfo: Client profile and metadata
        * Query Patterns:
            - Date-based filtering
            - Client-specific queries
            - Aggregation pipelines
    
    - Authentication Integration:
        * Uses shared auth module
        * Role-based access control
        * Query filtering based on permissions
    
    - Error Handling:
        * Consistent error responses
        * Detailed logging
        * Graceful fallbacks for missing data

Data Models:
    1. Session Data:
        - Date (MM-DD-YYYY format)
        - Metrics:
            * Views (story_views, impressions, reach)
            * Engagement (followers_added, followers_lost)
            * Time metrics (story_view_time)
    
    2. Client Data:
        - client_id/user_id mapping
        - Platform information
        - Analytics availability

Integration Points:
    - Frontend: Provides data for dashboard visualizations
    - Auth System: Enforces access control
    - Logging: Detailed operation tracking
    - Error Handling: Consistent error responses
"""

from fastapi import APIRouter, HTTPException, Depends, Body
from app.shared.database import async_client
import logging
from app.shared.auth import get_current_user_group, get_filtered_query
from bson.json_util import dumps
import json

# Initialize FastAPI router with analytics prefix
router = APIRouter(prefix="/api/analytics")

# Configure logging for analytics operations
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Collections
content_data_collection = async_client['ClientDb']['content_data']
client_info_collection = async_client['ClientDb']['ClientInfo']

@router.get("/snapchat")
async def get_snapchat_analytics(
    client_id: str,
    start_date: str,
    end_date: str,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Retrieves Snapchat analytics data for a specified date range.
    
    Data Flow:
        1. Authentication and permission validation
        2. Query construction with date range filters
        3. MongoDB document retrieval
        4. Response formatting with BSON handling
    
    Security:
        - User permission verification
        - Query filtering based on auth data
        - Sanitized response formatting
    
    Args:
        client_id: Target client identifier
        start_date: Range start (MM-DD-YYYY)
        end_date: Range end (MM-DD-YYYY)
        auth_data: User authentication context
    
    Returns:
        JSON response with analytics data or error message
    """
    try:
        # Get filtered query based on user's permissions
        filter_query = await get_filtered_query(auth_data)
        
        # Debug log the incoming request
        logger.info(f"Analytics request - client_id: {client_id}, start: {start_date}, end: {end_date}")
        
        # Add client, platform and date range filters
        filter_query.update({
            "client_id": client_id,
            "platform": "snapchat",
            "sessions": {
                "$elemMatch": {
                    "date": {
                        "$gte": start_date,
                        "$lte": end_date
                    }
                }
            }
        })
        
        # Debug log the query
        logger.info(f"MongoDB query: {filter_query}")
        
        # Get analytics data
        analytics = await content_data_collection.find_one(filter_query)
        
        # Debug log what we found
        logger.info(f"Found analytics: {analytics is not None}")
        if analytics:
            logger.info(f"Number of sessions: {len(analytics.get('sessions', []))}")
            logger.info(f"Sample session: {analytics.get('sessions', [])[0] if analytics.get('sessions') else 'No sessions'}")
        
        if not analytics:
            return {
                "status": "error",
                "message": "No data found"
            }
            
        return {
            "status": "success",
            "data": json.loads(dumps(analytics))
        }
        
    except Exception as e:
        logger.error(f"Error in get_snapchat_analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/clients")
async def get_analytics_clients(
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Retrieves list of clients with available analytics data.
    
    Pipeline Stages:
        1. Match authorized clients (filter_query)
        2. Group by client_id
        3. Project relevant fields:
            - client_id
            - username
            - platform
    
    Security:
        - Filtered based on user permissions
        - Minimal data exposure
    
    Returns:
        List of clients with their associated platforms and usernames
    """
    try:
        # Get filtered query based on user's permissions
        filter_query = await get_filtered_query(auth_data)
        
        # Find all unique clients with analytics data
        pipeline = [
            {"$match": filter_query},
            {"$group": {
                "_id": "$client_id",
                "username": {"$first": "$username"},
                "platform": {"$first": "$platform"}
            }},
            {"$project": {
                "_id": 0,
                "client_id": "$_id",
                "username": 1,
                "platform": 1
            }}
        ]
        
        cursor = content_data_collection.aggregate(pipeline)
        clients = await cursor.to_list(length=None)
        
        return {
            "status": "success",
            "data": clients
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/snapchat/single")
async def get_snapchat_analytics_single(
    client_id: str,
    date: str,  # YYYY-MM-DD format
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Retrieves Snapchat analytics for a specific date.
    
    Data Processing:
        1. Date format conversion (YYYY-MM-DD → MM-DD-YYYY)
        2. Client identification (client_id/user_id)
        3. Session matching and metric extraction
    
    Error Handling:
        - Returns 0 for missing data
        - Logs all operations
        - Graceful error recovery
    
    Args:
        client_id: Client identifier
        date: Target date (YYYY-MM-DD)
        auth_data: User authentication context
    
    Returns:
        Story views count or 0 if no data found
    """
    try:
        logger.info(f"Single analytics request - client_id: {client_id}, date: {date}")
        
        # Convert date from YYYY-MM-DD to MM-DD-YYYY
        year, month, day = date.split('-')
        mongo_date = f"{month}-{day}-{year}"  # Convert to format in MongoDB
        
        logger.info(f"Converted date: {mongo_date}")
        
        filter_query = await get_filtered_query(auth_data)
        
        query = {
            "$or": [
                {"client_id": client_id},
                {"user_id": client_id}
            ],
            "platform": "snapchat"
        }
        
        logger.info(f"MongoDB Query: {query}")
        
        if filter_query:
            query.update(filter_query)

        result = await content_data_collection.find_one(query)
        
        logger.info(f"MongoDB Result: {result}")
        
        if result and result.get("sessions"):
            # Find session with matching date using converted format
            matching_session = next((s for s in result["sessions"] if s["date"] == mongo_date), None)
            
            logger.info(f"Matching session found: {matching_session is not None}")
            
            if matching_session:
                return {
                    "story_views": matching_session.get("metrics", {}).get("views", {}).get("story_views", 0)
                }
            
        return {"story_views": 0}
        
    except Exception as e:
        logger.error(f"Error getting story views: {str(e)}")
        return {"story_views": 0}  # Return 0 instead of error

@router.get("/snapchat/metrics")
async def get_snapchat_metrics(
    client_id: str,
    date: str,  # YYYY-MM-DD format
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Retrieves comprehensive Snapchat metrics for a specific date.
    
    Metrics Calculated:
        - Story Views: Total story view count
        - Impressions: Content impression count
        - Follower Change: Net follower gain/loss
        - Reach: Unique viewer count
        - Story View Time: Total viewing duration
    
    Data Processing:
        1. Date format conversion
        2. Session lookup
        3. Metric extraction and calculation
        4. Default handling for missing data
    
    Returns:
        Dictionary of calculated metrics with 0 defaults
    """
    try:
        # Convert date from YYYY-MM-DD to MM-DD-YYYY
        year, month, day = date.split('-')
        mongo_date = f"{month}-{day}-{year}"  # Convert to format in MongoDB
        
        logger.info(f"Date conversion:")
        logger.info(f"  Input date: {date}")
        logger.info(f"  Converted date: {mongo_date}")
        
        filter_query = await get_filtered_query(auth_data)
        
        query = {
            "$or": [
                {"client_id": client_id},
                {"user_id": client_id}
            ],
            "platform": "snapchat"
        }
        
        if filter_query:
            query.update(filter_query)

        result = await content_data_collection.find_one(query)
        
        if result and result.get("sessions"):
            logger.info(f"Found sessions: {result['sessions']}")
            # Find session with matching date using converted format
            matching_session = next((s for s in result["sessions"] if s["date"] == mongo_date), None)
            logger.info(f"Matching session: {matching_session}")
            
            if matching_session:
                # Calculate follower change as followers_added - followers_lost
                engagement = matching_session.get("metrics", {}).get("engagement", {})
                follower_change = (
                    engagement.get("followers_added", 0) - 
                    engagement.get("followers_lost", 0)
                )
                
                metrics = {
                    "story_views": matching_session.get("metrics", {}).get("views", {}).get("story_views", 0),
                    "impressions": matching_session.get("metrics", {}).get("views", {}).get("impressions", 0),
                    "follower_change": follower_change,  # Use calculated value
                    "reach": matching_session.get("metrics", {}).get("views", {}).get("reach", 0),
                    "story_view_time": matching_session.get("metrics", {}).get("time_metrics", {}).get("story_view_time", 0)
                }
                return metrics
            
        # Default values if no matching session found
        return {
            "story_views": 0,
            "impressions": 0,
            "follower_change": 0,
            "reach": 0,
            "story_view_time": 0
        }
        
    except Exception as e:
        logger.error(f"Error getting metrics: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get metrics: {str(e)}"
        )

@router.delete("/content-notes/delete-note/{client_id}/{session_folder}")
async def delete_note(
    client_id: str,
    session_folder: str,
    note_data: dict = Body(...),
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Deletes a specific content note from a session.
    
    Operation Flow:
        1. Permission validation
        2. Note identification
        3. Atomic removal operation
        4. Result verification
    
    Security:
        - User permission verification
        - Filtered queries
        - Atomic operations
    
    Args:
        client_id: Target client
        session_folder: Session identifier
        note_data: Note metadata for deletion
        auth_data: User authentication context
    
    Returns:
        Success confirmation or error details
    """
    try:
        # Get filtered query based on user's permissions
        filter_query = await get_filtered_query(auth_data)
        
        logger.info(f"Deleting note for client {client_id} in session {session_folder}")
        logger.info(f"Note data: {note_data}")
        
        # Add client and session folder filters
        filter_query.update({
            "client_id": client_id,
            "session_folder": session_folder
        })
        
        # Remove the note from the notes array
        result = await content_data_collection.update_one(
            filter_query,
            {
                "$pull": {
                    "notes": {
                        "created_at": note_data.get("created_at"),
                        "note": note_data.get("note")
                    }
                }
            }
        )
        
        if result.modified_count > 0:
            return {"status": "success", "message": "Note deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Note not found")
            
    except Exception as e:
        logger.error(f"Error deleting note: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
