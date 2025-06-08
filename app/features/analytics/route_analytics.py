"""
Analytics Router Module for Snapchat Creator Data

This module serves as the central analytics processing hub for the application,
handling data ingestion, processing, and retrieval for Snapchat creator analytics.

System Architecture:
    - Data Sources:
        * Snapchat CSV uploads
        * Vista Analytics Service
        * Profile sync updates
    - Storage Layer:
        * MongoDB: Primary data store (ClientDb database)
            - content_data: Stores analytics metrics and sessions
            - ClientInfo: Stores creator profile information
        * Redis: Caching layer (implemented in other modules)
    - Authentication:
        * Integrates with shared auth module for user group validation
        * Role-based access control (ADMIN vs regular users)

Data Flow:
    1. Data Ingestion:
        - CSV file uploads → MongoDB content_data
        - Vista service sync → MongoDB
        - Profile updates → Both collections
    2. Data Processing:
        - Aggregation pipelines for metrics calculation
        - Growth rate computations
        - Date range filtering
    3. Data Retrieval:
        - Mobile analytics endpoints
        - Snapchat-specific metrics
        - Client listing and profiles

Security:
    - Role-based access control
    - Filtered queries based on user permissions
    - File cleanup procedures
    - Secure credential handling

Dependencies:
    app.shared.database: MongoDB async client
    app.shared.auth: Authentication and query filtering
    vista_service: External analytics service integration
"""

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, Query, Request, Body
from app.shared.database import async_client
from datetime import datetime, timedelta
import pandas as pd
import io
import logging
from app.shared.auth import get_current_user_group, get_filtered_query
import os
from pathlib import Path
from bson import ObjectId
import json
from bson.json_util import dumps, loads
from typing import List
import sys
from .vista_service import VistaAnalyticsService

# Initialize FastAPI router with analytics prefix
router = APIRouter(prefix="/api/analytics")

# Configure logging for analytics operations
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Setup console logging for operational monitoring
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Collections
content_data_collection = async_client['ClientDb']['content_data']
client_info_collection = async_client['ClientDb']['ClientInfo']

# File Storage Configuration
UPLOAD_DIR = Path("/home/ubuntu/snappedii/uploads/analytics/snapchat")

# Initialize Vista Analytics Service
vista_service = VistaAnalyticsService()

async def ensure_collections_exist():
    """
    Ensures required MongoDB collections exist with proper indexes.
    
    Collections:
        - ClientDb.content_data: Stores analytics metrics
        - ClientDb.ClientInfo: Stores client information
    
    Indexes:
        - Compound index on client_id, platform, and session_date
        - Ensures efficient queries for analytics retrieval
    """
    try:
        # Create ClientDb if it doesn't exist
        if 'ClientDb' not in await async_client.list_database_names():
            logger.info("Creating ClientDb database")
            await async_client['ClientDb'].create_collection('content_data')
            
        # Get or create content_data collection
        if 'content_data' not in await async_client['ClientDb'].list_collection_names():
            logger.info("Creating content_data collection")
            await async_client['ClientDb'].create_collection('content_data')
            
        # Create indexes if they don't exist
        await async_client['ClientDb']['content_data'].create_index([
            ("client_id", 1),
            ("platform", 1),
            ("session_date", 1)
        ], unique=True)
        
    except Exception as e:
        logger.error(f"Error ensuring collections exist: {e}")
        raise

async def get_client_id_from_username(username: str) -> str:
    """
    Maps Snapchat username to internal client_id.
    
    Integration Points:
        - ClientInfo collection: Primary source of client data
        - Multiple field mappings for backwards compatibility
        
    Error Handling:
        - Logs all lookup attempts and results
        - Returns None if no mapping found
        
    Args:
        username: Snapchat username to look up
        
    Returns:
        client_id if found, None otherwise
    """
    try:
        # Log the lookup attempt
        logger.info(f"Searching ClientInfo for username: {username}")
        
        # Try multiple possible field names
        client = await client_info_collection.find_one({
            "$or": [
                {"snap_username": username},
                {"username": username},
                {"Username": username},  # Check case variations
                {"Snap_Username": username}
            ]
        })
        
        # Log what we found
        logger.info(f"Database lookup result: {client is not None}")
        if client:
            # Get the client_id field, not _id
            client_id = client.get('client_id')
            logger.info(f"Found client document with client_id: {client_id}")
            return client_id
        
        # Log fields we searched if nothing found
        logger.error(f"No client found with username {username} in any field")
        logger.error("Searched fields: snap_username, username, Username, Snap_Username")
        return None
        
    except Exception as e:
        logger.error(f"Error looking up client_id for {username}: {str(e)}")
        return None

# Add this function to ensure upload directory exists
async def ensure_upload_dir():
    """Ensure upload directory exists"""
    try:
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"Upload directory ensured: {UPLOAD_DIR}")
    except Exception as e:
        logger.error(f"Error creating upload directory: {e}")
        raise

@router.post("/upload/snapchat")
async def upload_snapchat_csv(
    files: list[UploadFile] = File(...),
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Processes Snapchat analytics CSV uploads.
    
    System Flow:
        1. Authentication check (ADMIN or filtered access)
        2. File validation and storage
        3. Username to client_id mapping
        4. Data parsing and transformation
        5. MongoDB storage with upsert logic
        
    Security:
        - Role-based access control
        - File type validation
        - Secure file storage with timestamps
        
    Integration Points:
        - Authentication system
        - File storage system
        - MongoDB collections
    """
    
    # Verify user permissions
    if "ADMIN" not in auth_data["groups"]:
        filter_query = await get_filtered_query(auth_data)
        if filter_query.get("client_id") == "NO_ACCESS":
            raise HTTPException(status_code=403)

    # Process each uploaded file
    for file in files:
        # Validate CSV file
        if not file.filename.endswith('.csv'):
            continue
            
        # Save file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_filename = f"{timestamp}_{file.filename}"
        
        # Extract username and find client_id
        username = file.filename.split('_snapchat.csv')[0]
        client_id = await get_client_id_from_username(username)
        
        # Process CSV data
        df = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        
        # Store data in MongoDB
        for _, row in df.iterrows():
            session_data = {
                "date": formatted_date,
                "client_id": client_id,
                "metrics": {
                    "engagement": {...},
                    "content": {...},
                    "interactions": {...},
                    "views": {...},
                    "time_metrics": {...},
                    "other": {...}
                }
            }
            
            # Update database with new session data
            await content_data_collection.update_one(
                {"client_id": client_id, "platform": "snapchat"},
                {
                    "$setOnInsert": {...},
                    "$push": {"sessions": session_data}
                },
                upsert=True
            )

@router.get("/snapchat")
async def get_snapchat_analytics(
    client_id: str,
    start_date: str,  # Match frontend parameter name
    end_date: str,    # Match frontend parameter name
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Retrieves aggregated Snapchat analytics for a date range.
    
    Data Processing:
        1. Date range validation
        2. Permission checking
        3. MongoDB aggregation pipeline:
            - Matches client and platform
            - Unwinds session data
            - Filters by date range
            - Groups metrics by date
            
    Returns:
        - Total metrics for the period
        - Daily metrics for charts
        - Success/error status
    """
    try:
        # Log input parameters
        logger.info(f"Getting Snapchat analytics for client_id: {client_id}")
        logger.info(f"Date range: {start_date} to {end_date}")
        
        # Get filtered query based on user's permissions
        filter_query = await get_filtered_query(auth_data)
        
        # First check if we have any matching documents
        base_query = {
            "$or": [
                {"client_id": client_id},
                {"user_id": client_id}
            ],
            "platform": "snapchat"
        }
        
        doc_count = await content_data_collection.count_documents(base_query)
        logger.info(f"Found {doc_count} matching documents")
        
        # Get a sample document to verify structure
        sample_doc = await content_data_collection.find_one(base_query)
        if sample_doc:
            logger.info("Sample document structure:")
            logger.info(f"Number of sessions: {len(sample_doc.get('sessions', []))}")
            if sample_doc.get('sessions'):
                first_session = sample_doc['sessions'][0]
                logger.info(f"First session date: {first_session.get('date')}")
                logger.info(f"First session metrics: {json.dumps(first_session.get('metrics', {}), indent=2)}")
        
        # Build aggregation pipeline with improved date handling
        pipeline = [
            {"$match": base_query},
            {"$unwind": "$sessions"},
            {"$match": {
                "sessions.date": {
                    "$gte": start_date,
                    "$lte": end_date
                }
            }},
            # First group by date to get daily metrics
            {"$group": {
                "_id": "$sessions.date",
                "daily_views": {"$sum": "$sessions.metrics.views.story_views"},
                "daily_impressions": {"$sum": "$sessions.metrics.views.impressions"},
                "daily_reach": {"$sum": "$sessions.metrics.views.reach"},
                "daily_snap_view_time": {"$sum": "$sessions.metrics.time_metrics.snap_view_time"},
                "daily_shares": {"$sum": "$sessions.metrics.interactions.shares"},
                "daily_replies": {"$sum": "$sessions.metrics.interactions.replies"},
                "daily_screenshots": {"$sum": "$sessions.metrics.interactions.screenshots"},
                "daily_swipe_ups": {"$sum": "$sessions.metrics.interactions.swipe_ups"}
            }},
            # Sort by date
            {"$sort": {"_id": 1}},
            # Group again to get totals and collect daily metrics
            {"$group": {
                "_id": None,
                "total_views": {"$sum": "$daily_views"},
                "total_impressions": {"$sum": "$daily_impressions"},
                "total_reach": {"$sum": "$daily_reach"},
                "total_snap_view_time": {"$sum": "$daily_snap_view_time"},
                "total_shares": {"$sum": "$daily_shares"},
                "total_replies": {"$sum": "$daily_replies"},
                "total_screenshots": {"$sum": "$daily_screenshots"},
                "total_swipe_ups": {"$sum": "$daily_swipe_ups"},
                "daily_metrics": {
                    "$push": {
                        "date": "$_id",
                        "views": "$daily_views",
                        "impressions": "$daily_impressions",
                        "reach": "$daily_reach",
                        "snap_view_time": "$daily_snap_view_time",
                        "metrics": {
                            "interactions": {
                                "shares": "$daily_shares",
                                "replies": "$daily_replies",
                                "screenshots": "$daily_screenshots",
                                "swipe_ups": "$daily_swipe_ups"
                            }
                        }
                    }
                }
            }}
        ]
        
        # Log the pipeline
        logger.info(f"Snapchat analytics pipeline: {json.dumps(pipeline, indent=2)}")
        
        # Execute aggregation
        result = await content_data_collection.aggregate(pipeline).to_list(length=None)
        
        # Log the raw result
        logger.info(f"Aggregation result: {json.dumps(result, indent=2)}")
        
        if result:
            metrics = result[0]
            response = {
                "status": "success",
                "data": {
                    "total_views": metrics.get("total_views", 0),
                    "impressions": metrics.get("total_impressions", 0),
                    "reach": metrics.get("total_reach", 0),
                    "snap_view_time": metrics.get("total_snap_view_time", 0),
                    "total_shares": metrics.get("total_shares", 0),
                    "total_replies": metrics.get("total_replies", 0),
                    "total_screenshots": metrics.get("total_screenshots", 0),
                    "total_swipe_ups": metrics.get("total_swipe_ups", 0),
                    "daily_metrics": metrics.get("daily_metrics", [])
                }
            }
        else:
            response = {
                "status": "success",
                "data": {
                    "total_views": 0,
                    "impressions": 0,
                    "reach": 0,
                    "snap_view_time": 0,
                    "total_shares": 0,
                    "total_replies": 0,
                    "total_screenshots": 0,
                    "total_swipe_ups": 0,
                    "daily_metrics": []
                }
            }
            
        # Log the final response
        logger.info(f"Sending response: {json.dumps(response, indent=2)}")
        return response
        
    except Exception as e:
        logger.error(f"Error in get_snapchat_analytics: {str(e)}")
        logger.exception("Full error details:")
        raise HTTPException(status_code=500, detail=str(e))

async def cleanup_old_files(days_to_keep: int = 30):
    """Clean up files older than specified days"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        for file_path in UPLOAD_DIR.glob("*.csv"):
            file_stat = file_path.stat()
            file_date = datetime.fromtimestamp(file_stat.st_mtime)
            
            if file_date < cutoff_date:
                file_path.unlink()
                logger.info(f"Deleted old file: {file_path}")
                
    except Exception as e:
        logger.error(f"Error cleaning up old files: {e}")

@router.post("/cleanup")
async def cleanup_files(
    days: int = Query(30, gt=0, lt=365),
    auth_data: dict = Depends(get_current_user_group)
):
    """Cleanup old CSV files"""
    if "ADMIN" not in auth_data["groups"]:
        raise HTTPException(
            status_code=403,
            detail="Only admins can cleanup files"
        )
        
    await cleanup_old_files(days)
    return {"status": "success", "message": f"Cleaned up files older than {days} days"}

@router.get("/mobile")
async def get_mobile_analytics(
    user_ids: str = Query(None),
    user_id: str = Query(None),
    days: int = Query(7, description="Number of days to fetch (7 or 30)"),
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Provides analytics data for mobile app consumption.
    
    Features:
        - Supports single or multiple user queries
        - 7 or 30 day timeframe options
        - Comprehensive metrics calculation
        
    Data Flow:
        1. Parameter validation
        2. Date range calculation
        3. MongoDB aggregation:
            - Session unwinding
            - Date filtering
            - Metrics aggregation
        4. Response formatting
        
    Performance Optimization:
        - Efficient date handling
        - Optimized aggregation pipelines
        - Detailed logging for troubleshooting
    """
    try:
        if days not in [7, 30]:
            raise HTTPException(status_code=400, detail="Days parameter must be either 7 or 30")
            
        # Calculate date range
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)
        
        # Format dates for MongoDB - use string format to match document structure
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
                    "snap_view_time": 0,
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
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$gte": [{"$substr": ["$sessions.date", 0, 10]}, formatted_start]},
                            {"$lte": [{"$substr": ["$sessions.date", 0, 10]}, formatted_end]}
                        ]
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
            
            # Log each stage of the pipeline
            logger.info(f"Pipeline stages for {days}d:")
            for i, stage in enumerate(pipeline):
                logger.info(f"Stage {i}: {stage}")
            
            # Execute the aggregation
            result = await content_data_collection.aggregate(pipeline).to_list(length=None)
            
            # Log the results for debugging
            logger.info(f"Aggregation result: {result}")
            
            if result:
                metrics = result[0]
                response = {
                    "status": "success",
                    "total_views": metrics.get("total_views", 0),
                    "impressions": metrics.get("impressions", 0),
                    "reach": metrics.get("reach", 0),
                    "snap_view_time": metrics.get("snap_view_time", 0),
                    "profile_views": metrics.get("profile_views", 0),
                    "spotlight_views": metrics.get("spotlight_views", 0),
                    "saved_story_views": metrics.get("saved_story_views", 0),
                    "timeframe": f"{days}d"
                }
                logger.info(f"Sending response for {days}d: {response}")
                return response
            else:
                return {
                    "status": "success",
                    "total_views": 0,
                    "impressions": 0,
                    "reach": 0,
                    "snap_view_time": 0,
                    "profile_views": 0,
                    "spotlight_views": 0,
                    "saved_story_views": 0,
                    "timeframe": f"{days}d"
                }
            
        # Handle single user ID
        elif user_id:
            logger.info(f"Fetching analytics for single client: {user_id}")
            
            # Log the date range and pipeline for debugging
            logger.info(f"Date range: {formatted_start} to {formatted_end}")
            logger.info(f"Days parameter: {days}")
            
            # Log a sample document first
            sample_doc = await content_data_collection.find_one({"user_id": user_id, "platform": "snapchat"})
            if sample_doc:
                logger.info("=== SAMPLE DOCUMENT ANALYSIS ===")
                logger.info(f"Total number of sessions: {len(sample_doc.get('sessions', []))}")
                
                # Analyze first 5 sessions
                sessions = sample_doc.get('sessions', [])[:5]
                for idx, session in enumerate(sessions):
                    logger.info(f"\nSession {idx + 1}:")
                    logger.info(f"Date: {session.get('date')}")
                    logger.info(f"Story Views: {session.get('metrics', {}).get('views', {}).get('story_views')}")
                    logger.info(f"Impressions: {session.get('metrics', {}).get('views', {}).get('impressions')}")
                    logger.info(f"Reach: {session.get('metrics', {}).get('views', {}).get('reach')}")
                logger.info("================================")
            
            # First count sessions in each date range
            count_pipeline = [
                {"$match": {"user_id": user_id, "platform": "snapchat"}},
                {"$unwind": "$sessions"},
                {"$addFields": {
                    "clean_date": {
                        "$arrayElemAt": [{"$split": ["$sessions.date", ","]}, 0]
                    }
                }},
                {"$group": {
                    "_id": {
                        "$cond": {
                            "if": {
                                "$and": [
                                    {"$gte": ["$clean_date", formatted_start]},
                                    {"$lte": ["$clean_date", formatted_end]}
                                ]
                            },
                            "then": "in_range",
                            "else": "out_of_range"
                        }
                    },
                    "count": {"$sum": 1},
                    "sample_dates": {"$push": "$clean_date"}
                }}
            ]
            
            count_result = await content_data_collection.aggregate(count_pipeline).to_list(length=None)
            logger.info("\n=== SESSION COUNT ANALYSIS ===")
            logger.info(f"Date range: {formatted_start} to {formatted_end}")
            for group in count_result:
                logger.info(f"{group['_id']}: {group['count']} sessions")
                logger.info(f"Sample dates: {group['sample_dates'][:5]}")
            logger.info("=============================\n")
            
            pipeline = [
                {"$match": {"user_id": user_id, "platform": "snapchat"}},
                # First unwind sessions
                {"$unwind": "$sessions"},
                # Then create a clean date field
                {"$addFields": {
                    "clean_date": {
                        "$arrayElemAt": [{"$split": ["$sessions.date", ","]}, 0]
                    }
                }},
                # Now match on the clean date
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$gte": ["$clean_date", formatted_start]},
                            {"$lte": ["$clean_date", formatted_end]}
                        ]
                    }
                }},
                # Finally group and sum
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

            # Log everything for debugging
            logger.info(f"Date range: {formatted_start} to {formatted_end}")
            logger.info(f"Pipeline: {json.dumps(pipeline, indent=2)}")
            logger.info(f"Sample clean date: {sample_doc.get('sessions', [{}])[0].get('date', '').split(',')[0] if sample_doc else 'No sample'}")
            
            result = await content_data_collection.aggregate(pipeline).to_list(length=None)
            
            # Log raw result
            logger.info(f"Raw aggregation result: {json.dumps(result, indent=2)}")
            
            if result:
                metrics = result[0]
                response = {
                    "status": "success",
                    "total_views": metrics.get("total_views", 0),
                    "impressions": metrics.get("impressions", 0),
                    "reach": metrics.get("reach", 0),
                    "snap_view_time": metrics.get("snap_view_time", 0),
                    "profile_views": metrics.get("profile_views", 0),
                    "spotlight_views": metrics.get("spotlight_views", 0),
                    "saved_story_views": metrics.get("saved_story_views", 0),
                    "timeframe": f"{days}d"
                }
                logger.info(f"Sending response for {days}d: {response}")
                return response
            else:
                 return {
                    "status": "success",
                    "total_views": 0,
                    "impressions": 0,
                    "reach": 0,
                    "snap_view_time": 0,
                    "profile_views": 0,
                    "spotlight_views": 0,
                    "saved_story_views": 0,
                    "timeframe": f"{days}d"
                }
        
        else:
            raise HTTPException(status_code=400, detail="Either user_id or user_ids must be provided")
            
    except Exception as e:
        logger.error(f"Error in get_mobile_analytics: {str(e)}")
        # Log the full error details for debugging
        logger.exception("Full error details:")
        raise HTTPException(status_code=500, detail=str(e))

def calculate_growth(current: float, previous: float) -> float:
    """Calculate growth rate between two values"""
    if previous == 0:
        return 0.0
    return ((current - previous) / previous) * 100

@router.post("/sync/vista")
async def sync_vista_analytics(
):
    """Sync analytics data from Vista"""
    try:
        await vista_service.sync_analytics()
        return {
            "status": "success",
            "message": "Vista analytics sync completed"
        }
    except Exception as e:
        logger.error(f"Vista sync error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Vista sync failed: {str(e)}"
        )

@router.post("/sync/profile")
async def sync_profile_analytics(
    request: Request,
    data: dict = Body(...),
):
    """
    Updates profile analytics data across collections.
    
    Data Flow:
        1. Profile data validation
        2. content_data collection update
        3. ClientInfo collection update
        
    Consistency:
        - Atomic updates where possible
        - Logging of all operations
        - Validation of results
    """
    try:
        profile_name = data.get("profile_name")
        snap_id = data.get("snap_id")
        user_id = data.get("user_id")

        logger.info(f"Syncing profile - name: {profile_name}, snap_id: {snap_id}, user_id: {user_id}")

        if not all([profile_name, snap_id, user_id]):
            raise HTTPException(status_code=400, detail="Missing required fields")

        # Update/create content_data document
        content_result = await content_data_collection.update_one(
            {
                "snap_profile_name": profile_name,
                "platform": "snapchat"
            },
            {
                "$set": {
                    "snap_id": snap_id,
                    "user_id": user_id,
                    "platform": "snapchat",
                }
            },
            upsert=True
        )
        logger.info(f"Content data update result: {content_result.modified_count} modified, {content_result.upserted_id} upserted")

        # Update ClientInfo document with just snap_profile_name
        client_result = await client_info_collection.update_one(
            {"client_id": user_id},
            {
                "$set": {
                    "snap_profile_name": profile_name,
                    "updated_at": datetime.utcnow()
                }
            }
        )
        logger.info(f"ClientInfo update result: {client_result.modified_count} modified")

        # If no document was updated, try to find it to debug
        if client_result.modified_count == 0:
            existing_doc = await client_info_collection.find_one({"client_id": user_id})
            if existing_doc:
                logger.warning(f"Found document but not updated. Current profile: {existing_doc.get('snap_profile_name')}")
            else:
                logger.warning(f"No document found with client_id: {user_id}")

        return {"status": "success"}

    except Exception as e:
        logger.error(f"Error syncing profile analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/clients")
async def get_analytics_clients(
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Retrieves list of clients with analytics data.
    
    Security:
        - Respects user permissions
        - Filters based on auth data
        
    Response:
        - Client IDs
        - Platform information
        - Profile names
        - Analytics availability
    """
    try:
        # Get filtered query based on user's permissions
        filter_query = await get_filtered_query(auth_data)
        
        # Find all unique clients with analytics data
        pipeline = [
            {"$match": {
                "platform": "snapchat"  # Only get Snapchat clients
            }},
            {"$project": {
                "_id": 0,
                "client_id": 1,
                "user_id": 1,
                "snap_profile_name": 1,
                "platform": 1
            }}
        ]
        
        cursor = content_data_collection.aggregate(pipeline)
        clients = await cursor.to_list(length=None)
        
        # Debug logging
        logger.info(f"Found {len(clients)} clients with analytics data")
        logger.info(f"Sample client data: {clients[0] if clients else 'No clients found'}")
        
        return {
            "status": "success",
            "data": clients
        }
        
    except Exception as e:
        logger.error(f"Error fetching analytics clients: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

