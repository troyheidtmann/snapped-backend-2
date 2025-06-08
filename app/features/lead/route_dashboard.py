"""
Lead Dashboard API - Analytics and Search Interface

This module provides FastAPI routes for the lead management dashboard, offering
comprehensive analytics and advanced search functionality for leads across
multiple social media platforms.

Features:
--------
1. Dashboard Analytics:
   - Total lead counts
   - Monetization statistics
   - Platform-specific metrics
   - Verification status tracking

2. Platform Statistics:
   - Instagram: Followers, verified status
   - TikTok: Followers, verified status
   - YouTube: Followers, verified status
   - Snapchat: Followers, star status

3. Search Capabilities:
   - Multi-field text search
   - Platform filtering
   - Monetization status
   - Follower range filtering
   - Contract status

Data Model:
----------
Each lead includes:
- Basic Information: Names, email, stage name
- Platform Profiles: Usernames and follower counts
- Status Flags: Monetization, verification, signing
- Analytics: Platform-specific metrics

Dependencies:
-----------
- FastAPI: Web framework and routing
- MongoDB: Data storage
- bson: ObjectId handling
- typing: Type hints

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException
from typing import Optional
from app.shared.database import client_info
from bson import ObjectId

router = APIRouter(prefix="/api/dashboard")

@router.get("/stats")
async def get_dashboard_stats():
    """
    Get comprehensive dashboard statistics across all platforms.
    
    Calculates and returns aggregated statistics including:
    - Total lead counts
    - Monetization rates
    - Platform-specific metrics
    - Verification status
    
    Returns:
        dict: Dashboard statistics containing:
            - total_leads: Total number of leads
            - monetized_leads: Number of monetized leads
            - signed_leads: Number of signed leads
            - platform_stats: Per-platform statistics:
                - total_followers: Total follower count
                - monetized_followers: Followers for monetized accounts
                - signed_followers: Followers for signed accounts
                - verified_count/star_count: Verification metrics
                
    Raises:
        HTTPException: For database errors
    """
    try:
        # Convert to async operation
        cursor = client_info.find({})
        leads = await cursor.to_list(length=None)
        
        # Initialize statistics
        stats = {
            "total_leads": len(leads),
            "monetized_leads": 0,
            "signed_leads": 0,
            "platform_stats": {
                "instagram": {
                    "total_followers": 0,
                    "monetized_followers": 0,
                    "signed_followers": 0,
                    "verified_count": 0
                },
                "tiktok": {
                    "total_followers": 0,
                    "monetized_followers": 0,
                    "signed_followers": 0,
                    "verified_count": 0
                },
                "youtube": {
                    "total_followers": 0,
                    "monetized_followers": 0,
                    "signed_followers": 0,
                    "verified_count": 0
                },
                "snapchat": {
                    "total_followers": 0,
                    "monetized_followers": 0,
                    "signed_followers": 0,
                    "star_count": 0
                }
            }
        }
        
        # Calculate statistics
        for lead in leads:
            try:
                if lead.get('Snap_Monetized'):
                    stats["monetized_leads"] += 1
                if lead.get('is_signed'):
                    stats["signed_leads"] += 1
                
                # Instagram stats
                ig_followers = int(lead.get('IG_Followers', 0) or 0)
                stats["platform_stats"]["instagram"]["total_followers"] += ig_followers
                if lead.get('Snap_Monetized'):
                    stats["platform_stats"]["instagram"]["monetized_followers"] += ig_followers
                if lead.get('is_signed'):
                    stats["platform_stats"]["instagram"]["signed_followers"] += ig_followers
                if lead.get('IG_Verified'):
                    stats["platform_stats"]["instagram"]["verified_count"] += 1

                # TikTok stats
                tt_followers = int(lead.get('TT_Followers', 0) or 0)
                stats["platform_stats"]["tiktok"]["total_followers"] += tt_followers
                if lead.get('Snap_Monetized'):
                    stats["platform_stats"]["tiktok"]["monetized_followers"] += tt_followers
                if lead.get('is_signed'):
                    stats["platform_stats"]["tiktok"]["signed_followers"] += tt_followers
                if lead.get('TT_Verified'):
                    stats["platform_stats"]["tiktok"]["verified_count"] += 1

                # YouTube stats
                yt_followers = int(lead.get('YT_Followers', 0) or 0)
                stats["platform_stats"]["youtube"]["total_followers"] += yt_followers
                if lead.get('Snap_Monetized'):
                    stats["platform_stats"]["youtube"]["monetized_followers"] += yt_followers
                if lead.get('is_signed'):
                    stats["platform_stats"]["youtube"]["signed_followers"] += yt_followers
                if lead.get('YT_Verified'):
                    stats["platform_stats"]["youtube"]["verified_count"] += 1

                # Snapchat stats
                snap_followers = int(lead.get('Snap_Followers', 0) or 0) if lead.get('Snap_Followers') != "na" else 0
                stats["platform_stats"]["snapchat"]["total_followers"] += snap_followers
                if lead.get('Snap_Monetized'):
                    stats["platform_stats"]["snapchat"]["monetized_followers"] += snap_followers
                if lead.get('is_signed'):
                    stats["platform_stats"]["snapchat"]["signed_followers"] += snap_followers
                if lead.get('Snap_Star'):
                    stats["platform_stats"]["snapchat"]["star_count"] += 1
                
            except Exception as e:
                print(f"Error processing lead: {str(e)}")
                continue
        
        return stats
    except Exception as e:
        print("Error in get_dashboard_stats:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/search")
async def search_leads(
    query: Optional[str] = None,
    platform: Optional[str] = None,
    is_monetized: Optional[bool] = None,
    is_signed: Optional[bool] = None,
    min_followers: Optional[int] = None,
    max_followers: Optional[int] = None
):
    """
    Search and filter leads based on multiple criteria.
    
    Args:
        query (Optional[str]): Search text for names, usernames, email
        platform (Optional[str]): Platform filter (instagram, tiktok, youtube, snapchat)
        is_monetized (Optional[bool]): Filter by monetization status
        is_signed (Optional[bool]): Filter by signing status
        min_followers (Optional[int]): Minimum follower count
        max_followers (Optional[int]): Maximum follower count
        
    Returns:
        list: Filtered leads with transformed IDs
        
    Raises:
        HTTPException: For database errors
        
    Notes:
        - Text search is case-insensitive
        - Follower filters apply to the selected platform only
        - Results include all fields from the lead documents
    """
    try:
        # Build the search filter
        search_filter = {}
        
        if query:
            # Search in multiple fields
            search_filter["$or"] = [
                {"First_Legal_Name": {"$regex": query, "$options": "i"}},
                {"Last_Legal_Name": {"$regex": query, "$options": "i"}},
                {"Stage_Name": {"$regex": query, "$options": "i"}},
                {"Email_Address": {"$regex": query, "$options": "i"}},
                {"IG_Username": {"$regex": query, "$options": "i"}},
                {"TT_Username": {"$regex": query, "$options": "i"}},
                {"YT_Username": {"$regex": query, "$options": "i"}},
                {"Snap_Username": {"$regex": query, "$options": "i"}}
            ]
        
        if is_monetized is not None:
            search_filter["Snap_Monetized"] = is_monetized
            
        if is_signed is not None:
            search_filter["is_signed"] = is_signed
        
        if platform:
            if platform == "instagram" and min_followers:
                search_filter["IG_Followers"] = {"$gte": min_followers}
            elif platform == "tiktok" and min_followers:
                search_filter["TT_Followers"] = {"$gte": min_followers}
            elif platform == "youtube" and min_followers:
                search_filter["YT_Followers"] = {"$gte": min_followers}
            elif platform == "snapchat" and min_followers:
                search_filter["Snap_Followers"] = {"$gte": min_followers}
                
            if platform == "instagram" and max_followers:
                search_filter.setdefault("IG_Followers", {})["$lte"] = max_followers
            elif platform == "tiktok" and max_followers:
                search_filter.setdefault("TT_Followers", {})["$lte"] = max_followers
            elif platform == "youtube" and max_followers:
                search_filter.setdefault("YT_Followers", {})["$lte"] = max_followers
            elif platform == "snapchat" and max_followers:
                search_filter.setdefault("Snap_Followers", {})["$lte"] = max_followers
        
        # Convert to async operation
        cursor = client_info.find(search_filter)
        leads = await cursor.to_list(length=None)
        
        # Transform the results
        transformed_leads = []
        for lead in leads:
            lead['id'] = str(lead.pop('_id'))
            transformed_leads.append(lead)
            
        return transformed_leads
    except Exception as e:
        print("Error in search_leads:", str(e))
        raise HTTPException(status_code=500, detail=str(e)) 