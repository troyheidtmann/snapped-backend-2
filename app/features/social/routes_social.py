"""
Social Media Statistics Integration Module

This module provides integration with Social Blade's Matrix API to fetch statistics
for various social media platforms including Instagram, TikTok, and YouTube.

Features:
- Platform-specific statistics retrieval
- Engagement metrics tracking
- Historical data analysis
- Error handling and logging

Data Model:
- User statistics
- Engagement metrics
- Historical trends
- Platform rankings

Security:
- API authentication
- Rate limiting
- Error handling
- Access control

Dependencies:
- FastAPI for routing
- aiohttp for async HTTP requests
- MongoDB for data storage
- logging for debug tracking

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException
from app.shared.database import async_client
from app.config.socialblade import SOCIALBLADE_CLIENT_ID, SOCIALBLADE_ACCESS_TOKEN
import logging
import aiohttp
import json
from typing import Optional, Dict, Any, List

router = APIRouter()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SocialBladeAPI:
    """
    Social Blade Matrix API client for fetching social media statistics.
    
    Handles authentication and API requests to Social Blade's Matrix API
    for retrieving platform-specific statistics and metrics.
    
    Attributes:
        BASE_URL (str): Base URL for the Matrix API
        client_id (str): Social Blade client ID
        access_token (str): Social Blade access token
        session (aiohttp.ClientSession): Async HTTP session
    """
    
    BASE_URL = "https://matrix.sbapis.com/b"
    
    def __init__(self, client_id: str, access_token: str):
        """
        Initialize the Social Blade API client.
        
        Args:
            client_id (str): Social Blade client ID
            access_token (str): Social Blade access token
        """
        self.client_id = client_id
        self.access_token = access_token
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers={
            "token": self.access_token,
            "clientid": self.client_id
        })
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_instagram_stats(self, username: str) -> Dict[str, Any]:
        """
        Get Instagram statistics using the Matrix API.
        
        Args:
            username (str): Instagram username to fetch stats for
            
        Returns:
            Dict[str, Any]: Instagram statistics including:
                - User profile data
                - Engagement metrics
                - Media statistics
                - Ranking information
                
        Raises:
            HTTPException: For API errors or user not found
        """
        async with self.session.get(
            f"{self.BASE_URL}/instagram/statistics",
            params={
                "query": username,
                "history": "default",
                "allow-stale": "false"
            }
        ) as response:
            if response.status == 404:
                raise HTTPException(status_code=404, detail=f"Instagram user {username} not found")
            elif response.status != 200:
                raise HTTPException(
                    status_code=response.status,
                    detail=f"Error fetching Instagram stats: {await response.text()}"
                )
            
            data = await response.json()
            logger.info(f"Raw Instagram API response: {json.dumps(data)}")
            
            if not data.get("status", {}).get("success"):
                error = data.get("status", {}).get("error", "Unknown error")
                raise HTTPException(status_code=400, detail=error)
            
            # Get the data object directly since it's not a list anymore
            user_data = data.get("data")
            if not user_data:
                raise HTTPException(status_code=404, detail=f"No data found for Instagram user {username}")
            
            try:
                return {
                    "user": {
                        "id": user_data.get("id", {}).get("id"),
                        "username": user_data.get("id", {}).get("username"),
                        "display_name": user_data.get("id", {}).get("display_name")
                    },
                    "general": {
                        "branding": {
                            "website": user_data.get("general", {}).get("branding", {}).get("website"),
                            "avatar": user_data.get("general", {}).get("branding", {}).get("avatar")
                        },
                        "media": {
                            "recent": user_data.get("general", {}).get("media", {}).get("recent", [])
                        }
                    },
                    "statistics": {
                        "followers": user_data.get("statistics", {}).get("total", {}).get("followers", 0),
                        "following": user_data.get("statistics", {}).get("total", {}).get("following", 0),
                        "media": user_data.get("statistics", {}).get("total", {}).get("media", 0),
                        "engagement_rate": user_data.get("statistics", {}).get("total", {}).get("engagement_rate", 0)
                    },
                    "ranks": {
                        "sbrank": user_data.get("ranks", {}).get("sbrank", 0),
                        "followers": user_data.get("ranks", {}).get("followers", 0),
                        "following": user_data.get("ranks", {}).get("following", 0),
                        "media": user_data.get("ranks", {}).get("media", 0),
                        "engagement_rate": user_data.get("ranks", {}).get("engagement_rate", 0)
                    },
                    "misc": {
                        "sb_verified": user_data.get("misc", {}).get("sb_verified", False),
                        "grade": user_data.get("misc", {}).get("grade", {})
                    }
                }
            except Exception as e:
                logger.error(f"Error parsing Instagram data for {username}: {str(e)}")
                logger.error(f"User data: {json.dumps(user_data)}")
                raise HTTPException(status_code=500, detail=f"Error parsing Instagram data: {str(e)}")

    async def get_tiktok_stats(self, username: str) -> Dict[str, Any]:
        """
        Get TikTok statistics using the Matrix API.
        
        Args:
            username (str): TikTok username to fetch stats for
            
        Returns:
            Dict[str, Any]: TikTok statistics including:
                - User profile data
                - Video metrics
                - Engagement data
                - Historical trends
                
        Raises:
            HTTPException: For API errors or user not found
        """
        async with self.session.get(
            f"{self.BASE_URL}/tiktok/statistics",
            params={
                "query": username,
                "history": "default",
                "allow-stale": "false"
            }
        ) as response:
            if response.status == 404:
                raise HTTPException(status_code=404, detail=f"TikTok user {username} not found")
            elif response.status != 200:
                raise HTTPException(
                    status_code=response.status,
                    detail=f"Error fetching TikTok stats: {await response.text()}"
                )
            
            data = await response.json()
            
            if not data.get("status", {}).get("success"):
                error = data.get("status", {}).get("error", "Unknown error")
                raise HTTPException(status_code=400, detail=error)
            
            user_data = data.get("data", {})
            
            # Extract daily history
            daily_stats = []
            for day in user_data.get("daily", []):
                daily_stats.append({
                    "date": day.get("date"),
                    "followers": day.get("followers", 0),
                    "following": day.get("following", 0),
                    "uploads": day.get("uploads", 0),
                    "likes": day.get("likes", 0)
                })
            
            return {
                "user": {
                    "id": user_data.get("id", {}).get("id"),
                    "username": user_data.get("id", {}).get("username"),
                    "display_name": user_data.get("id", {}).get("display_name"),
                    "avatar": user_data.get("general", {}).get("branding", {}).get("avatar")
                },
                "statistics": {
                    "followers": user_data.get("statistics", {}).get("total", {}).get("followers", 0),
                    "following": user_data.get("statistics", {}).get("total", {}).get("following", 0),
                    "likes": user_data.get("statistics", {}).get("total", {}).get("likes", 0),
                    "uploads": user_data.get("statistics", {}).get("total", {}).get("uploads", 0)
                },
                "misc": {
                    "grade": {
                        "color": user_data.get("misc", {}).get("grade", {}).get("color"),
                        "grade": user_data.get("misc", {}).get("grade", {}).get("grade")
                    },
                    "sb_verified": user_data.get("misc", {}).get("sb_verified", False)
                },
                "ranks": {
                    "sbrank": user_data.get("ranks", {}).get("sbrank", 0),
                    "followers": user_data.get("ranks", {}).get("followers", 0),
                    "following": user_data.get("ranks", {}).get("following", 0),
                    "uploads": user_data.get("ranks", {}).get("uploads", 0),
                    "likes": user_data.get("ranks", {}).get("likes", 0)
                },
                "daily": daily_stats
            }
    
    async def get_youtube_stats(self, username: str) -> Dict[str, Any]:
        """
        Get YouTube statistics using the Matrix API.
        
        Args:
            username (str): YouTube username to fetch stats for
            
        Returns:
            Dict[str, Any]: YouTube statistics including:
                - Channel information
                - Subscriber counts
                - View metrics
                - Ranking data
                
        Raises:
            HTTPException: For API errors or user not found
        """
        async with self.session.get(
            f"{self.BASE_URL}/youtube/statistics",
            params={"query": username}
        ) as response:
            if response.status == 404:
                raise HTTPException(status_code=404, detail=f"YouTube user {username} not found")
            elif response.status != 200:
                raise HTTPException(
                    status_code=response.status,
                    detail=f"Error fetching YouTube stats: {await response.text()}"
                )
            
            data = await response.json()
            
            if not data.get("status", {}).get("success"):
                error = data.get("status", {}).get("error", "Unknown error")
                raise HTTPException(status_code=400, detail=error)
            
            user_data = data.get("data", {})
            
            return {
                "user": {
                    "username": user_data.get("id", {}).get("username"),
                    "display_name": user_data.get("id", {}).get("display_name"),
                    "handle": user_data.get("id", {}).get("handle"),
                    "created_at": user_data.get("general", {}).get("created_at"),
                    "channel_type": user_data.get("general", {}).get("channel_type"),
                    "country": user_data.get("general", {}).get("geo", {}).get("country"),
                    "country_code": user_data.get("general", {}).get("geo", {}).get("country_code")
                },
                "statistics": {
                    "subscribers": user_data.get("statistics", {}).get("total", {}).get("subscribers", 0),
                    "views": user_data.get("statistics", {}).get("total", {}).get("views", 0),
                    "uploads": user_data.get("statistics", {}).get("total", {}).get("uploads", 0)
                },
                "ranks": {
                    "sbrank": user_data.get("ranks", {}).get("sbrank", 0),
                    "subscribers": user_data.get("ranks", {}).get("subscribers", 0),
                    "views": user_data.get("ranks", {}).get("views", 0),
                    "country": user_data.get("ranks", {}).get("country", 0),
                    "channel_type": user_data.get("ranks", {}).get("channel_type", 0)
                }
            }
    
    async def get_stats(self, platform: str, username: str) -> Dict[str, Any]:
        """
        Get statistics for a given platform and username.
        
        Args:
            platform (str): Social media platform (youtube, tiktok, instagram)
            username (str): Username to fetch stats for
            
        Returns:
            Dict[str, Any]: Platform-specific statistics
            
        Raises:
            ValueError: If platform is not supported
            HTTPException: For API errors
        """
        platform_handlers = {
            "youtube": self.get_youtube_stats,
            "tiktok": self.get_tiktok_stats,
            "instagram": self.get_instagram_stats
        }
        
        if platform not in platform_handlers:
            raise ValueError(f"Platform {platform} not supported")
            
        return await platform_handlers[platform](username)

@router.get("/social-stats/{platform}/{username}")
async def get_social_stats(platform: str, username: str):
    """
    Get social media stats for a given platform and username using Social Blade Matrix API.
    
    Args:
        platform (str): Social media platform to fetch stats from
        username (str): Username to fetch stats for
        
    Returns:
        Dict: Platform-specific statistics and metrics
        
    Raises:
        HTTPException: For API errors or invalid requests
        
    Notes:
        - Supports YouTube, TikTok, and Instagram
        - Uses Social Blade Matrix API
        - Includes error logging
    """
    try:
        logger.info(f"Fetching stats for {platform} user: {username}")
        logger.info(f"Using client ID: {SOCIALBLADE_CLIENT_ID}")
        
        async with SocialBladeAPI(SOCIALBLADE_CLIENT_ID, SOCIALBLADE_ACCESS_TOKEN) as api:
            logger.info("Created SocialBladeAPI instance")
            result = await api.get_stats(platform, username)
            logger.info(f"Got result: {result}")
            return result
            
    except ValueError as e:
        logger.error(f"Value error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException as he:
        logger.error(f"HTTP error: {str(he.detail)}")
        raise he
    except Exception as e:
        logger.error(f"Error getting {platform} stats for {username}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 