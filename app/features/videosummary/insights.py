"""
Video Insights Module

This module provides functionality to extract and manage insights
from video analysis results.

Features:
- Content insights extraction
- Best practices generation
- Pattern recognition
- Confidence scoring
- Metadata tracking

Data Model:
- Content insights
- Best practices
- Confidence scores
- Implementation tips
- Source tracking

Dependencies:
- Pydantic for models
- MongoDB for storage
- datetime for tracking
- typing for type hints

Author: Snapped Development Team
"""

from typing import Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel
from app.shared.database import video_analysis_collection, analysis_queue_collection

class ContentInsight(BaseModel):
    """
    Content insight model.
    
    Represents an insight extracted from video analysis.
    
    Attributes:
        insight_type (str): Type of insight (engagement, style, pattern)
        description (str): Insight description
        confidence_score (float): Confidence level
        source_videos (List[str]): Source video IDs
        metadata (Optional[Dict]): Additional metadata
        created_at (datetime): Creation timestamp
        last_updated (datetime): Last update timestamp
    """
    insight_type: str  # e.g., "engagement", "style", "pattern"
    description: str
    confidence_score: float
    source_videos: List[str]  # List of video IDs that led to this insight
    metadata: Optional[Dict] = None
    created_at: datetime
    last_updated: datetime

class BestPractice(BaseModel):
    """
    Best practice model.
    
    Represents a best practice derived from insights.
    
    Attributes:
        practice_id (str): Unique identifier
        title (str): Practice title
        description (str): Practice description
        category (str): Practice category
        supporting_insights (List[ContentInsight]): Supporting insights
        confidence_score (float): Confidence level
        implementation_tips (List[str]): Implementation guidance
        created_at (datetime): Creation timestamp
        last_updated (datetime): Last update timestamp
        source_client_ids (Optional[List[str]]): Source clients
    """
    practice_id: str
    title: str
    description: str
    category: str  # e.g., "content", "engagement", "technical"
    supporting_insights: List[ContentInsight]
    confidence_score: float
    implementation_tips: List[str]
    created_at: datetime
    last_updated: datetime
    source_client_ids: Optional[List[str]] = None

async def store_video_analysis_results(
    client_id: str,
    video_id: str,
    analysis_results: Dict,
    performance_metrics: Optional[Dict] = None
) -> bool:
    """
    Store and process analysis results.
    
    Args:
        client_id: Client identifier
        video_id: Video identifier
        analysis_results: Analysis data
        performance_metrics: Optional metrics
        
    Returns:
        bool: Success status
        
    Notes:
        - Updates analysis
        - Updates queue
        - Handles errors
        - Returns status
    """
    try:
        # Update video analysis with results
        await video_analysis_collection.update_one(
            {"client_id": client_id, "video_metadata.file_name": video_id},
            {
                "$set": {
                    "analysis_results": analysis_results,
                    "performance_metrics": performance_metrics,
                    "analysis_status": "completed",
                    "last_updated": datetime.utcnow()
                }
            }
        )
        
        # Update queue status
        await analysis_queue_collection.update_one(
            {"client_id": client_id, "video_id": video_id},
            {"$set": {"status": "completed"}}
        )
        
        return True
    except Exception as e:
        print(f"Error storing analysis results: {str(e)}")
        return False

async def extract_insights(analysis_results: Dict) -> List[ContentInsight]:
    """
    Extract insights from analysis.
    
    Args:
        analysis_results: Analysis data
        
    Returns:
        List[ContentInsight]: Extracted insights
        
    Notes:
        - Processes patterns
        - Creates insights
        - Sets confidence
        - Tracks sources
    """
    insights = []
    
    # Process different types of insights
    # This will be expanded based on 12labs response structure
    if "patterns" in analysis_results:
        for pattern in analysis_results["patterns"]:
            insights.append(
                ContentInsight(
                    insight_type="pattern",
                    description=pattern["description"],
                    confidence_score=pattern["confidence"],
                    source_videos=[pattern["video_id"]],
                    created_at=datetime.utcnow(),
                    last_updated=datetime.utcnow()
                )
            )
    
    return insights

async def update_best_practices(
    insights: List[ContentInsight],
    client_id: str
) -> None:
    """Update best practices based on new insights"""
    for insight in insights:
        # Check if this insight should create/update a best practice
        if insight.confidence_score > 0.8:  # High confidence threshold
            practice = BestPractice(
                practice_id=f"BP_{datetime.utcnow().timestamp()}",
                title=f"Best Practice from {insight.insight_type}",
                description=insight.description,
                category=insight.insight_type,
                supporting_insights=[insight],
                confidence_score=insight.confidence_score,
                implementation_tips=[
                    f"Implement {insight.description} in your content"
                ],
                created_at=datetime.utcnow(),
                last_updated=datetime.utcnow(),
                source_client_ids=[client_id]
            )
            
            # Store in database
            await video_analysis_collection.update_one(
                {"practice_id": practice.practice_id},
                {"$set": practice.dict()},
                upsert=True
            ) 