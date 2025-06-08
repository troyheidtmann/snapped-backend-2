"""
Video Summary Review Module

This module provides functionality to review and manage video summaries,
including prompt management and feedback collection.

Features:
- Summary review
- Prompt management
- Feedback collection
- Quality metrics
- Content sync

Data Model:
- Video summaries
- Review prompts
- Feedback data
- Quality scores
- Sync status

Security:
- API key validation
- Access control
- Data validation
- Error handling

Dependencies:
- FastAPI for routing
- MongoDB for storage
- OpenAI for prompts
- logging for tracking
- typing for type hints

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, List
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from app.shared.database import async_client
import logging
from openai import AsyncOpenAI
import json
import os
from dotenv import load_dotenv
from pathlib import Path
from pydantic import BaseModel

# Set up logging
logger = logging.getLogger(__name__)

# Load environment variables from .env file
env_path = Path(__file__).resolve().parents[2] / '.env'
load_dotenv(env_path)

router = APIRouter(prefix="/api/ai-review/video-summary", tags=["video-summary"])

class PromptActivateRequest(BaseModel):
    """
    Prompt activation request model.
    
    Attributes:
        prompt_text (str): Prompt text to activate
    """
    prompt_text: str

# Initialize OpenAI client with better error handling
try:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("OPENAI_API_KEY not found in environment variables. Please check .env file.")
        raise ValueError("OPENAI_API_KEY not found")
    openai_client = AsyncOpenAI(api_key=api_key)
    logger.info("OpenAI client initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize OpenAI client: {str(e)}")
    raise RuntimeError(f"OpenAI client initialization failed: {str(e)}")

# Get the correct collections
video_analysis_collection = async_client["AIVideo"]["video_analysis"]
summary_prompt_collection = async_client["AIVideo"]["summary_prompts"]
summary_review_collection = async_client["AIVideo"]["Summary_Prompt_Review"]

@router.get("/sync-content")
async def sync_new_content():
    """
    Sync new videos for summary review.
    
    Returns:
        dict: Sync results with video data
        
    Notes:
        - Checks new videos
        - Gets active prompt
        - Formats response
        - Handles errors
    """
    try:
        videos_with_summaries = []
        
        # Get all documents with video summaries
        async for doc in video_analysis_collection.find({"STORY": {"$exists": True}}):
            if doc and "STORY" in doc:
                for story in doc["STORY"]:
                    if story.get("video_summary"):
                        # Check if this video has already been reviewed
                        existing_review = await summary_review_collection.find_one({
                            "video_id": story.get("video_id")
                        })
                        
                        if not existing_review:
                            videos_with_summaries.append({
                                "video_id": story.get("video_id"),
                                "client_id": doc.get("client_id"),
                                "file_name": story.get("file_name"),
                                "summary": story.get("video_summary"),
                                "cdn_url": story.get("cdn_url"),
                                "session_id": story.get("session_id"),
                                "indexed_at": story.get("indexed_at"),
                                "task_id": story.get("task_id"),
                                "index_id": story.get("index_id")
                            })

        # Get current active prompt from database
        active_prompt = await summary_prompt_collection.find_one({"active": True})
        
        # If no active prompt exists, create v1 prompt
        if not active_prompt:
            logger.info("No active prompt found, creating v1 prompt")
            active_prompt = {
                "prompt_id": "v1",
                "prompt_text": """Summarize this video for Snapchat Stories. Return the following structured response:

Theme (≤4 words): [Concise theme]

3 Bullet Points on Actions (≤7 words each):
- [Action 1]
- [Action 2]
- [Action 3]

Overview (≤10 words): [Brief summary]

Improvement Suggestion (≤10 words): [How to improve]

Similar Ideas: 
- [Alternative ideas for similar content]
- [Alternative ideas for similar content]
- [Alternative ideas for similar content]

Environment (≤7 words): [Lighting, clothing, voice tone]""",
                "created_at": datetime.utcnow(),
                "active": True,
                "average_rating": 0,
                "total_feedback": 0
            }
            result = await summary_prompt_collection.insert_one(active_prompt)
            active_prompt["_id"] = result.inserted_id

        response = {
            "total_new_videos": len(videos_with_summaries),
            "videos": videos_with_summaries,
            "current_prompt": {
                "id": str(active_prompt["_id"]),
                "prompt_id": active_prompt.get("prompt_id"),
                "text": active_prompt.get("prompt_text"),
                "average_rating": active_prompt.get("average_rating", 0),
                "total_feedback": active_prompt.get("total_feedback", 0)
            }
        }

        logger.info(f"Found {len(videos_with_summaries)} new videos needing review")
        return response

    except Exception as e:
        logger.error(f"Error in sync_new_content: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/save-review/{video_id}")
async def save_review(video_id: str, review_data: Dict):
    """
    Save review feedback.
    
    Args:
        video_id: Video identifier
        review_data: Review feedback data
        
    Returns:
        dict: Save status
        
    Notes:
        - Validates prompt
        - Saves review
        - Updates metrics
        - Handles errors
    """
    try:
        # Get current active prompt to associate with review
        active_prompt = await summary_prompt_collection.find_one({"active": True})
        if not active_prompt:
            raise HTTPException(status_code=400, detail="No active prompt found")

        # Add metadata to review data
        review_data.update({
            "video_id": video_id,
            "created_at": datetime.utcnow(),
            "prompt_id": active_prompt.get("prompt_id"),
            "prompt_text": active_prompt.get("prompt_text")
        })
        
        # Save to Summary_Prompt_Review collection
        await summary_review_collection.insert_one(review_data)
        
        # Update metrics in summary_prompts collection if needed
        if review_data.get("ratings"):
            avg_rating = sum(review_data["ratings"].values()) / len(review_data["ratings"])
            await summary_prompt_collection.update_one(
                {"_id": active_prompt["_id"]},
                {
                    "$inc": {
                        "total_feedback": 1
                    },
                    "$set": {
                        "average_rating": (
                            (active_prompt.get("average_rating", 0) * active_prompt.get("total_feedback", 0) + avg_rating) /
                            (active_prompt.get("total_feedback", 0) + 1)
                        )
                    }
                }
            )
        
        return {"status": "success"}

    except Exception as e:
        logger.error(f"Error saving review: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/prompts")
async def get_prompts():
    """
    Get active and v1 prompts.
    
    Returns:
        dict: Prompt data
        
    Notes:
        - Gets active prompt
        - Gets v1 prompt
        - Formats response
        - Includes metrics
    """
    try:
        # Get active prompt and v1 prompt
        prompts = await summary_prompt_collection.find({
            "$or": [
                {"active": True},
                {"prompt_id": "v1"}
            ]
        }).to_list(length=None)
        
        return {
            "prompts": [{
                "id": str(prompt["_id"]),
                "prompt_id": prompt.get("prompt_id"),
                "prompt_text": prompt.get("prompt_text"),
                "active": prompt.get("active", False),
                "created_at": prompt.get("created_at"),
                "average_rating": prompt.get("average_rating", 0),
                "total_feedback": prompt.get("total_feedback", 0)
            } for prompt in prompts]
        }

    except Exception as e:
        logger.error(f"Error getting prompts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/generate-prompt")
async def generate_new_prompt():
    """Analyze feedback and generate new prompt suggestions"""
    try:
        # Check for any unreviewed videos
        unreviewed_count = 0
        async for doc in video_analysis_collection.find({"STORY": {"$exists": True}}):
            if doc and "STORY" in doc:
                for story in doc["STORY"]:
                    if story.get("video_summary"):
                        existing_review = await summary_review_collection.find_one({
                            "video_id": story.get("video_id")
                        })
                        if not existing_review:
                            unreviewed_count += 1
                            break
                if unreviewed_count > 0:
                    break

        if unreviewed_count > 0:
            raise HTTPException(
                status_code=400,
                detail="Cannot generate new prompt while there are unreviewed videos. Please review all pending videos first."
            )

        if not openai_client:
            raise HTTPException(
                status_code=503,
                detail="OpenAI integration not available. Please check API key configuration."
            )

        # Get current active prompt
        active_prompt = await summary_prompt_collection.find_one({"active": True})
        if not active_prompt:
            raise HTTPException(status_code=404, detail="No active prompt found")

        # Get all reviews for the current prompt
        reviews = await summary_review_collection.find({
            "prompt_id": active_prompt.get("prompt_id")
        }).to_list(length=None)

        # Format feedback for analysis
        feedback_data = {
            "current_prompt": active_prompt.get("prompt_text"),
            "average_rating": active_prompt.get("average_rating", 0),
            "total_feedback": len(reviews),
            "reviews": [{
                "ratings": review.get("ratings", {}),
                "feedback": review.get("feedback", "")
            } for review in reviews]
        }

        # Analyze feedback patterns
        rating_categories = {}
        feedback_themes = []
        
        for review in reviews:
            ratings = review.get("ratings", {})
            for category, rating in ratings.items():
                if category not in rating_categories:
                    rating_categories[category] = []
                rating_categories[category].append(rating)
            
            if review.get("feedback"):
                feedback_themes.append(review["feedback"])

        # Calculate average ratings per category
        rating_analysis = {
            category: sum(ratings)/len(ratings) 
            for category, ratings in rating_categories.items()
            if ratings
        }

        # Create prompt for OpenAI
        analysis_prompt = f"""You are an AI prompt engineering expert. Analyze this feedback for a Snapchat video summary prompt and generate 3 improved versions.

Current Prompt:
{feedback_data['current_prompt']}

Feedback Analysis:
- Total Reviews: {feedback_data['total_feedback']}
- Average Rating: {feedback_data['average_rating']}
- Category Ratings: {json.dumps(rating_analysis, indent=2)}
- User Feedback: {json.dumps(feedback_themes, indent=2)}

Generate 3 different improved prompts that:
1. Address any low-rated categories
2. Incorporate common feedback themes
3. Maintain the structured format but improve clarity
4. Keep character limits and section organization
5. Focus on Snapchat-specific content analysis

Your response MUST be a JSON object with this exact structure:
{{
    "suggestions": [
        {{
            "prompt_text": "The full prompt text...",
            "explanation": "Why this version might perform better...",
            "focus_areas": ["Key improvement 1", "Key improvement 2", ...]
        }},
        // Two more suggestions with the same structure
    ]
}}

Do not include any text outside of this JSON structure."""

        # Call OpenAI to generate suggestions
        response = await openai_client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[
                {"role": "system", "content": "You are an expert prompt engineer specializing in content analysis prompts. You must return a JSON object with a 'suggestions' array containing exactly 3 prompt suggestions."},
                {"role": "user", "content": analysis_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.7
        )

        # Parse OpenAI response
        try:
            response_json = json.loads(response.choices[0].message.content)
            if not isinstance(response_json, dict) or "suggestions" not in response_json:
                logger.error(f"Unexpected response format from OpenAI: {response_json}")
                # Create a default suggestions array if the format is wrong
                suggestions = [{
                    "prompt_text": feedback_data["current_prompt"],
                    "explanation": "Maintaining current prompt due to processing error",
                    "focus_areas": ["Maintaining existing format"]
                }]
            else:
                suggestions = response_json["suggestions"]
                
            # Validate each suggestion has required fields
            for suggestion in suggestions:
                if not all(key in suggestion for key in ["prompt_text", "explanation", "focus_areas"]):
                    logger.error(f"Invalid suggestion format: {suggestion}")
                    raise ValueError("Invalid suggestion format")

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Error parsing OpenAI response: {str(e)}")
            logger.error(f"Raw response content: {response.choices[0].message.content}")
            raise HTTPException(status_code=500, detail="Failed to generate valid prompt suggestions")

        return {
            "current_prompt": {
                "id": str(active_prompt["_id"]),
                "prompt_id": active_prompt.get("prompt_id"),
                "prompt_text": active_prompt.get("prompt_text"),
                "feedback_summary": {
                    "total_reviews": feedback_data["total_feedback"],
                    "average_rating": feedback_data["average_rating"],
                    "category_ratings": rating_analysis
                }
            },
            "suggestions": suggestions
        }

    except Exception as e:
        logger.error(f"Error generating prompts: {str(e)}")
        logger.error(f"Full error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/activate-prompt")
async def activate_new_prompt(request: PromptActivateRequest):
    """Save and activate new prompt"""
    try:
        # Get current active prompt to determine next version
        current_prompt = await summary_prompt_collection.find_one({"active": True})
        current_version = current_prompt.get("prompt_id", "v1")
        next_version = f"v{int(current_version[1:]) + 1}"

        # Deactivate current prompt
        await summary_prompt_collection.update_many(
            {"active": True},
            {"$set": {"active": False}}
        )

        # Create and activate new prompt
        new_prompt = {
            "prompt_id": next_version,
            "prompt_text": request.prompt_text,
            "created_at": datetime.utcnow(),
            "active": True,
            "average_rating": 0,
            "total_feedback": 0
        }
        
        result = await summary_prompt_collection.insert_one(new_prompt)
        
        return {
            "id": str(result.inserted_id),
            "prompt_id": next_version,
            "prompt_text": request.prompt_text
        }

    except Exception as e:
        logger.error(f"Error activating prompt: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pending-reviews")
async def get_pending_reviews():
    """Get list of videos pending review"""
   # try:
        # Return list of videos needing review
   #     pass
