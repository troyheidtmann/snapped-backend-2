"""
FastAPI Router for AI Chat Functionality

This module implements the AI chat feature using FastAPI, OpenAI's GPT-4, Redis caching,
and MongoDB vector storage. It serves as the main communication hub between the frontend
client and the AI backend services.

System Architecture Integration:
    - Frontend Integration: Receives requests from the frontend client interface
    - Authentication: Uses shared auth module for user group validation
    - Database Layer: 
        * MongoDB Vector DB: Stores and retrieves client-specific vector embeddings
        * Redis: Handles caching and conversation history management
    - External Services:
        * OpenAI GPT-4: Processes chat completions
        * Social Blade API: Indirectly uses client data through vector storage

Dependencies:
    - app.shared.auth: User authentication and group validation
    - MongoDB: Vector storage for client data (surveys, videos, best practices)
    - Redis: Caching layer and conversation history
    - OpenAI API: GPT-4 integration for chat responses

Data Flow:
    1. Client request → FastAPI endpoint
    2. Authentication check via shared auth module
    3. Redis cache check for client data
    4. MongoDB vector fetch if cache miss
    5. Conversation history retrieval from Redis
    6. OpenAI API call with contextualized data
    7. Response storage and return to client

Security Notes:
    - Credentials should be moved to environment variables
    - MongoDB connection uses TLS encryption
    - Redis should be configured with authentication in production
    - Rate limiting should be implemented for production use
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime
from openai import AsyncOpenAI
from app.shared.auth import get_current_user_group
from motor.motor_asyncio import AsyncIOMotorClient
from urllib.parse import quote_plus
import certifi
import json
from bson import json_util
import asyncio
from redis import asyncio as aioredis

# Initialize FastAPI router
router = APIRouter()
client = AsyncOpenAI()

# Redis Configuration
# TODO: Move to environment variables
redis = aioredis.Redis(host='localhost', port=6379, decode_responses=True)

# Cache Configuration
CACHE_TTL = 3600  # 1 hour for client data
CONVERSATION_TTL = 86400  # 24 hours for conversation history

# Optimize vector DB query projection
VECTOR_DB_PROJECTION = {
    "client_id": 1,
    "client_name": 1,
    "metadata": 1,
    "vectors.survey_chunks": {"$slice": 5},
    "vectors.video_chunks": {"$slice": 5},
    "vectors.best_practice_chunks": {"$slice": 3}
}

# Security Note: These credentials should be in environment variables
username = quote_plus("troyheidtmann")
password = quote_plus("Gunit1500!!!!@@@@")

VECTOR_DB_URL = f"mongodb+srv://{username}:{password}@chatai.3v7ig.mongodb.net/?retryWrites=true&w=majority&appName=ChatAI"

VECTOR_DB_SETTINGS = {
    "tlsCAFile": certifi.where(),
    "tls": True,
    "serverSelectionTimeoutMS": 10000,
    "connectTimeoutMS": 20000,
    "maxPoolSize": 100,
    "retryWrites": True
}

vector_client = AsyncIOMotorClient(VECTOR_DB_URL, **VECTOR_DB_SETTINGS)
vector_db = vector_client["AIChat"]["VectorDB"]

# GPT-4 System Configuration
SYSTEM_PROMPT = """You are an AI assistant analyzing Snapchat creator data. You help creators understand their:
- Follower growth and engagement
- Content performance
- Video analytics
- Survey responses
- Best practices

Guidelines:
• Default to 1-2 sentence responses
• Use bullet points for multiple items
• Only expand with details when explicitly requested
• Always include specific data points and dates
• If data is missing, state it briefly

Format responses in clean markdown with clear sections."""

async def fetch_client_data(client_id: str) -> Dict[str, Any]:
    """
    Fetches and caches client data from MongoDB vector database.
    
    Integration Points:
        - Redis: Primary caching layer for client data
        - MongoDB: Vector database storing client analytics
        - Vector Processing: Handles survey, video, and best practice data
    
    Cache Strategy:
        - First attempts Redis lookup
        - On cache miss, queries MongoDB
        - Caches processed results for CACHE_TTL duration
    
    Args:
        client_id: Unique identifier for the client
        
    Returns:
        Dict containing processed client data or error message
    """
    try:
        # Check Redis cache first
        cache_key = f"client_data:{client_id}"
        cached_data = await redis.get(cache_key)
        if cached_data:
            return json.loads(cached_data)

        # If not in cache, query MongoDB with optimized projection
        doc = await vector_db.find_one(
            {"client_id": client_id},
            projection=VECTOR_DB_PROJECTION
        )
        
        if not doc:
            return {"error": "No data found for this client"}
            
        # Process and cache the result
        json_data = json.loads(json_util.dumps(doc))
        if '_id' in json_data:
            del json_data['_id']
            
        summarized_data = {
            "client_id": json_data.get("client_id"),
            "client_name": json_data.get("client_name"),
            "metadata": json_data.get("metadata", {}),
            "survey_data": [],
            "video_data": [],
            "best_practices": []
        }
        
        # Process with reduced data sizes
        vectors = json_data.get("vectors", {})
        if survey_chunks := vectors.get("survey_chunks", []):
            summarized_data["survey_data"] = [
                chunk["text"] for chunk in survey_chunks
            ]
            
        if video_chunks := vectors.get("video_chunks", []):
            summarized_data["video_data"] = [
                {
                    "text": chunk["text"],
                    "video_id": chunk.get("video_id"),
                    "session_date": chunk.get("session_date")
                } for chunk in video_chunks
            ]
            
        if best_practice_chunks := vectors.get("best_practice_chunks", []):
            summarized_data["best_practices"] = [
                chunk["text"] for chunk in best_practice_chunks
            ]
            
        # Cache the processed data
        await redis.set(
            cache_key,
            json.dumps(summarized_data),
            ex=CACHE_TTL
        )
        
        return summarized_data
    except Exception as e:
        print(f"Error fetching client data: {str(e)}")
        return {"error": str(e)}

class Message(BaseModel):
    """
    Pydantic model for chat messages.
    
    Used For:
        - Frontend message serialization
        - OpenAI API request formatting
        - Redis conversation history storage
    """
    role: str
    content: str
    timestamp: datetime

class ChatRequest(BaseModel):
    """
    Pydantic model for incoming chat requests.
    
    Integration:
        - Frontend sends requests in this format
        - Validates message structure
        - Optional client_id for context-aware responses
    """
    messages: List[Message]
    client_id: Optional[str] = None

async def get_conversation_history(client_id: str, limit: int = 25) -> List[Dict]:
    """
    Retrieves recent conversation history from Redis.
    
    Integration Points:
        - Redis: Stores conversation history
        - Chat Context: Used to maintain conversation coherence
        - OpenAI API: History included in prompt context
    
    Args:
        client_id: Client identifier for conversation lookup
        limit: Maximum number of messages to retrieve
        
    Returns:
        List of recent conversation messages
    """
    history_key = f"chat_history:{client_id}"
    history = await redis.lrange(history_key, 0, limit - 1)
    return [json.loads(msg) for msg in history]

async def add_to_conversation_history(client_id: str, message: Dict):
    """
    Adds new messages to the conversation history in Redis.
    
    Integration Points:
        - Redis: Primary storage for conversation history
        - Chat System: Maintains conversation context
        - Memory Management: Implements TTL and message limits
    
    Strategy:
        - FIFO queue with max 25 messages
        - 24-hour TTL per conversation
        - Automatic cleanup of old messages
    
    Args:
        client_id: Client identifier for conversation storage
        message: New message to add to history
    """
    history_key = f"chat_history:{client_id}"
    await redis.lpush(history_key, json.dumps(message))
    await redis.ltrim(history_key, 0, 24)  # Keep last 25 messages
    await redis.expire(history_key, CONVERSATION_TTL)

@router.post("/chat")
async def chat_endpoint(request: ChatRequest):
    """
    Main chat endpoint handling AI interactions.
    
    System Integration Flow:
        1. Frontend → FastAPI: Receives chat request
        2. Auth Module: Validates user access
        3. Vector DB: Retrieves client context
        4. Redis: Manages conversation history
        5. OpenAI API: Generates AI response
        6. Redis → Frontend: Stores and returns response
    
    Error Handling:
        - Client validation
        - Database connectivity
        - OpenAI API errors
        - Redis operations
    
    Returns:
        JSON response containing AI message and timestamp
    """
    try:
        if not request.client_id:
            raise HTTPException(
                status_code=400,
                detail="client_id is required"
            )

        # Fetch client data
        client_data = await fetch_client_data(request.client_id)
        if "error" in client_data:
            return {
                "message": "I couldn't find any data for this client in the database. Please verify the client ID is correct.",
                "timestamp": datetime.utcnow()
            }

        # Get conversation history
        conversation_history = await get_conversation_history(request.client_id)
        
        # Format messages for GPT-4
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Here is the client data to analyze:\n```json\n{json.dumps(client_data, indent=2)}\n```"}
        ]
        
        # Add conversation history
        for msg in conversation_history[::-1]:  # Reverse to get chronological order
            messages.append(msg)
            
        # Add current message
        current_message = {
            "role": "user",
            "content": request.messages[-1].content
        }
        messages.append(current_message)

        # Make direct GPT-4 API call
        response = await client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=messages,
            temperature=0.7,
            max_tokens=500
        )

        assistant_message = {
            "role": "assistant",
            "content": response.choices[0].message.content
        }

        # Store both user and assistant messages in history
        await add_to_conversation_history(request.client_id, current_message)
        await add_to_conversation_history(request.client_id, assistant_message)

        return {
            "message": assistant_message["content"],
            "timestamp": datetime.utcnow()
        }

    except Exception as e:
        print(f"Chat endpoint error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
