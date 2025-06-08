"""
Message Management API - Communication and AI Analysis System

This module provides FastAPI routes for managing messages, AI analysis notes,
and task tracking in the Snapped platform. It handles both direct messaging
between users and AI-generated content analysis.

Features:
--------
1. Message Management:
   - Message creation and retrieval
   - Read status tracking
   - Filtered message queries
   - Message deletion

2. AI Analysis:
   - Client-specific notes
   - Date-based filtering
   - Task tracking
   - Performance insights

Data Model:
----------
Message Structure:
- Basic: content, sender, recipient
- Metadata: id, read status, timestamp
- AI Analysis: notes, tasks, sessions
- Date-based organization

Security:
--------
- Input validation via Pydantic
- Error handling
- Logging system
- Query timeouts

Dependencies:
-----------
- FastAPI: Web framework
- MongoDB: Data storage
- Pydantic: Data validation
- Motor: Async MongoDB driver
- Logging: Debug tracking

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException, status
from typing import List, Optional
from pydantic import BaseModel
from app.shared.database import message_store
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
import logging

# Initialize router with prefix
router = APIRouter(prefix="/api/messages")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define message schema
class MessageBase(BaseModel):
    """
    Base message model with core attributes.
    
    Attributes:
        content (str): Message content
        sender (str): Message sender identifier
        recipient (str): Message recipient identifier
    """
    content: str
    sender: str
    recipient: str

class MessageCreate(MessageBase):
    """
    Message creation model inheriting from base.
    Used for incoming message requests.
    """
    pass

class Message(MessageBase):
    """
    Complete message model with additional attributes.
    
    Attributes:
        id (int): Unique message identifier
        read (bool): Message read status
    """
    id: int
    read: bool = False

    class Config:
        from_attributes = True

@router.post("/", response_model=Message, status_code=status.HTTP_201_CREATED)
async def create_message(message: MessageCreate):
    """
    Create a new message in the system.
    
    Automatically assigns the next available ID and initializes
    read status as false.
    
    Args:
        message (MessageCreate): Message data including content and participants
        
    Returns:
        Message: Created message with assigned ID
        
    Raises:
        HTTPException: For database errors
    """
    # Get the next message ID
    last_message = await message_store.find_one(sort=[("id", -1)])
    next_id = (last_message["id"] + 1) if last_message else 1
    
    # Create new message document
    new_message = Message(
        id=next_id,
        **message.model_dump()
    ).model_dump()
    
    await message_store.insert_one(new_message)
    return new_message

@router.get("/", response_model=List[Message])
async def get_messages(sender: Optional[str] = None, recipient: Optional[str] = None):
    """
    Retrieve messages with optional filtering.
    
    Supports filtering by sender, recipient, or both.
    Returns all messages if no filters provided.
    
    Args:
        sender (Optional[str]): Filter by sender
        recipient (Optional[str]): Filter by recipient
        
    Returns:
        List[Message]: Matching messages
        
    Notes:
        - Returns empty list if no matches
        - Ordered by message ID
    """
    query = {}
    if sender and recipient:
        query = {"sender": sender, "recipient": recipient}
    elif sender:
        query = {"sender": sender}
    elif recipient:
        query = {"recipient": recipient}
    
    cursor = message_store.find(query)
    messages = await cursor.to_list(length=None)
    return messages

@router.get("/ai-notes/{client_id}/{date}")
async def get_ai_notes(client_id: str, date: str):
    """
    Retrieve AI-generated analysis notes for a client on a specific date.
    
    Fetches and aggregates AI analysis notes from all sessions on the
    specified date for the given client.
    
    Args:
        client_id (str): Client identifier
        date (str): Target date in YYYY-MM-DD format
        
    Returns:
        dict: Collection of notes with:
            - notes: List of AI analysis notes
            
    Raises:
        HTTPException: For invalid dates or database errors
        
    Notes:
        - 5-second query timeout
        - Returns empty list if no notes found
        - Handles both single notes and note arrays
    """
    logger.info(f"Received request for client_id: {client_id}, date: {date}")
    
    try:
        logger.info("Attempting to parse date")
        query_date = datetime.strptime(date, "%Y-%m-%d")
        logger.info(f"Date parsed successfully: {query_date}")
        
        logger.info("Executing MongoDB query")
        result = await message_store.find_one(
            {"user_id": client_id},
            max_time_ms=5000  # 5 second timeout
        )
        
        logger.info(f"MongoDB query completed. Result found: {result is not None}")

        if not result:
            logger.info("No results found in database")
            return {"notes": []}

        logger.info("Processing result data")
        sessions = result.get("sessions", {})
        logger.info(f"Found sessions: {bool(sessions)}")
        
        # Only get notes from the exact requested date
        all_notes = []
        date_sessions = sessions.get(date, [])
        
        for session in date_sessions:
            if session.get("type") == "ai_analysis":
                notes = session.get("notes", [])
                if isinstance(notes, list):
                    all_notes.extend(notes)
                else:
                    all_notes.append(notes)
                logger.info(f"Added notes from date {date}")

        logger.info(f"Total notes found: {len(all_notes)}")
        return {"notes": all_notes}

    except ValueError as ve:
        logger.error(f"Date parsing error: {ve}")
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.get("/tasks/{client_id}/{date}")
async def get_tasks(client_id: str, date: str):
    """
    Retrieve AI-generated tasks for a client on a specific date.
    
    Fetches and aggregates AI analysis tasks from all sessions on the
    specified date for the given client.
    
    Args:
        client_id (str): Client identifier
        date (str): Target date in YYYY-MM-DD format
        
    Returns:
        dict: Collection of tasks with:
            - tasks: List of AI analysis tasks
            
    Raises:
        HTTPException: For invalid dates or database errors
        
    Notes:
        - 5-second query timeout
        - Returns empty list if no tasks found
        - Handles both single tasks and task arrays
    """
    logger.info(f"Received request for client_id: {client_id}, date: {date}")
    
    try:
        logger.info("Attempting to parse date")
        query_date = datetime.strptime(date, "%Y-%m-%d")
        logger.info(f"Date parsed successfully: {query_date}")
        
        logger.info("Executing MongoDB query")
        result = await message_store.find_one(
            {"user_id": client_id},
            max_time_ms=5000  # 5 second timeout
        )
        
        logger.info(f"MongoDB query completed. Result found: {result is not None}")

        if not result:
            logger.info("No results found in database")
            return {"tasks": []}

        logger.info("Processing result data")
        sessions = result.get("sessions", {})
        logger.info(f"Found sessions: {bool(sessions)}")
        
        # Only get tasks from the exact requested date
        all_tasks = []
        date_sessions = sessions.get(date, [])
        
        for session in date_sessions:
            if session.get("type") == "ai_analysis":
                tasks = session.get("tasks", [])
                if isinstance(tasks, list):
                    all_tasks.extend(tasks)
                else:
                    all_tasks.append(tasks)
                logger.info(f"Added tasks from date {date}")

        logger.info(f"Total tasks found: {len(all_tasks)}")
        return {"tasks": all_tasks}

    except ValueError as ve:
        logger.error(f"Date parsing error: {ve}")
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.get("/{message_id}", response_model=Message)
async def get_message(message_id: int):
    """
    Retrieve a specific message by ID.
    
    Args:
        message_id (int): Message identifier
        
    Returns:
        Message: Message details if found
        
    Raises:
        HTTPException: If message not found
    """
    message = await message_store.find_one({"id": message_id})
    if not message:
        raise HTTPException(status_code=404, detail="Message not found")
    return message

@router.put("/{message_id}/read", response_model=Message)
async def mark_message_as_read(message_id: int):
    """
    Mark a message as read.
    
    Updates the read status of a message and returns the updated message.
    
    Args:
        message_id (int): Message identifier
        
    Returns:
        Message: Updated message with read=True
        
    Raises:
        HTTPException: If message not found
    """
    result = await message_store.find_one_and_update(
        {"id": message_id},
        {"$set": {"read": True}},
        return_document=True
    )
    if not result:
        raise HTTPException(status_code=404, detail="Message not found")
    return result

@router.delete("/{message_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_message(message_id: int):
    """
    Delete a message from the system.
    
    Args:
        message_id (int): Message identifier
        
    Returns:
        None
        
    Raises:
        HTTPException: If message not found
        
    Notes:
        - Operation is irreversible
        - Returns 204 on success
    """
    result = await message_store.delete_one({"id": message_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Message not found") 