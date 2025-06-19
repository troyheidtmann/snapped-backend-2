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

from fastapi import APIRouter, HTTPException, status, Depends, Security
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from app.shared.database import message_store
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
import logging
import hashlib
from bson import ObjectId
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from app.features.tasks.routes_tasks import get_current_user_group
from app.shared.auth import filter_by_partner
from app.shared.database import async_client

# Initialize router with prefix and security
router = APIRouter()

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

class ReviewStatus(BaseModel):
    """
    Model for review status updates.
    
    Attributes:
        action (str): Action to take ('delete' or 'add')
        item_id (str): ID of the task or note
    """
    action: str
    item_id: str

def generate_id(content: str, date: str) -> str:
    """Generate a unique ID for a note or task."""
    hash_input = f"{content}{date}".encode('utf-8')
    return hashlib.md5(hash_input).hexdigest()

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
                    for note in notes:
                        note_data = {
                            "id": str(note.get("_id", "")) if isinstance(note, dict) else "",
                            "content": note if isinstance(note, str) else note.get("content", ""),
                            "date": date,
                            "session_id": session.get("session_id", ""),
                            "timestamp": session.get("timestamp", "")
                        }
                        all_notes.append(note_data)
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
                # Get unreviewed tasks
                tasks = session.get("tasks", [])
                if isinstance(tasks, list):
                    for task in tasks:
                        if isinstance(task, str):
                            task_id = generate_id(task, date)
                            task_data = {
                                "title": task,
                                "_id": task_id,
                                "assignees": [
                                    {
                                        "client_id": client_id,
                                        "id": client_id,
                                        "type": "client"
                                    }
                                ],
                                "priority": "medium",
                                "description": task,
                                "timestamp": session.get("timestamp", ""),
                                "date": date,
                                "session_id": session.get("session_id", ""),
                                "status": "active",
                                "created_by": session.get("created_by", client_id)
                            }
                            all_tasks.append(task_data)
                
                # Get reviewed tasks
                reviewed_tasks = session.get("reviewed_tasks", [])
                if isinstance(reviewed_tasks, list):
                    for reviewed_task in reviewed_tasks:
                        if isinstance(reviewed_task, dict) and "content" in reviewed_task:
                            task = reviewed_task["content"]
                            task_id = generate_id(task, date)
                            task_data = {
                                "title": task,
                                "_id": task_id,
                                "assignees": [
                                    {
                                        "client_id": client_id,
                                        "id": client_id,
                                        "type": "client"
                                    }
                                ],
                                "priority": "medium",
                                "description": task,
                                "timestamp": session.get("timestamp", ""),
                                "date": date,
                                "session_id": session.get("session_id", ""),
                                "status": "complete",  # Mark reviewed tasks as complete
                                "created_by": session.get("created_by", client_id),
                                "completed_by": reviewed_task.get("completed_by"),
                                "completed_at": reviewed_task.get("reviewed_at")
                            }
                            all_tasks.append(task_data)

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

@router.get("/unreviewed-notes/{client_id}")
async def get_unreviewed_notes(client_id: str):
    """
    Retrieve all AI-generated notes for a client across all dates.
    
    Args:
        client_id (str): Client identifier
        
    Returns:
        dict: Collection of all notes
        
    Raises:
        HTTPException: For database errors
    """
    logger.info(f"Fetching all notes for client: {client_id}")
    
    try:
        result = await message_store.find_one(
            {"user_id": client_id},
            max_time_ms=5000  # 5 second timeout
        )
        
        if not result or not isinstance(result, dict):
            logger.info("No valid results found in database")
            return {"notes": []}

        # Get all notes across all sessions
        all_notes = []
        sessions = result.get("sessions", {})
        
        if not isinstance(sessions, dict):
            logger.warning(f"Sessions is not a dictionary: {type(sessions)}")
            return {"notes": []}
        
        for date, date_sessions in sessions.items():
            if not isinstance(date_sessions, list):
                logger.warning(f"Date sessions for {date} is not a list: {type(date_sessions)}")
                continue
                
            for session in date_sessions:
                if not isinstance(session, dict):
                    logger.warning(f"Session is not a dictionary: {type(session)}")
                    continue
                    
                if session.get("type") == "ai_analysis":
                    notes = session.get("notes", [])
                    if isinstance(notes, list):
                        for note in notes:
                            if isinstance(note, str):
                                note_id = generate_id(note, date)
                                note_data = {
                                    "id": note_id,
                                    "content": note,
                                    "date": date,
                                    "session_id": session.get("session_id", ""),
                                    "timestamp": session.get("timestamp", "")
                                }
                                all_notes.append(note_data)

        logger.info(f"Found {len(all_notes)} total notes")
        return {"notes": all_notes}

    except Exception as e:
        logger.error(f"Error fetching notes: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.get("/unreviewed-tasks/{client_id}")
async def get_unreviewed_tasks(
    client_id: str, 
    filter_type: Optional[str] = None,
    tab: Optional[str] = None,  # Added for iOS app compatibility
    user_groups: dict = Depends(get_current_user_group)
):
    """
    Retrieve all AI-generated tasks for a client across all dates.
    
    Args:
        client_id (str): Client identifier
        filter_type (Optional[str]): Filter type ("priority", "completed", or None for active)
        tab (Optional[str]): Tab name from iOS app ("completed", "priority", or None)
        user_groups (dict): User authentication data
        
    Returns:
        dict: Collection of all tasks
        
    Raises:
        HTTPException: For database errors or unauthorized access
    """
    logger.info(f"Fetching all tasks for client: {client_id}, filter: {filter_type}, tab: {tab}")
    
    # Check client access permission
    filter_query = await filter_by_partner(user_groups)
    if filter_query:  # If not admin (empty filter = admin)
        # Check if user has access to this client
        client_access = await async_client["ClientDb"]["ClientInfo"].find_one({
            "client_id": client_id,
            **filter_query
        })
        if not client_access:
            raise HTTPException(status_code=403, detail="Not authorized to access this client")
    
    try:
        # Get AI-generated tasks from messages
        result = await message_store.find_one(
            {"user_id": client_id},
            max_time_ms=5000
        )
        
        all_tasks = []
        
        # Process AI-generated tasks
        if result and isinstance(result, dict):
            sessions = result.get("sessions", {})
            
            if isinstance(sessions, dict):
                for date, date_sessions in sessions.items():
                    if isinstance(date_sessions, list):
                        for session in date_sessions:
                            if isinstance(session, dict) and session.get("type") == "ai_analysis":
                                tasks = session.get("tasks", [])
                                if isinstance(tasks, list):
                                    for task in tasks:
                                        if isinstance(task, str):
                                            task_id = generate_id(task, date)
                                            task_data = {
                                                "_id": str(task_id),
                                                "title": str(task),
                                                "description": str(task),
                                                "status": "active",
                                                "priority": "high",
                                                "due_date": str(date),
                                                "assignees": [
                                                    {
                                                        "id": str(client_id),
                                                        "name": f"Client {client_id}",
                                                        "type": "client",
                                                        "client_id": str(client_id),
                                                        "employee_id": None
                                                    }
                                                ]
                                            }
                                            all_tasks.append(task_data)

        return {"tasks": all_tasks}

    except Exception as e:
        logger.error(f"Error fetching tasks: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.post("/review-note-status/{client_id}")
async def update_note_review_status(client_id: str, review: ReviewStatus):
    """
    Update the review status of a note.
    
    Args:
        client_id (str): Client identifier
        review (ReviewStatus): Review status update data
        
    Returns:
        dict: Success status
        
    Raises:
        HTTPException: For database errors or invalid actions
    """
    logger.info(f"Updating note review status for client: {client_id}")
    
    try:
        if review.action not in ["delete", "add"]:
            raise HTTPException(status_code=400, detail="Invalid action. Must be 'delete' or 'add'")

        # Find the note content using the ID
        result = await message_store.find_one(
            {"user_id": client_id},
            max_time_ms=5000
        )
        
        if not result:
            raise HTTPException(status_code=404, detail="Client not found")
            
        sessions = result.get("sessions", {})
        note_content = None
        note_date = None
        
        for date, date_sessions in sessions.items():
            for session in date_sessions:
                if session.get("type") == "ai_analysis":
                    notes = session.get("notes", [])
                    for note in notes:
                        if isinstance(note, str):
                            note_id = generate_id(note, date)
                            if note_id == review.item_id:
                                note_content = note
                                note_date = date
                                break
                    if note_content:
                        break
            if note_content:
                break
                
        if not note_content:
            raise HTTPException(status_code=404, detail="Note not found")

        # Update the review status in the database
        if review.action == "add":
            # Move the note to reviewed_notes array
            result = await message_store.update_one(
                {
                    "user_id": client_id,
                    "sessions": {
                        "$elemMatch": {
                            "type": "ai_analysis",
                            "notes": note_content
                        }
                    }
                },
                {
                    "$pull": {
                        "sessions.$[session].notes": note_content
                    },
                    "$push": {
                        "sessions.$[session].reviewed_notes": {
                            "content": note_content,
                            "reviewed_at": datetime.utcnow()
                        }
                    }
                },
                array_filters=[{"session.type": "ai_analysis"}]
            )
        else:
            # Delete the note
            result = await message_store.update_one(
                {
                    "user_id": client_id,
                    "sessions": {
                        "$elemMatch": {
                            "type": "ai_analysis",
                            "notes": note_content
                        }
                    }
                },
                {
                    "$pull": {
                        "sessions.$[session].notes": note_content
                    }
                },
                array_filters=[{"session.type": "ai_analysis"}]
            )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Note not found")

        return {"status": "success", "message": f"Note review status updated to {review.action}"}

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error updating note review status: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.post("/review-task-status/{client_id}")
async def update_task_review_status(client_id: str, review: ReviewStatus, user_groups: dict = Depends(get_current_user_group)):
    """
    Update the review status of a task.
    Also handles task completion in the main task system.
    
    Args:
        client_id (str): Client identifier
        review (ReviewStatus): Review status update data
        user_groups (dict): User authentication data
        
    Returns:
        dict: Success status
        
    Raises:
        HTTPException: For database errors or invalid actions
    """
    logger.info(f"Updating task review status for client: {client_id}")
    
    try:
        if review.action not in ["delete", "add"]:
            raise HTTPException(status_code=400, detail="Invalid action. Must be 'delete' or 'add'")

        # First try to find and update in the main task system
        from app.features.tasks.routes_tasks import tasks, complete_task  # Import tasks collection and complete_task function
        from app.features.tasks.models import TaskCompletion
        
        try:
            # Try to interpret the item_id as a task ID
            task = await tasks.find_one({"_id": ObjectId(review.item_id)})
            if task:
                # This is a task from the main system, complete it
                completion = TaskCompletion(
                    hours=0,  # Default to 0 since we don't have this info
                    minutes=30,  # Default to 30 minutes
                    notes="Completed via mobile app"
                )
                return await complete_task(review.item_id, completion, user_groups)
        except:
            # Not a valid ObjectId, continue with message system
            pass

        # If not found in main system, try message system
        result = await message_store.find_one(
            {"user_id": client_id},
            max_time_ms=5000
        )
        
        if not result:
            raise HTTPException(status_code=404, detail="Client not found")
            
        sessions = result.get("sessions", {})
        task_content = None
        task_date = None
        
        for date, date_sessions in sessions.items():
            for session in date_sessions:
                if session.get("type") == "ai_analysis":
                    tasks = session.get("tasks", [])
                    for task in tasks:
                        if isinstance(task, str):
                            task_id = generate_id(task, date)
                            if task_id == review.item_id:
                                task_content = task
                                task_date = date
                                break
                    if task_content:
                        break
            if task_content:
                break
                
        if not task_content:
            raise HTTPException(status_code=404, detail="Task not found")

        # Update the review status in the database
        if review.action == "add":
            # Move the task to reviewed_tasks array
            result = await message_store.update_one(
                {
                    "user_id": client_id,
                    "sessions": {
                        "$elemMatch": {
                            "type": "ai_analysis",
                            "tasks": task_content
                        }
                    }
                },
                {
                    "$pull": {
                        "sessions.$[session].tasks": task_content
                    },
                    "$push": {
                        "sessions.$[session].reviewed_tasks": {
                            "content": task_content,
                            "reviewed_at": datetime.utcnow(),
                            "completed_by": user_groups.get("user_id")
                        }
                    }
                },
                array_filters=[{"session.type": "ai_analysis"}]
            )
        else:
            # Delete the task
            result = await message_store.update_one(
                {
                    "user_id": client_id,
                    "sessions": {
                        "$elemMatch": {
                            "type": "ai_analysis",
                            "tasks": task_content
                        }
                    }
                },
                {
                    "$pull": {
                        "sessions.$[session].tasks": task_content
                    }
                },
                array_filters=[{"session.type": "ai_analysis"}]
            )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Task not found")

        return {"status": "success", "message": f"Task review status updated to {review.action}"}

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error updating task review status: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}") 