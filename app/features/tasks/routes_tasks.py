"""
Task Management System Module

This module provides task management functionality including creation, assignment,
tracking, and status updates for both employees and clients.

Features:
- Task creation and assignment
- Priority management
- Status tracking
- User permissions
- Search functionality
- Client integration

Data Model:
- Task structure
- Assignee types
- Status tracking
- Priority levels
- Visibility rules

Security:
- Role-based access
- Task visibility
- Data validation
- Error handling

Dependencies:
- FastAPI for routing
- MongoDB for storage
- Pydantic for validation
- datetime for tracking

Author: Snapped Development Team
"""

from fastapi import HTTPException, APIRouter, Depends
from pydantic import BaseModel, Field
from datetime import datetime, timezone
from typing import List, Optional
from app.shared.database import async_client
from bson import ObjectId
from app.shared.auth import get_current_user_group

router = APIRouter()

# Initialize database collection
tasks = async_client["Opps"]["Tasks"]

class TaskAssignee(BaseModel):
    """
    Task assignee data model.
    
    Represents an assignee for a task, which can be either an employee or client.
    
    Attributes:
        id (str): Unique identifier for the assignee
        name (str): Display name of the assignee
        type (str): Type of assignee ("employee" or "client")
        client_id (Optional[str]): Client ID if type is "client"
        employee_id (Optional[str]): Employee ID if type is "employee"
    """
    id: str
    name: str
    type: str  # "employee" or "client"
    client_id: Optional[str] = None
    employee_id: Optional[str] = None

class Task(BaseModel):
    """
    Task data model.
    
    Represents a task with its properties and metadata.
    
    Attributes:
        title (str): Task title
        description (str): Task description
        status (str): Current status ("active", "hold", "complete")
        priority (str): Priority level ("high", "medium", "low")
        due_date (str): Task due date
        assignees (List[TaskAssignee]): List of task assignees
        created_by (Optional[str]): User ID of task creator
        created_at (Optional[datetime]): Task creation timestamp
        updated_at (Optional[datetime]): Last update timestamp
        visible_to (List[str]): Groups that can see this task
    """
    title: str = Field(...)
    description: str = Field(...)
    status: str = Field(...)  # "active", "hold", "complete"
    priority: str = Field(...)  # "high", "medium", "low"
    due_date: str = Field(...)
    assignees: List[TaskAssignee] = Field(default=[])
    created_by: Optional[str] = Field(default=None)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    visible_to: List[str] = Field(default=[])  # Groups that can see this task

def can_access_task(task: dict, user_groups: List[str]) -> bool:
    # Admins can see all tasks
    if "admin" in user_groups:
        return True
        
    # If no specific visibility is set, only admins can see it
    if not task.get("visible_to"):
        return "admin" in user_groups
        
    # Check if user's groups overlap with task's visible_to groups
    return any(group in task["visible_to"] for group in user_groups)

@router.post("/tasks")
async def create_task(
    task: Task,
    user_groups: dict = Depends(get_current_user_group)
):
    """
    Create a new task with specified properties.
    
    Args:
        task (Task): Task data model
        user_groups (dict): User authentication and group data
        
    Returns:
        dict: Created task ID and status
        
    Raises:
        HTTPException: For validation or database errors
        
    Notes:
        - Sets creator and visibility
        - Processes assignees
        - Validates permissions
    """
    try:
        task_dict = task.dict()
        
        # Set the creator to the current user's ID
        task_dict["created_by"] = user_groups.get("user_id")
        
        # Set visibility based on user's groups
        if "admin" in user_groups.get("groups", []):
            task_dict["visible_to"] = task_dict.get("visible_to", user_groups.get("groups", []))
        else:
            # Non-admins can only create tasks visible to their own groups
            task_dict["visible_to"] = [
                group for group in user_groups.get("groups", [])
                if group != "admin"
            ]
        
        # Process assignees
        for assignee in task_dict['assignees']:
            if assignee['type'] == 'client':
                assignee['client_id'] = assignee['id']
                assignee['employee_id'] = None
            elif assignee['type'] == 'employee':
                assignee['employee_id'] = assignee['id']
                assignee['client_id'] = None
        
        result = await tasks.insert_one(task_dict)
        return {"status": "success", "task_id": str(result.inserted_id)}
    except Exception as e:
        print("Error in create_task:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks")
async def get_tasks(
    filter_type: Optional[str] = None,
    user_groups: dict = Depends(get_current_user_group)
):
    """
    Retrieve tasks based on filter and user permissions.
    
    Args:
        filter_type (Optional[str]): Filter for task type
        user_groups (dict): User authentication and group data
        
    Returns:
        dict: List of filtered tasks
        
    Raises:
        HTTPException: For database errors
        
    Notes:
        - Applies visibility filters
        - Handles priority tasks
        - Processes completion status
    """
    try:
        # Base query for task status
        base_query = {}
        if filter_type == "priority":
            base_query["priority"] = "high"
            base_query["status"] = {"$ne": "complete"}
        elif filter_type == "completed":
            base_query["status"] = "complete"
        else:
            base_query["status"] = {"$ne": "complete"}
            
        # Add visibility filters based on user permissions
        user_id = user_groups.get("user_id")
        groups = user_groups.get("groups", [])
        
        # User should see tasks if any of these conditions are met:
        # 1. They created the task
        # 2. They are assigned to the task
        # 3. They have permission through visible_to groups
        # 4. They are an admin
        visibility_query = {
            "$or": [
                {"created_by": user_id},
                {"assignees": {
                    "$elemMatch": {
                        "$or": [
                            {"employee_id": user_id},
                            {"client_id": user_id}
                        ]
                    }
                }},
                {"visible_to": {"$in": groups}}
            ]
        }
        
        # If user is not admin, apply visibility restrictions
        if "admin" not in groups:
            base_query.update(visibility_query)
            
        print(f"Final query: {base_query}")  # Debug print
        
        cursor = tasks.find(base_query)
        task_list = await cursor.to_list(length=None)
        
        # Convert ObjectId to string
        for task in task_list:
            task["_id"] = str(task["_id"])
            if "assignees" not in task:
                task["assignees"] = []
                
        print(f"Found {len(task_list)} tasks")  # Debug print
        
        return {"tasks": task_list}
    except Exception as e:
        print(f"Error in get_tasks: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/search_assignees")
async def search_assignees(
    query: str,
    user_groups: dict = Depends(get_current_user_group)
):
    """
    Search for potential task assignees.
    
    Args:
        query (str): Search query string
        user_groups (dict): User authentication and group data
        
    Returns:
        dict: List of matching assignees
        
    Raises:
        HTTPException: For database errors
        
    Notes:
        - Searches employees and clients
        - Applies permission filters
        - Formats results consistently
    """
    try:
        groups = user_groups.get("groups", [])
        print(f"Searching assignees with query: {query}, groups: {groups}")

        results = []
        
        # Search employees
        employee_query = {
            "$or": [
                {"first_name": {"$regex": query, "$options": "i"}},
                {"last_name": {"$regex": query, "$options": "i"}},
                {"email": {"$regex": query, "$options": "i"}}
            ]
        }
        
        employees_cursor = async_client["Opps"]["Employees"].find(employee_query)
        employees = await employees_cursor.to_list(length=None)
        print(f"Found {len(employees)} matching employees")

        # Add employees to results
        results.extend([
            {
                "id": str(emp.get("employee_id", emp.get("_id"))),
                "name": f"{emp.get('first_name', '')} {emp.get('last_name', '')}",
                "type": "employee",
                "employee_id": str(emp.get("employee_id", emp.get("_id")))
            } for emp in employees
        ])

        # If user is admin or manager, also search clients
        if "ADMIN" in groups or "admin" in groups or "MANAGER" in groups or "manager" in groups:
            client_query = {
                "$or": [
                    {"First_Legal_Name": {"$regex": query, "$options": "i"}},
                    {"Last_Legal_Name": {"$regex": query, "$options": "i"}},
                    {"client_id": {"$regex": query, "$options": "i"}}
                ]
            }
            
            clients_cursor = async_client["ClientDb"]["ClientInfo"].find(client_query)
            clients = await clients_cursor.to_list(length=None)
            print(f"Found {len(clients)} matching clients")

            # Add clients to results
            results.extend([
                {
                    "id": client.get("client_id"),
                    "name": f"{client.get('First_Legal_Name', '')} {client.get('Last_Legal_Name', '')}",
                    "type": "client",
                    "client_id": client.get("client_id")
                } for client in clients if client.get("client_id")
            ])

        print(f"Returning {len(results)} total results")
        return {"assignees": results}
        
    except Exception as e:
        print(f"Error in search_assignees: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/tasks/{task_id}")
async def update_task(
    task_id: str,
    task: Task,
    user_groups: dict = Depends(get_current_user_group)
):
    try:
        # Check if task exists
        existing_task = await tasks.find_one({"_id": ObjectId(task_id)})
        if not existing_task:
            raise HTTPException(status_code=404, detail="Task not found")
            
        # Get user info
        user_id = user_groups.get("user_id")
        groups = user_groups.get("groups", [])
        
        # Check permissions
        can_update = (
            "admin" in groups or
            existing_task.get("created_by") == user_id or
            any(
                assignee.get("employee_id") == user_id or 
                assignee.get("client_id") == user_id 
                for assignee in existing_task.get("assignees", [])
            ) or
            any(group in existing_task.get("visible_to", []) for group in groups)
        )
        
        if not can_update:
            raise HTTPException(
                status_code=403,
                detail="You don't have permission to update this task"
            )

        # Prepare update data - only include fields that should be updated
        update_data = {
            "title": task.title,
            "description": task.description,
            "status": task.status,
            "priority": task.priority,
            "due_date": task.due_date,
            "assignees": [assignee.dict() for assignee in task.assignees],
            "updated_at": datetime.now(timezone.utc)
        }
        
        # Process assignees
        for assignee in update_data['assignees']:
            if assignee['type'] == 'client':
                assignee['client_id'] = assignee['id']
                assignee['employee_id'] = None
            elif assignee['type'] == 'employee':
                assignee['employee_id'] = assignee['id']
                assignee['client_id'] = None
                
        # Handle visibility permissions
        if "admin" in groups:
            update_data["visible_to"] = task.visible_to
        else:
            # Non-admins can't change visibility
            update_data["visible_to"] = existing_task.get("visible_to", [])
            
        print(f"Updating task {task_id} with data:", update_data)
            
        result = await tasks.update_one(
            {"_id": ObjectId(task_id)},
            {"$set": update_data}
        )
        
        if result.modified_count:
            updated_task = await tasks.find_one({"_id": ObjectId(task_id)})
            if updated_task:
                updated_task["_id"] = str(updated_task["_id"])
                return {"status": "success", "task": updated_task}
                
        raise HTTPException(status_code=404, detail="Task not found")
        
    except Exception as e:
        print(f"Error updating task: {str(e)}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/tasks/{task_id}")
async def delete_task(
    task_id: str,
    user_groups: dict = Depends(get_current_user_group)
):
    try:
        # Get user info and print for debugging
        groups = user_groups.get("groups", [])
        print(f"User groups for delete: {groups}")
        
        # Check if task exists
        existing_task = await tasks.find_one({"_id": ObjectId(task_id)})
        if not existing_task:
            raise HTTPException(
                status_code=404, 
                detail="Task not found"
            )

        # Check admin status and print for debugging
        is_admin = "ADMIN" in groups or "admin" in groups  # Case insensitive check
        print(f"Is admin: {is_admin}")

        if not is_admin:
            raise HTTPException(
                status_code=403, 
                detail="Only administrators can delete tasks"
            )

        result = await tasks.delete_one({"_id": ObjectId(task_id)})
        if result.deleted_count:
            return {"status": "success", "message": "Task deleted successfully"}
            
        raise HTTPException(
            status_code=404, 
            detail="Task not found"
        )
    except Exception as e:
        print(f"Error deleting task: {str(e)}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks/client/{client_id}")
async def get_client_tasks(
    client_id: str,
    user_groups: dict = Depends(get_current_user_group)
):
    try:
        print(f"Getting tasks for client: {client_id}")  # Add debug log
        
        base_query = {
            "assignees": {
                "$elemMatch": {
                    "type": "client",
                    "client_id": client_id
                }
            }
        }
        
        # Fix: Access groups from the dict correctly
        groups = user_groups.get("groups", [])
        if "admin" not in groups:
            base_query["visible_to"] = {"$in": groups}
        
        print(f"Query: {base_query}")  # Add debug log
        
        cursor = tasks.find(base_query)
        task_list = await cursor.to_list(length=None)
        
        print(f"Found {len(task_list)} tasks")  # Add debug log
        
        # Convert ObjectId to string
        for task in task_list:
            task["_id"] = str(task["_id"])
            
        return {"status": "success", "tasks": task_list}
    except Exception as e:
        print(f"Error in get_client_tasks: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
    
    