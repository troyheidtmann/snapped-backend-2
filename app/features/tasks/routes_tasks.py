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
- Automated recurring tasks

Data Model:
- Task structure
- Assignee types
- Status tracking
- Priority levels
- Visibility rules
- Task templates

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
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from app.shared.database import async_client
from bson import ObjectId
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Security
import jwt
from app.shared.auth import filter_by_partner
from .task_notifications import router as notifications_router

router = APIRouter()
security = HTTPBearer()

async def get_current_user_group(credentials: HTTPAuthorizationCredentials = Security(security)) -> dict:
    """
    Extract user groups and ID from JWT token.
    Now handles both 'groups' and 'cognito:groups' formats.
    """
    try:
        token = credentials.credentials
        # Decode without verification since we trust Cognito's signed tokens
        decoded = jwt.decode(token, options={"verify_signature": False})
        
        # Try both group formats
        groups = decoded.get("cognito:groups", decoded.get("groups", ["DEFAULT"]))
        groups = [g.upper() for g in groups]  # Normalize to uppercase
        
        # Get user ID
        user_id = decoded.get("custom:UserID", "")
        
        return {
            "groups": groups,
            "user_id": user_id
        }
    except Exception as e:
        return {
            "groups": ["DEFAULT"],
            "user_id": ""
        }

# Initialize database collections
tasks = async_client["Opps"]["Tasks"]
task_templates = async_client["Opps"]["TaskTemplates"]

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

class TaskTemplate(BaseModel):
    """
    Task template for recurring tasks.
    
    Represents a template for automatically creating recurring tasks.
    
    Attributes:
        title (str): Template title
        description (str): Template description
        frequency (str): How often to create tasks ("daily" or "weekly")
        priority (str): Priority level ("high", "medium", "low")
        assignees (List[TaskAssignee]): Default assignees
        job_type (str): Type of job (e.g. "Content Team", "Talent Team")
        created_by (Optional[str]): User ID of template creator
        created_at (Optional[datetime]): Template creation timestamp
        updated_at (Optional[datetime]): Last update timestamp
        visible_to (List[str]): Groups that can see tasks created from this template
        is_active (bool): Whether the template is currently active
    """
    title: str = Field(...)
    description: str = Field(...)
    frequency: str = Field(...)  # "daily" or "weekly"
    priority: str = Field(...)  # "high", "medium", "low"
    assignees: List[TaskAssignee] = Field(default=[])
    job_type: str = Field(...)
    created_by: Optional[str] = Field(default=None)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    visible_to: List[str] = Field(default=[])
    is_active: bool = Field(default=True)

class TaskCompletion(BaseModel):
    """
    Task completion data model.
    
    Contains information about task completion including time spent.
    
    Attributes:
        hours (int): Hours spent on task
        minutes (int): Additional minutes spent
        notes (Optional[str]): Any completion notes
    """
    hours: int = Field(...)
    minutes: int = Field(...)
    notes: Optional[str] = None

def can_access_task(task: dict, user_groups: List[str], user_id: str) -> bool:
    """
    Check if a user can access a task based on their user_id and groups.
    User can access if they:
    1. Are an admin
    2. Created the task
    3. Are assigned to the task
    4. Have group visibility permissions
    """
    # Check admin access first
    if "admin" in user_groups:
        return True
        
    # Check if user created the task
    if task.get("created_by") == user_id:
        return True
        
    # Check if user is assigned to the task
    assignees = task.get("assignees", [])
    for assignee in assignees:
        if (assignee.get("employee_id") == user_id or 
            assignee.get("client_id") == user_id):
            return True
            
    # Finally check group visibility
    if task.get("visible_to"):
        return any(group in task["visible_to"] for group in user_groups)
        
    return False

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
    try:
        # Get user info
        user_id = user_groups.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="User ID not found")
            
        print(f"Getting tasks for user {user_id}, filter: {filter_type}")

        # Base query for task status
        base_query = {}
        if filter_type == "priority":
            base_query["priority"] = "high"
            base_query["$or"] = [{"status": {"$ne": "completed"}}, {"status": {"$ne": "Completed"}}]
        elif filter_type == "completed":
            base_query["$or"] = [{"status": "completed"}, {"status": "Completed"}]
        elif filter_type == "client":
            base_query["$or"] = [{"status": {"$ne": "completed"}}, {"status": {"$ne": "Completed"}}]
        else:
            base_query["$or"] = [{"status": {"$ne": "completed"}}, {"status": {"$ne": "Completed"}}]

        # User should see tasks if ANY of these conditions are met:
        # 1. They are assigned to the task (as employee or client)
        # 2. They created the task
        # 3. They completed the task
        # 4. They have permission through visible_to groups (only if above conditions aren't met)
        visibility_query = {
            "$or": [
                # Check if user is assigned as employee
                {"assignees": {
                    "$elemMatch": {
                        "type": "employee",
                        "employee_id": user_id
                    }
                }},
                # Check if user is assigned as client
                {"assignees": {
                    "$elemMatch": {
                        "type": "client",
                        "client_id": user_id
                    }
                }},
                # Check if user created the task
                {"created_by": user_id},
                # Check if user completed the task
                {"completed_by": user_id}
            ]
        }

        # Add group visibility as a fallback
        groups = user_groups.get("groups", ["DEFAULT"])  # Always include DEFAULT
        if not groups:  # If groups is empty, ensure DEFAULT is included
            groups = ["DEFAULT"]
        if "admin" in groups:
            # Admins can see all tasks - no need for visibility query
            final_query = base_query
        else:
            # Add group visibility to OR conditions
            visibility_query["$or"].append({"visible_to": {"$in": groups}})
            final_query = {
                "$and": [
                    base_query,
                    visibility_query
                ]
            }

        print(f"Final query: {final_query}")
        
        cursor = tasks.find(final_query)
        task_list = await cursor.to_list(length=None)
        
        # Convert ObjectId to string and ensure assignees exists
        for task in task_list:
            task["_id"] = str(task["_id"])
            if "assignees" not in task:
                task["assignees"] = []
                
        print(f"Found {len(task_list)} tasks")
        print(f"Task statuses: {[task.get('status') for task in task_list]}")
        
        return {"tasks": task_list}
        
    except Exception as e:
        print(f"Error in get_tasks: {str(e)}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/tasks/search_assignees")
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
                "id": str(emp.get("user_id", emp.get("_id"))),
                "name": f"{emp.get('first_name', '')} {emp.get('last_name', '')}",
                "type": "employee",
                "employee_id": str(emp.get("user_id", emp.get("_id")))
            } for emp in employees
        ])

        # Search clients with access restrictions
        client_query = {
            "$or": [
                {"First_Legal_Name": {"$regex": query, "$options": "i"}},
                {"Last_Legal_Name": {"$regex": query, "$options": "i"}},
                {"client_id": {"$regex": query, "$options": "i"}}
            ]
        }
        
        # Apply client access filter
        filter_query = await filter_by_partner(user_groups)
        if filter_query:  # If not admin (empty filter = admin)
            client_query = {"$and": [client_query, filter_query]}
        
        clients_cursor = async_client["ClientDb"]["ClientInfo"].find(client_query)
        clients = await clients_cursor.to_list(length=None)
        print(f"Found {len(clients)} matching clients (after filtering)")

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

@router.get("/search_assignees")  # Original endpoint for iOS app
async def search_assignees_legacy(
    query: str,
    user_groups: dict = Depends(get_current_user_group)
):
    """Legacy endpoint for iOS app compatibility."""
    return await search_assignees(query, user_groups)

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
        groups = user_groups.get("groups", ["DEFAULT"])
        
        # Check permissions using user_id
        if not can_access_task(existing_task, groups, user_id):
            raise HTTPException(
                status_code=403,
                detail="You don't have permission to update this task"
            )

        # Prepare update data
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
            update_data["visible_to"] = existing_task.get("visible_to", ["DEFAULT"])
            
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

@router.post("/task-templates")
async def create_task_template(
    template: TaskTemplate,
    user_groups: dict = Depends(get_current_user_group)
):
    """Create a new task template for recurring tasks."""
    try:
        template_dict = template.dict()
        template_dict["created_by"] = user_groups.get("user_id")
        template_dict["created_at"] = datetime.now(timezone.utc)
        template_dict["updated_at"] = datetime.now(timezone.utc)
        
        # Set visibility based on user's groups
        if "admin" in user_groups.get("groups", []):
            template_dict["visible_to"] = template_dict.get("visible_to", user_groups.get("groups", []))
        else:
            template_dict["visible_to"] = [
                group for group in user_groups.get("groups", [])
                if group != "admin"
            ]
            
        # Process assignees
        for assignee in template_dict['assignees']:
            if assignee['type'] == 'client':
                assignee['client_id'] = assignee['id']
                assignee['employee_id'] = None
            elif assignee['type'] == 'employee':
                assignee['employee_id'] = assignee['id']
                assignee['client_id'] = None
        
        result = await task_templates.insert_one(template_dict)
        return {"status": "success", "template_id": str(result.inserted_id)}
    except Exception as e:
        print("Error in create_task_template:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/task-templates")
async def get_task_templates(
    user_groups: dict = Depends(get_current_user_group)
):
    """Get all task templates visible to the user."""
    try:
        groups = user_groups.get("groups", [])
        query = {
            "$or": [
                {"visible_to": {"$in": groups}},
                {"created_by": user_groups.get("user_id")}
            ]
        }
        
        if "admin" in groups:
            query = {}  # Admins can see all templates
            
        templates = []
        async for template in task_templates.find(query):
            template["_id"] = str(template["_id"])
            templates.append(template)
            
        return templates
    except Exception as e:
        print("Error in get_task_templates:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/task-templates/{template_id}")
async def update_task_template(
    template_id: str,
    template: TaskTemplate,
    user_groups: dict = Depends(get_current_user_group)
):
    """Update an existing task template."""
    try:
        # Verify user has permission to update
        existing = await task_templates.find_one({"_id": ObjectId(template_id)})
        if not existing:
            raise HTTPException(status_code=404, detail="Template not found")
            
        if (existing["created_by"] != user_groups.get("user_id") and 
            "admin" not in user_groups.get("groups", [])):
            raise HTTPException(status_code=403, detail="Not authorized to update this template")

        # Prepare update data while preserving important fields
        template_dict = template.dict()
        update_data = {
            **existing,  # Keep all existing fields
            "title": template_dict["title"],
            "description": template_dict["description"],
            "frequency": template_dict["frequency"],
            "priority": template_dict["priority"],
            "job_type": template_dict["job_type"],
            "assignees": template_dict["assignees"],
            "is_active": template_dict["is_active"],
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
        
        # Remove the _id field as it can't be updated
        if '_id' in update_data:
            del update_data['_id']
        
        result = await task_templates.update_one(
            {"_id": ObjectId(template_id)},
            {"$set": update_data}
        )
        
        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Template not found")
            
        return {"status": "success"}
    except Exception as e:
        print("Error in update_task_template:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/task-templates/{template_id}/duplicate")
async def duplicate_task_template(
    template_id: str,
    user_groups: dict = Depends(get_current_user_group)
):
    """
    Create a duplicate of an existing task template.
    
    Args:
        template_id: ID of template to duplicate
        user_groups: User authentication data
        
    Returns:
        dict: New template ID
        
    Raises:
        HTTPException: For validation or database errors
    """
    try:
        # Get the original template
        original = await task_templates.find_one({"_id": ObjectId(template_id)})
        if not original:
            raise HTTPException(status_code=404, detail="Template not found")
            
        # Create new template dict from original
        new_template = {
            **original,
            "title": f"{original['title']} (Copy)",
            "created_by": user_groups.get("user_id"),
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        
        # Remove the _id field as it will be auto-generated
        new_template.pop("_id")
        
        # Insert the new template
        result = await task_templates.insert_one(new_template)
        
        return {
            "status": "success",
            "template_id": str(result.inserted_id)
        }
        
    except Exception as e:
        print("Error in duplicate_task_template:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/tasks/create-from-templates")
async def create_tasks_from_templates_endpoint(
    user_groups: dict = Depends(get_current_user_group)
):
    """
    Manual endpoint to create tasks from active templates.
    Can be called via API or curl for reliable scheduling.
    """
    try:
        # Check if user has admin permissions
        if "ADMIN" not in user_groups.get("groups", []):
            raise HTTPException(status_code=403, detail="Admin access required")
        
        created_count = 0
        errors = []
        
        async for template in task_templates.find({"is_active": True}):
            try:
                # For weekly tasks, only create on Sundays (weekday 6)
                if template["frequency"] == "weekly" and datetime.now().weekday() != 6:
                    continue
                
                # Set due date based on frequency
                if template["frequency"] == "daily":
                    due_date = datetime.now() + timedelta(days=1)
                else:  # weekly
                    due_date = datetime.now() + timedelta(days=7)
                
                # Create new task from template
                task_dict = {
                    "title": template["title"],
                    "description": template["description"],
                    "status": "active",
                    "priority": template["priority"],
                    "due_date": due_date.strftime("%Y-%m-%d"),
                    "assignees": template["assignees"],
                    "created_by": template["created_by"],
                    "created_at": datetime.now(timezone.utc),
                    "updated_at": datetime.now(timezone.utc),
                    "visible_to": template["visible_to"],
                    "template_id": str(template["_id"]),
                    "job_type": template["job_type"]
                }
                
                await tasks.insert_one(task_dict)
                created_count += 1
                print(f"Created task from template: {template['title']}")
                
            except Exception as template_error:
                error_msg = f"Error processing template {template.get('title', 'Unknown')}: {str(template_error)}"
                errors.append(error_msg)
                print(error_msg)
                continue
        
        return {
            "status": "success",
            "created_count": created_count,
            "errors": errors,
            "message": f"Created {created_count} tasks from templates"
        }
        
    except Exception as e:
        print(f"Error in create_tasks_from_templates_endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/tasks/{task_id}/complete")
async def complete_task(
    task_id: str,
    completion: TaskCompletion,
    user_groups: dict = Depends(get_current_user_group)
):
    try:
        # Get user info first - this is the only ID we really need
        user_id = user_groups.get("user_id")
        if not user_id:
            print("Error: No user_id found in token")
            raise HTTPException(status_code=401, detail="User ID not found")

        print(f"Processing task completion for user {user_id}")
            
        # Get the task
        try:
            task = await tasks.find_one({"_id": ObjectId(task_id)})
        except Exception as e:
            print(f"Error finding task: {str(e)}")
            raise HTTPException(status_code=404, detail="Invalid task ID format")
            
        if not task:
            print(f"Task {task_id} not found")
            raise HTTPException(status_code=404, detail="Task not found")
            
        print(f"Found task: {task.get('title')}")
        
        # Update task status - if they have the user_id and task exists, let them complete it
        print(f"Updating task status for user {user_id}")
        update_result = await tasks.update_one(
            {"_id": ObjectId(task_id)},
            {
                "$set": {
                    "status": "Completed",
                    "completed_at": datetime.now(timezone.utc),
                    "completed_by": user_id,
                    "completion_notes": completion.notes,
                    "time_spent": {
                        "hours": completion.hours,
                        "minutes": completion.minutes
                    }
                }
            }
        )
        
        if update_result.modified_count == 0:
            print(f"Failed to update task {task_id}")
            raise HTTPException(status_code=500, detail="Failed to update task")
            
        print(f"Task {task_id} marked as complete")
        
        # Create timesheet entry - this should always work for the user's own time
        try:
            from app.features.timesheet.routes_timesheet import create_entry
            from app.features.timesheet.models import TimeEntryCreate
            
            # Get client ID from task assignees if available
            client_id = None
            for assignee in task.get("assignees", []):
                if assignee.get("type") == "client":
                    client_id = assignee.get("client_id")
                    break
            
            # Create timesheet entry - simplified for user's own time
            timesheet_entry = TimeEntryCreate(
                client_id=client_id,
                hours=completion.hours,
                minutes=completion.minutes,
                type="task",
                item=task.get("job_type", "General Task"),
                description=f"Completed task: {task.get('title')}",
                category="Task Work",
                date=datetime.now(timezone.utc).isoformat()
            )
            
            print(f"Creating timesheet entry for user {user_id}")
            # Pass minimal user info - just need user_id for timesheet
            simplified_user_groups = {
                "user_id": user_id,
                "groups": ["DEFAULT"]  # Ensure basic access
            }
            await create_entry(timesheet_entry, simplified_user_groups)
            print(f"Timesheet entry created successfully")
            
        except Exception as e:
            print(f"Error creating timesheet entry: {str(e)}")
            # Still return success for task completion even if timesheet fails
            return {
                "status": "partial_success",
                "message": "Task marked complete but timesheet entry failed",
                "error": str(e)
            }
            
        return {
            "status": "success",
            "message": "Task marked complete and timesheet entry created"
        }
        
    except Exception as e:
        print(f"Error in complete_task: {str(e)}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/task-templates/{template_id}")
async def delete_task_template(
    template_id: str,
    user_groups: dict = Depends(get_current_user_group)
):
    """
    Delete a task template.
    
    Args:
        template_id (str): ID of template to delete
        user_groups (dict): User authentication data
        
    Returns:
        dict: Deletion status
        
    Raises:
        HTTPException: For validation or database errors
    """
    try:
        # Verify user has permission to delete
        existing = await task_templates.find_one({"_id": ObjectId(template_id)})
        if not existing:
            raise HTTPException(status_code=404, detail="Template not found")
            
        if (existing["created_by"] != user_groups.get("user_id") and 
            "admin" not in user_groups.get("groups", [])):
            raise HTTPException(status_code=403, detail="Not authorized to delete this template")

        result = await task_templates.delete_one({"_id": ObjectId(template_id)})
        
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail="Template not found")
            
        return {"status": "success"}
    except Exception as e:
        print("Error in delete_task_template:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

router.include_router(notifications_router)
    
    
    