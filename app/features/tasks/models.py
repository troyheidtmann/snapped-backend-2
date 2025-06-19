"""
Task Management Models

This module defines the data models for task management, including tasks,
task templates, task completion records, and task assignees.

Models:
- Task: Core task model
- TaskCreate: Model for creating new tasks
- TaskUpdate: Model for updating existing tasks
- TaskTemplate: Model for recurring task templates
- TaskTemplateCreate: Model for creating templates
- TaskTemplateUpdate: Model for updating templates
- TaskCompletion: Model for task completion data
- TaskAssignee: Model for task assignees
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional
from datetime import datetime
from bson import ObjectId

class TaskAssignee(BaseModel):
    """
    Task assignee data model.
    
    Represents an assignee for a task, which can be either an employee or client.
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
    """
    id: str = Field(default_factory=lambda: str(ObjectId()), alias="_id")
    title: str
    description: str
    status: str  # "active", "hold", "completed"
    priority: str  # "high", "medium", "low"
    due_date: str
    assignees: List[TaskAssignee] = []
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    visible_to: List[str] = []  # Groups that can see this task
    template_id: Optional[str] = None  # Reference to template if created from one
    job_type: str
    completed_at: Optional[datetime] = None
    completed_by: Optional[str] = None
    completion_notes: Optional[str] = None
    time_spent: Optional[dict] = None

    model_config = ConfigDict(
        populate_by_name=True,
        json_encoders={ObjectId: str}
    )

class TaskCreate(BaseModel):
    """
    Model for creating new tasks.
    """
    title: str
    description: str
    priority: str
    status: str = "Active"
    assignees: List[TaskAssignee] = []
    template_id: Optional[str] = None
    job_type: str
    due_date: Optional[str] = None
    visible_to: List[str] = []

class TaskUpdate(BaseModel):
    """
    Model for updating existing tasks.
    """
    title: Optional[str] = None
    description: Optional[str] = None
    priority: Optional[str] = None
    status: Optional[str] = None
    assignees: Optional[List[TaskAssignee]] = None
    job_type: Optional[str] = None
    due_date: Optional[str] = None
    visible_to: Optional[List[str]] = None

class TaskTemplate(BaseModel):
    """
    Task template for recurring tasks.
    """
    id: str = Field(default_factory=lambda: str(ObjectId()), alias="_id")
    title: str
    description: str
    frequency: str  # "daily" or "weekly"
    priority: str  # "high", "medium", "low"
    assignees: List[TaskAssignee] = []
    job_type: str
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    visible_to: List[str] = []
    is_active: bool = True

    model_config = ConfigDict(
        populate_by_name=True,
        json_encoders={ObjectId: str}
    )

class TaskTemplateCreate(BaseModel):
    """
    Model for creating task templates.
    """
    title: str
    description: str
    frequency: str
    priority: str
    assignees: List[TaskAssignee] = []
    job_type: str
    visible_to: List[str] = []
    is_active: bool = True

class TaskTemplateUpdate(BaseModel):
    """
    Model for updating task templates.
    """
    title: Optional[str] = None
    description: Optional[str] = None
    frequency: Optional[str] = None
    priority: Optional[str] = None
    assignees: Optional[List[TaskAssignee]] = None
    job_type: Optional[str] = None
    visible_to: Optional[List[str]] = None
    is_active: Optional[bool] = None

class TaskCompletion(BaseModel):
    """
    Task completion data model.
    
    Contains information about task completion including time spent.
    """
    hours: int
    minutes: int
    notes: Optional[str] = None 