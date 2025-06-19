"""
Task Scheduler Module

This module handles the automated creation of tasks from templates.
It runs as a background process and creates tasks based on template
frequency settings.

Features:
- Daily task creation from templates
- Weekly task creation (on Sundays)
- Error handling and logging
- Status tracking
- Daily task notification emails

Dependencies:
- APScheduler for scheduling
- MongoDB for storage
- datetime for timing
"""

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timezone, timedelta
from app.shared.database import async_client
from bson import ObjectId
import logging
from . import task_notifications

# Initialize logger
logger = logging.getLogger(__name__)

# Initialize database collections
tasks = async_client["Opps"]["Tasks"]
task_templates = async_client["Opps"]["TaskTemplates"]

async def create_tasks_from_templates():
    """
    Create new tasks from active templates based on their frequency.
    Daily templates create tasks every day, weekly templates create tasks on Sundays.
    """
    try:
        logger.info("Starting task creation from templates")
        
        # Get all active templates
        async for template in task_templates.find({"is_active": True}):
            try:
                # For weekly tasks, only create on Sundays
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
                logger.info(f"Created task from template {template['_id']}")
                
            except Exception as template_error:
                logger.error(f"Error processing template {template.get('_id')}: {str(template_error)}")
                continue
                
    except Exception as e:
        logger.error(f"Error in create_tasks_from_templates: {str(e)}")

def init_scheduler():
    """
    Initialize the task scheduler.
    Runs every day at midday to create tasks from templates.
    Weekly tasks are only created on Sundays.
    Also initializes the task notification scheduler.
    """
    try:
        scheduler = AsyncIOScheduler()
        
        # Schedule task creation to run daily at midday (12:00 PM)
        scheduler.add_job(
            create_tasks_from_templates,
            CronTrigger(hour=12, minute=0),
            id='create_tasks_from_templates',
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("Task scheduler initialized successfully")
        
        # Initialize task notifications
        task_notifications.init_notification_scheduler()
        
    except Exception as e:
        logger.error(f"Error initializing scheduler: {str(e)}")

# Initialize scheduler when module is imported
init_scheduler() 