#!/usr/bin/env python3
"""
Task Template Processor Script

A standalone script to create tasks from active templates.
Designed to be run via cron job for reliable scheduling.

Usage:
    python scripts/create_tasks_from_templates.py

Cron example (daily at 12:00 PM):
    0 12 * * * cd /path/to/snappedii && python scripts/create_tasks_from_templates.py

Environment:
    Requires the same environment as the main application
"""

import asyncio
import sys
import os
from datetime import datetime, timezone, timedelta
import logging

# Add the app directory to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.shared.database import async_client

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/tmp/task_creation.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# Database collections
tasks = async_client["Opps"]["Tasks"]
task_templates = async_client["Opps"]["TaskTemplates"]

async def create_tasks_from_templates():
    """
    Create new tasks from active templates based on their frequency.
    Daily templates create tasks every day, weekly templates create tasks on Sundays.
    """
    try:
        logger.info("Starting task creation from templates")
        created_count = 0
        skipped_count = 0
        error_count = 0
        
        # Get all active templates
        async for template in task_templates.find({"is_active": True}):
            try:
                template_title = template.get("title", "Unknown")
                
                # For weekly tasks, only create on Sundays (weekday 6)
                if template["frequency"] == "weekly" and datetime.now().weekday() != 6:
                    logger.info(f"Skipping weekly template '{template_title}' - not Sunday")
                    skipped_count += 1
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
                logger.info(f"Created task from template: {template_title}")
                
            except Exception as template_error:
                error_count += 1
                logger.error(f"Error processing template {template.get('title', 'Unknown')}: {str(template_error)}")
                continue
        
        logger.info(f"Task creation completed - Created: {created_count}, Skipped: {skipped_count}, Errors: {error_count}")
        return created_count, skipped_count, error_count
                
    except Exception as e:
        logger.error(f"Error in create_tasks_from_templates: {str(e)}")
        raise

async def main():
    """Main execution function"""
    try:
        logger.info("Task template processor starting...")
        created, skipped, errors = await create_tasks_from_templates()
        
        if errors > 0:
            logger.warning(f"Completed with {errors} errors")
            return 1
        else:
            logger.info("Task creation completed successfully")
            return 0
            
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 