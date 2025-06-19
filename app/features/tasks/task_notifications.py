"""
Task Notifications Module

This module handles the generation and sending of daily task notification emails
to employees. It aggregates tasks by employee, sorts them by priority and due date,
and sends formatted email digests.

Features:
- Daily task digest emails
- Priority-based task sorting
- Due date organization
- Employee-specific task filtering
- Test mode for verification
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from app.shared.database import async_client, employees_collection, tasks_collection
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import os
from dotenv import load_dotenv
import ssl
from fastapi import APIRouter, HTTPException

# Initialize logger
logger = logging.getLogger(__name__)

# Import email configuration from shared config
from app.shared.config import (
    SMTP_SERVER,
    SMTP_PORT,
    SMTP_USERNAME,
    SMTP_PASSWORD,
    FROM_EMAIL
)

# Company details for the email
COMPANY_NAME = os.getenv("COMPANY_NAME", "Snapped")
COMPANY_WEBSITE = os.getenv("COMPANY_WEBSITE", "https://snapped.cc")
EMAIL_REPLY_TO = os.getenv("EMAIL_REPLY_TO", "support@snapped.cc")

# Create router with prefix
router = APIRouter(
    prefix="/tasks",
    tags=["task-notifications"]
)

async def get_employee_tasks() -> Dict[str, List[dict]]:
    """
    Retrieve all active tasks grouped by employee email.
    
    Returns:
        Dict mapping employee emails to their tasks
    """
    employee_tasks = {}
    
    try:
        # First get all employees to have their IDs and emails ready
        employees = {}  # Map of employee_id to employee info
        async for employee in employees_collection.find({}):
            # Store by user_id (which is their employee_id)
            if user_id := employee.get("user_id"):
                employees[user_id] = {
                    "email": employee.get("email"),
                    "name": f"{employee.get('first_name', '')} {employee.get('last_name', '')}".strip()
                }
                if user_id == "th10021994":
                    logger.info(f"Found TH: {employee}")
        
        logger.info(f"Found {len(employees)} employees in database")
        
        # Get active and pending tasks
        cursor = tasks_collection.find({
            "status": {
                "$in": [
                    "active", "Active",
                    "pending", "Pending"
                ]
            }
        })
        
        active_tasks = []
        async for task in cursor:
            # Check each assignee
            for assignee in task.get("assignees", []):
                # Only look at employee assignees
                if assignee.get("type") == "employee":
                    employee_id = assignee.get("employee_id")
                    if employee_id == "th10021994":
                        logger.info(f"Found task for TH: {task}")
                    
                    if employee_id in employees:
                        employee_info = employees[employee_id]
                        email = employee_info["email"]
                        
                        if email not in employee_tasks:
                            employee_tasks[email] = []
                        employee_tasks[email].append(task)
                        active_tasks.append(task)
                        break
        
        logger.info(f"Found {len(active_tasks)} active/pending tasks")
        logger.info(f"Tasks assigned to {len(employee_tasks)} employees")
        if "th@snapped.cc" in employee_tasks:
            logger.info(f"Tasks for TH: {len(employee_tasks['th@snapped.cc'])}")
            for task in employee_tasks["th@snapped.cc"]:
                logger.info(f"TH Task: {task.get('title')} - {task.get('status')}")
        else:
            logger.info("No tasks found for TH")
        
    except Exception as e:
        logger.error(f"Error retrieving employee tasks: {str(e)}")
        logger.error("Full error details:", exc_info=True)
    
    return employee_tasks

def sort_tasks(tasks: List[dict]) -> tuple[List[dict], List[dict]]:
    """
    Sort tasks into priority and non-priority lists.
    
    Args:
        tasks: List of task documents
        
    Returns:
        Tuple of (priority_tasks, other_tasks)
    """
    priority_tasks = []
    other_tasks = []
    
    for task in tasks:
        if task["priority"].lower() == "high":
            priority_tasks.append(task)
        else:
            other_tasks.append(task)
    
    # Sort both lists by due date
    priority_tasks.sort(key=lambda x: x["due_date"])
    other_tasks.sort(key=lambda x: x["due_date"])
    
    return priority_tasks, other_tasks

def format_task_email(priority_tasks: List[dict], other_tasks: List[dict]) -> str:
    """
    Format tasks into an HTML email body with summary section.
    
    Args:
        priority_tasks: List of high priority tasks
        other_tasks: List of other tasks
        
    Returns:
        HTML formatted email body
    """
    # Get today's date and end of week
    today = datetime.now(timezone.utc).date()
    end_of_week = today + timedelta(days=6-today.weekday())  # Sunday
    
    # Filter tasks for summary
    due_today = []
    due_this_week = []
    high_priority = []
    
    for task in priority_tasks + other_tasks:
        try:
            due_date = datetime.strptime(task['due_date'], '%Y-%m-%d').date()
            if due_date == today:
                due_today.append(task)
            if due_date <= end_of_week:
                due_this_week.append(task)
        except (ValueError, TypeError):
            continue
            
        if task.get('priority', '').lower() == 'high':
            high_priority.append(task)
    
    html = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                line-height: 1.6;
                color: #2c3e50;
                max-width: 800px;
                margin: 0 auto;
                background-color: #f8f9fa;
            }}
            .header {{
                background-color: #0066cc;
                color: white;
                padding: 30px;
                text-align: center;
                border-radius: 8px 8px 0 0;
            }}
            .header h1 {{
                margin: 0;
                font-size: 28px;
                font-weight: 600;
            }}
            .header h2 {{
                margin: 10px 0 0;
                font-size: 18px;
                font-weight: 400;
                opacity: 0.9;
            }}
            .container {{
                background-color: white;
                padding: 30px;
                border-radius: 0 0 8px 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .summary {{
                margin-bottom: 30px;
                border-radius: 8px;
                overflow: hidden;
                max-width: 600px;
                margin-left: auto;
                margin-right: auto;
            }}
            .summary h3 {{
                margin: 0;
                padding: 15px 20px;
                background-color: #0066cc;
                color: white;
                font-size: 18px;
                font-weight: 500;
            }}
            .summary-content {{
                padding: 20px;
                display: flex;
                justify-content: space-between;
                flex-wrap: wrap;
            }}
            .summary-box {{
                flex: 1;
                min-width: 150px;
                background: white;
                border: 1px solid #e9ecef;
                border-radius: 8px;
                padding: 15px;
                margin: 10px;
                text-align: center;
                box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            }}
            .summary-box-title {{
                color: #0066cc;
                font-weight: 600;
                font-size: 16px;
                margin-bottom: 10px;
            }}
            .summary-box-count {{
                font-size: 24px;
                font-weight: bold;
                color: #2c3e50;
            }}
            .tasks-section {{
                margin-top: 30px;
            }}
            .tasks-section h3 {{
                color: #2c3e50;
                font-size: 20px;
                margin-bottom: 15px;
                padding-bottom: 10px;
                border-bottom: 2px solid #e9ecef;
            }}
            .task {{
                background-color: white;
                border: 1px solid #e9ecef;
                border-radius: 6px;
                padding: 15px;
                margin-bottom: 15px;
                transition: all 0.2s ease;
            }}
            .task:hover {{
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .priority {{
                border-left: 4px solid #dc3545;
            }}
            .task .title {{
                font-weight: 600;
                font-size: 16px;
                margin-bottom: 8px;
                color: #2c3e50;
            }}
            .task .description {{
                color: #6c757d;
                margin-bottom: 8px;
            }}
            .task .due-date {{
                color: #0066cc;
                font-size: 14px;
                font-weight: 500;
            }}
            .footer {{
                margin-top: 30px;
                text-align: center;
                color: #6c757d;
                font-size: 14px;
            }}
            .footer p {{
                margin: 5px 0;
            }}
            .footer a {{
                color: #0066cc;
                text-decoration: none;
            }}
            .footer a:hover {{
                text-decoration: underline;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>{COMPANY_NAME} Daily Task Summary</h1>
            <h2>{datetime.now().strftime('%B %d, %Y')}</h2>
        </div>
        
        <div class="container">
            <div class="summary">
                <h3>Today's Overview</h3>
                <div class="summary-content">
                    <div class="summary-box">
                        <div class="summary-box-title">Due Today</div>
                        <div class="summary-box-count">{len(due_today)}</div>
                    </div>
                    
                    <div class="summary-box">
                        <div class="summary-box-title">Due This Week</div>
                        <div class="summary-box-count">{len(due_this_week)}</div>
                    </div>
                    
                    <div class="summary-box">
                        <div class="summary-box-title">High Priority</div>
                        <div class="summary-box-count">{len(high_priority)}</div>
                    </div>
                </div>
            </div>
            
            <div class="tasks-section">
                <h3>Due Today</h3>
                {"".join(f'''
                    <div class="task {'priority' if task['priority'].lower() == 'high' else ''}">
                        <div class="title">{task["title"]}</div>
                        <div class="description">{task["description"]}</div>
                        <div class="due-date">Due: {task["due_date"]}</div>
                    </div>
                ''' for task in due_today) if due_today else '<div class="task">No tasks due today</div>'}
                
                <h3>Due This Week</h3>
                {"".join(f'''
                    <div class="task {'priority' if task['priority'].lower() == 'high' else ''}">
                        <div class="title">{task["title"]}</div>
                        <div class="description">{task["description"]}</div>
                        <div class="due-date">Due: {task["due_date"]}</div>
                    </div>
                ''' for task in due_this_week) if due_this_week else '<div class="task">No tasks due this week</div>'}
                
                <h3>High Priority Tasks</h3>
                {"".join(f'''
                    <div class="task priority">
                        <div class="title">{task["title"]}</div>
                        <div class="description">{task["description"]}</div>
                        <div class="due-date">Due: {task["due_date"]}</div>
                    </div>
                ''' for task in high_priority) if high_priority else '<div class="task">No high priority tasks</div>'}
            </div>
            
            <div class="footer">
                <p>This is an automated message from {COMPANY_NAME}</p>
                <p>For support, please reply to <a href="mailto:{EMAIL_REPLY_TO}">{EMAIL_REPLY_TO}</a></p>
                <p><a href="{COMPANY_WEBSITE}">{COMPANY_WEBSITE}</a></p>
            </div>
        </div>
    </body>
    </html>
    """
    return html

async def send_email(to_email: str, subject: str, html_content: str, test_mode: bool = False) -> bool:
    """
    Send an HTML email using the shared SMTP configuration.
    
    Args:
        to_email: Recipient email address
        subject: Email subject line
        html_content: HTML formatted email body
        test_mode: If True, logs email content without sending
        
    Returns:
        bool: True if email sent successfully, False otherwise
    """
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = FROM_EMAIL
        msg['To'] = to_email
        msg['Reply-To'] = EMAIL_REPLY_TO
        
        msg.attach(MIMEText(html_content, 'html'))
        
        if test_mode:
            logger.info(f"""
            Would send email:
            To: {to_email}
            Subject: {subject}
            From: {FROM_EMAIL}
            Reply-To: {EMAIL_REPLY_TO}
            Content: {html_content[:200]}...
            """)
            return True
            
        # Create secure SSL/TLS connection
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
            
        logger.info(f"Successfully sent email to {to_email}")
        return True
        
    except Exception as e:
        logger.error(f"Error sending email to {to_email}: {str(e)}")
        return False

async def send_task_emails(test_mode: bool = False):
    """
    Send daily task digest emails to all employees with active tasks.
    
    Args:
        test_mode: If True, runs in test mode without sending actual emails
    """
    try:
        # Get tasks grouped by employee
        logger.info("Starting send_task_emails...")
        
        # Log current date for due date comparison
        current_date = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"Current date for comparison: {current_date}")
        
        # Get all tasks first to see what exists
        all_tasks = []
        async for task in tasks_collection.find({}):
            logger.info(f"Found task in DB: Title='{task.get('title')}', Status='{task.get('status')}', Due='{task.get('due_date')}', Assignees={task.get('assignees', [])}")
            all_tasks.append(task)
        
        logger.info(f"Total tasks in database: {len(all_tasks)}")
        
        # Now get employee tasks
        employee_tasks = await get_employee_tasks()
        
        if not employee_tasks:
            logger.info("No active tasks found for any employees")
            return {
                "status": "success",
                "message": "No active tasks found for any employees",
                "details": {
                    "total_tasks": len(all_tasks),
                    "active_tasks": 0,
                    "employees_with_tasks": 0
                }
            }
            
        logger.info(f"Found tasks for {len(employee_tasks)} employees")
        
        emails_sent = 0
        task_counts = {}
        
        # Send email to each employee
        for email, tasks in employee_tasks.items():
            try:
                # Sort tasks by priority
                priority_tasks, other_tasks = sort_tasks(tasks)
                
                logger.info(f"\nProcessing email for {email}:")
                logger.info(f"- Priority tasks ({len(priority_tasks)}):")
                for task in priority_tasks:
                    logger.info(f"  * {task.get('title')} (Due: {task.get('due_date')})")
                logger.info(f"- Other tasks ({len(other_tasks)}):")
                for task in other_tasks:
                    logger.info(f"  * {task.get('title')} (Due: {task.get('due_date')})")
                
                # Format email content
                html_content = format_task_email(priority_tasks, other_tasks)
                subject = f"Daily Task Summary - {datetime.now().strftime('%Y-%m-%d')}"
                
                # Send email
                if test_mode:
                    logger.info(f"\nWould send email to {email}:")
                    logger.info(f"Subject: {subject}")
                    logger.info("Content preview:")
                    logger.info(html_content[:500] + "...")
                else:
                    success = await send_email(email, subject, html_content, test_mode)
                    if success:
                        emails_sent += 1
                        task_counts[email] = {
                            "priority_tasks": len(priority_tasks),
                            "other_tasks": len(other_tasks)
                        }
                
            except Exception as email_error:
                logger.error(f"Error processing email for {email}: {str(email_error)}")
                continue
    
        return {
            "status": "success",
            "message": "Task notifications processed successfully",
            "details": {
                "total_tasks": len(all_tasks),
                "employees_with_tasks": len(employee_tasks),
                "emails_sent": emails_sent,
                "task_counts": task_counts,
                "test_mode": test_mode
            }
        }
    
    except Exception as e:
        logger.error(f"Error in send_task_emails: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

@router.get("/test-email")
async def test_email(email: str):
    """
    Simple endpoint to test email sending.
    No auth required, just sends a test email to the provided address.
    """
    try:
        # Create a simple test email
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Test Email from {COMPANY_NAME}"
        msg['From'] = FROM_EMAIL
        msg['To'] = email
        msg['Reply-To'] = EMAIL_REPLY_TO
        
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
                .container {{ padding: 20px; }}
                .header {{ background-color: #0066cc; color: white; padding: 20px; text-align: center; }}
                .content {{ margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>{COMPANY_NAME} Test Email</h1>
            </div>
            <div class="container">
                <div class="content">
                    <p>This is a test email from the task notification system.</p>
                    <p>If you're receiving this, the email system is working correctly!</p>
                    <p>Time sent: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(html_content, 'html'))
        
        # Create secure SSL/TLS connection
        context = ssl.create_default_context()
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
            
        logger.info(f"Test email sent successfully to {email}")
        return {
            "status": "success",
            "message": f"Test email sent to {email}",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error sending test email: {str(e)}")
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }

@router.post("/notifications/test")
async def test_notifications(email: Optional[str] = None):
    """
    Test endpoint for task notifications.
    
    Args:
        email: Optional email to send test notification to
        
    Returns:
        dict: Test results
    """
    try:
        logger.info("Starting notification system test")
        
        # Get current tasks in system
        all_tasks = []
        async for task in tasks_collection.find({}):
            all_tasks.append({
                "title": task.get("title"),
                "status": task.get("status"),
                "due_date": task.get("due_date"),
                "priority": task.get("priority"),
                "assignees": task.get("assignees", [])
            })
        
        # Run notification system in test mode
        result = await send_task_emails(test_mode=True)
        
        return {
            "status": "success",
            "message": "Test notifications processed successfully",
            "test_email": email if email else "all employees",
            "system_status": {
                "total_tasks": len(all_tasks),
                "tasks": all_tasks,
                "notification_result": result
            }
        }
        
    except Exception as e:
        logger.error(f"Error in test_notifications: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/notifications/send")
async def send_notifications(test_mode: bool = False):
    """
    Endpoint to manually trigger task notifications.
    
    Args:
        test_mode: If True, runs in test mode without sending actual emails
        
    Returns:
        dict: Operation results with detailed information
    """
    try:
        # First get all employees
        employees = {}
        async for employee in employees_collection.find({}):
            employees[str(employee["_id"])] = {
                "email": employee.get("email"),
                "name": employee.get("name")
            }
        
        # Get all tasks to see what we have
        all_tasks = []
        status_counts = {}
        active_pending_tasks = []
        
        async for task in tasks_collection.find({}):
            status = task.get("status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1
            
            task_info = {
                "title": task.get("title"),
                "status": status,
                "due_date": task.get("due_date"),
                "priority": task.get("priority"),
                "assignees": task.get("assignees", [])
            }
            
            all_tasks.append(task_info)
            
            # Track active/pending tasks separately
            if status in ["active", "Active", "pending", "Pending"]:
                active_pending_tasks.append(task_info)
        
        # Now send notifications
        notification_result = await send_task_emails(test_mode=test_mode)
        
        return {
            "status": "success",
            "message": "Task notifications processed successfully",
            "details": {
                "total_employees": len(employees),
                "employee_sample": list(employees.values())[:3],  # Show first 3 employees
                "total_tasks": len(all_tasks),
                "active_pending_count": len(active_pending_tasks),
                "status_counts": status_counts,
                "active_pending_tasks": active_pending_tasks[:5],  # Show first 5 active/pending tasks
                "notification_result": notification_result,
                "test_mode": test_mode
            }
        }
    except Exception as e:
        logger.error(f"Error sending notifications: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/notifications/schedule")
async def schedule_notifications(hour: int = 8, minute: int = 0):
    """
    Endpoint to schedule daily task notifications.
    
    Args:
        hour: Hour to send notifications (24-hour format)
        minute: Minute to send notifications
        
    Returns:
        dict: Schedule results
    """
    try:
        scheduler = AsyncIOScheduler()
        scheduler.add_job(
            send_task_emails,
            CronTrigger(hour=hour, minute=minute),
            id='send_task_emails',
            replace_existing=True
        )
        scheduler.start()
        
        return {
            "status": "success",
            "message": f"Notifications scheduled for {hour:02d}:{minute:02d} daily",
            "schedule": {
                "hour": hour,
                "minute": minute
            }
        }
    except Exception as e:
        logger.error(f"Error scheduling notifications: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/notifications/test-route")
async def test_route():
    """Simple endpoint to test routing"""
    return {"status": "success", "message": "Route is working"} 