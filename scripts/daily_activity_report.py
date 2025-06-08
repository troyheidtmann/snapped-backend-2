#!/usr/bin/env python3
"""
Daily Activity Report Generator

A simple script to generate and email daily activity reports about content uploads and queuing.
Designed to be run as a daily cron job.

Example cron entry (run daily at 6 PM):
0 18 * * * /path/to/python /path/to/daily_activity_report.py
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from typing import Dict, List, Any
import os
import sys
from motor.motor_asyncio import AsyncIOMotorClient
import certifi

# Make sure we can import from the app directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import email config from shared app config
try:
    from app.shared.config import (
        SMTP_SERVER,
        SMTP_PORT,
        SMTP_USERNAME as EMAIL_USER,
        SMTP_PASSWORD as EMAIL_PASSWORD,
        FROM_EMAIL
    )
    EMAIL_CONFIGURED = True
except ImportError:
    # Fallback values if config can't be imported
    SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
    EMAIL_USER = os.environ.get("EMAIL_USER", "")
    EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "")
    FROM_EMAIL = EMAIL_USER
    EMAIL_CONFIGURED = False

# Email recipients - either from env or default
EMAIL_RECIPIENTS = os.environ.get("EMAIL_RECIPIENTS", "troyheidtmann@icloud.com,jacob@blackmatter.uk").split(",")

# Configure logging - ensure the directory exists
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'{log_dir}/daily_report.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# Log email configuration
if EMAIL_CONFIGURED:
    logger.info("Using email configuration from app.shared.config")
else:
    logger.info("Using fallback email configuration from environment")

# MongoDB connection settings
MONGODB_URL = os.environ.get("MONGODB_URL")
if not MONGODB_URL:
    from urllib.parse import quote_plus
    # Fallback to hardcoded connection if not in env vars
    username = quote_plus("troyheidtmann")
    password = quote_plus("Gunit1500!!!!@@@@")
    MONGODB_URL = f"mongodb+srv://{username}:{password}@clientdb.fsb2wz0.mongodb.net/?retryWrites=true&w=majority"

async def get_mongodb_connection():
    """Connect to MongoDB and return collections"""
    try:
        client = AsyncIOMotorClient(
            MONGODB_URL,
            tlsCAFile=certifi.where(),
            tls=True,
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=20000,
            maxPoolSize=100,
            retryWrites=True
        )
        
        # Test connection
        await client.admin.command('ping')
        logger.info("MongoDB connection successful")
        
        # Get database collections
        upload_collection = client['UploadDB']['Uploads']
        queue_collection = client['QueueDB']['Queue'] 
        clients_collection = client['ClientDb']['ClientInfo']
        employees_collection = client['Opps']['Employees']
        spotlight_collection = client['UploadDB']['Spotlights']
        content_dump_collection = client['UploadDB']['Content_Dump']
        saved_collection = client['UploadDB']['Saved']
        
        return {
            'client': client,
            'upload_collection': upload_collection,
            'queue_collection': queue_collection,
            'clients_collection': clients_collection,
            'employees_collection': employees_collection,
            'spotlight_collection': spotlight_collection,
            'content_dump_collection': content_dump_collection,
            'saved_collection': saved_collection
        }
    except Exception as e:
        logger.error(f"MongoDB connection error: {str(e)}")
        raise

async def get_daily_uploads(db):
    """Get upload activity for the current day"""
    # Use timezone-aware datetime objects
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today + timedelta(days=1)
    
    # Store results by client_id
    results = {}
    # Track snap_IDs for clients to help with lookups
    snap_id_mapping = {}
    
    # Query all upload collections
    collections = [
        db['upload_collection'],
        db['spotlight_collection'],
        db['content_dump_collection'],
        db['saved_collection']
    ]
    
    for collection in collections:
        cursor = collection.find({
            "last_updated": {"$gte": today, "$lt": tomorrow}
        })
        
        async for doc in cursor:
            # Check for both lowercase and uppercase field names
            client_id = doc.get("client_id") or doc.get("client_ID")
            if not client_id:
                logger.warning(f"Document in {collection.name} missing client_id field: {doc.get('_id')}")
                continue
                
            # Also capture snap_ID if available for later client lookup
            snap_id = doc.get("snap_id") or doc.get("snap_ID")
            if snap_id:
                snap_id_mapping[client_id] = snap_id
                logger.info(f"Found snap_id {snap_id} for client {client_id}")
            
            # Check all sessions to find ones updated today
            today_sessions = []
            for session in doc.get("sessions", []):
                if isinstance(session, dict):
                    # Check if session was updated today, handling both naive and aware datetimes
                    session_date = session.get("upload_date")
                    if session_date:
                        # Convert naive datetime to aware if needed
                        if isinstance(session_date, datetime) and session_date.tzinfo is None:
                            session_date = session_date.replace(tzinfo=timezone.utc)
                        
                        # Now compare with timezone-aware times
                        if today <= session_date < tomorrow:
                            today_sessions.append(session)
            
            if today_sessions:
                total_files = sum(session.get("total_files_count", 0) for session in today_sessions)
                total_videos = sum(session.get("total_videos", 0) for session in today_sessions)
                total_images = sum(session.get("total_images", 0) for session in today_sessions)
                
                # Get collection name for the report
                collection_name = collection.name
                
                # Add or update client data
                if client_id not in results:
                    results[client_id] = {
                        "session_count": len(today_sessions),
                        "total_files": total_files,
                        "total_videos": total_videos,
                        "total_images": total_images,
                        "snap_id": snap_id,  # Store snap_id with results
                        "collections": {collection_name: {
                            "session_count": len(today_sessions),
                            "total_files": total_files,
                            "total_videos": total_videos,
                            "total_images": total_images
                        }}
                    }
                else:
                    # Update existing client data
                    client_data = results[client_id]
                    client_data["session_count"] += len(today_sessions)
                    client_data["total_files"] += total_files
                    client_data["total_videos"] += total_videos
                    client_data["total_images"] += total_images
                    # Add or update snap_id if available
                    if snap_id and not client_data.get("snap_id"):
                        client_data["snap_id"] = snap_id
                    client_data["collections"][collection_name] = {
                        "session_count": len(today_sessions),
                        "total_files": total_files,
                        "total_videos": total_videos,
                        "total_images": total_images
                    }
    
    # Log snap_id mapping to help with debugging
    if snap_id_mapping:
        logger.info(f"Found snap_IDs for {len(snap_id_mapping)} clients: {snap_id_mapping}")
    
    return results, snap_id_mapping

async def get_daily_queue_activity(db):
    """Get queue building activity for the current day"""
    today = datetime.now(timezone.utc).date().isoformat()
    
    # Store results by client_id
    results = {}
    
    # Query for today's queue
    queue_doc = await db['queue_collection'].find_one({"queue_date": today})
    
    if queue_doc:
        client_queues = queue_doc.get("client_queues", {})
        
        for client_id, queue_data in client_queues.items():
            stories = queue_data.get("stories", [])
            
            # Count by session
            session_counts = {}
            for story in stories:
                session_id = story.get("session_id")
                if session_id:
                    if session_id not in session_counts:
                        session_counts[session_id] = 0
                    session_counts[session_id] += 1
            
            results[client_id] = {
                "session_count": len(session_counts),
                "total_stories": len(stories),
                "session_details": session_counts
            }
    
    return results

async def get_client_names(db, client_ids, snap_id_mapping=None):
    """Get client names for the given client IDs from ClientDb.ClientInfo"""
    results = {}
    
    if not snap_id_mapping:
        snap_id_mapping = {}
    
    # Get a sample document to debug field names
    sample_doc = await db['clients_collection'].find_one({})
    if sample_doc:
        logger.info(f"Sample client document fields: {', '.join(sample_doc.keys())}")
    
    for client_id in client_ids:
        logger.info(f"Looking up client with ID: {client_id}")
        
        # Create various possible ID formats to search
        possible_ids = [
            client_id,                 # Original ID (could be th10021994)
            client_id.lower(),         # Lowercase version (could be th10021994)
            client_id.upper(),         # Uppercase version
        ]
        
        # If ID starts with 'th' or 'kd', also try without the prefix
        if isinstance(client_id, str) and (client_id.startswith('th') or client_id.startswith('kd')):
            numeric_part = ''.join(c for c in client_id if c.isdigit())
            if numeric_part:
                possible_ids.append(numeric_part)
                
        # Add snap_ID to possible IDs if available
        snap_id = snap_id_mapping.get(client_id)
        if snap_id:
            possible_ids.append(snap_id)
            logger.info(f"Using snap_ID {snap_id} for client {client_id}")
        
        # Log which IDs we're trying
        logger.info(f"Trying these ID formats for {client_id}: {possible_ids}")
        
        # Try to find client with any of these IDs
        client_doc = None
        matched_field = None
        
        # Try each possible ID with each possible field name
        field_names = ["client_id", "client_ID", "clientId", "snap_id", "snap_ID", "snapId", "_id"]
        
        for field_name in field_names:
            if field_name == "_id":
                # Special handling for ObjectId
                try:
                    from bson import ObjectId
                    for possible_id in possible_ids:
                        if isinstance(possible_id, str) and len(possible_id) == 24 and all(c in '0123456789abcdef' for c in possible_id.lower()):
                            client_doc = await db['clients_collection'].find_one({"_id": ObjectId(possible_id)})
                            if client_doc:
                                logger.info(f"Found client with _id: {possible_id}")
                                matched_field = "_id"
                except (ImportError, ValueError):
                    pass
            else:
                # Regular field lookup
                for possible_id in possible_ids:
                    # Skip ObjectId for regular field lookups
                    if field_name != "_id":
                        query = {field_name: possible_id}
                        client_doc = await db['clients_collection'].find_one(query)
                        if client_doc:
                            logger.info(f"Found client with {field_name}: {possible_id}")
                            matched_field = field_name
                            break
            
            # If we found a match, stop trying other fields
            if client_doc:
                break
                
        if client_doc:
            # Log the full document for debugging
            logger.info(f"Found client document: {client_doc}")
            
            # Use business_name as primary, fall back to Stage_Name, then First+Last Legal name
            business_name = client_doc.get("business_name")
            stage_name = client_doc.get("Stage_Name")
            first_name = client_doc.get("First_Legal_Name")
            last_name = client_doc.get("Last_Legal_Name")
            
            # Get the actual client_id from the document, could be in either field
            actual_client_id = client_doc.get("client_id") or client_doc.get("client_ID") or client_id
            
            if business_name:
                client_name = business_name
            elif stage_name:
                client_name = stage_name
            elif first_name and last_name:
                client_name = f"{first_name} {last_name}"
            else:
                client_name = f"Unknown ({actual_client_id})"
            
            logger.info(f"Resolved client {client_id} to '{client_name}' via {matched_field}")
                
            # Add additional info for reference in the report
            results[client_id] = {
                "display_name": client_name,
                "business_name": business_name,
                "stage_name": stage_name,
                "legal_name": f"{first_name} {last_name}" if first_name and last_name else None,
                "client_id": actual_client_id,
                "snap_id": client_doc.get("snap_id") or snap_id,
                "ig_username": client_doc.get("IG_Username"),
                "followers": client_doc.get("IG_Followers")
            }
        else:
            logger.warning(f"No client document found for ID: {client_id}")
            # No client found - use client_id as fallback
            results[client_id] = {
                "display_name": f"Unknown ({client_id})",
                "client_id": client_id
            }
    
    return results

async def get_employee_mapping(db):
    """Get mapping of client to employee/editor"""
    results = {}
    
    async for employee in db['employees_collection'].find({}):
        employee_id = employee.get("employee_id")
        employee_name = employee.get("name", "Unknown")
        
        client_assignments = employee.get("client_assignments", [])
        
        for client_id in client_assignments:
            if client_id:
                results[client_id] = {
                    "employee_id": employee_id,
                    "employee_name": employee_name
                }
    
    return results

def generate_html_report(upload_data, queue_data, client_names, employee_mapping):
    """Generate HTML email content for the daily report"""
    today = datetime.now().strftime("%A, %B %d, %Y")
    
    # Combine data by employee
    employee_data = {}
    
    # Process all client IDs from both datasets
    all_client_ids = set(list(upload_data.keys()) + list(queue_data.keys()))
    
    for client_id in all_client_ids:
        employee_info = employee_mapping.get(client_id, {
            "employee_id": "unassigned",
            "employee_name": "Unassigned"
        })
        
        employee_id = employee_info["employee_id"]
        
        if employee_id not in employee_data:
            employee_data[employee_id] = {
                "name": employee_info["employee_name"],
                "clients": {}
            }
        
        # Get richer client information
        client_info = client_names.get(client_id, {"display_name": f"Unknown ({client_id})"})
        client_uploads = upload_data.get(client_id, {})
        client_queue = queue_data.get(client_id, {})
        
        employee_data[employee_id]["clients"][client_id] = {
            "client_info": client_info,
            "uploads": client_uploads,
            "queue": client_queue
        }
    
    # Build HTML content
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
            .container {{ max-width: 900px; margin: 0 auto; padding: 20px; }}
            h1 {{ color: #2c3e50; text-align: center; }}
            h2 {{ color: #3498db; margin-top: 30px; border-bottom: 1px solid #ddd; padding-bottom: 10px; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 15px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; text-align: center; }}
            tr:hover {{ background-color: #f5f5f5; }}
            .summary {{ background-color: #e8f4fc; font-weight: bold; }}
            .client-meta {{ color: #666; font-size: 0.9em; margin-top: 5px; }}
            .counts {{ text-align: center; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Daily Activity Report</h1>
            <p><strong>Date:</strong> {today}</p>
            
            <h2>Summary</h2>
            <table>
                <tr>
                    <th>Activity</th>
                    <th>Count</th>
                </tr>
                <tr>
                    <td>Total Clients with Activity</td>
                    <td>{len(all_client_ids)}</td>
                </tr>
                <tr>
                    <td>Total Uploads</td>
                    <td>{sum(data.get("total_files", 0) for data in upload_data.values())}</td>
                </tr>
                <tr>
                    <td>Total Queued Stories</td>
                    <td>{sum(data.get("total_stories", 0) for data in queue_data.values())}</td>
                </tr>
            </table>
    """
    
    # Add employee sections
    for employee_id, data in employee_data.items():
        employee_name = data["name"]
        clients = data["clients"]
        
        total_uploads = sum(client_data["uploads"].get("total_files", 0) for client_data in clients.values())
        total_queued = sum(client_data["queue"].get("total_stories", 0) for client_data in clients.values())
        total_videos = sum(client_data["uploads"].get("total_videos", 0) for client_data in clients.values())
        total_images = sum(client_data["uploads"].get("total_images", 0) for client_data in clients.values())
        
        # Change "Editor: Unassigned" to just "Clients"
        if employee_id == "unassigned":
            section_title = "Clients"
        else:
            section_title = f"Editor: {employee_name}"
            
        html += f"""
            <h2>{section_title}</h2>
            <table>
                <tr>
                    <th rowspan="2">Client</th>
                    <th colspan="4">Uploads</th>
                    <th rowspan="2">Queued<br>Stories</th>
                </tr>
                <tr>
                    <th>Total</th>
                    <th>Videos</th>
                    <th>Images</th>
                    <th>Sessions</th>
                </tr>
        """
        
        for client_id, client_data in clients.items():
            client_info = client_data["client_info"]
            client_name = client_info["display_name"]
            ig_username = client_info.get("ig_username", "")
            followers = client_info.get("followers", "")
            
            # Get upload details
            uploads = client_data["uploads"].get("total_files", 0)
            videos = client_data["uploads"].get("total_videos", 0)
            images = client_data["uploads"].get("total_images", 0)
            sessions = client_data["uploads"].get("session_count", 0)
            
            # Extract collection-specific data 
            collections = client_data["uploads"].get("collections", {})
            spotlights = collections.get("Spotlights", {}).get("total_files", 0)
            saved = collections.get("Saved", {}).get("total_files", 0)
            
            # Queue details
            queued = client_data["queue"].get("total_stories", 0)
            
            # Add client info including IG username if available
            ig_info = f" (@{ig_username})" if ig_username else ""
            followers_info = f" - {followers:,} followers" if followers else ""
            
            html += f"""
                <tr>
                    <td>
                        <div>{client_name}{ig_info}</div>
                        <div class="client-meta">{client_id}{followers_info}</div>
                    </td>
                    <td class="counts">{uploads}</td>
                    <td class="counts">{videos}</td>
                    <td class="counts">{images}</td>
                    <td class="counts">{sessions}</td>
                    <td class="counts">{queued}</td>
                </tr>
            """
        
        # Summary row
        summary_title = "Total" if employee_id == "unassigned" else f"Total for {employee_name}"
        
        html += f"""
                <tr class="summary">
                    <td>{summary_title}</td>
                    <td class="counts">{total_uploads}</td>
                    <td class="counts">{total_videos}</td>
                    <td class="counts">{total_images}</td>
                    <td class="counts">{sum(client_data["uploads"].get("session_count", 0) for client_data in clients.values())}</td>
                    <td class="counts">{total_queued}</td>
                </tr>
            </table>
        """
    
    html += """
        </div>
    </body>
    </html>
    """
    
    return html

def send_email_sync(msg):
    """Send email synchronously using the SMTP server"""
    try:
        logger.info("Connecting to SMTP server...")
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            logger.info("Starting TLS...")
            server.starttls()
            logger.info(f"Logging in as {EMAIL_USER}")
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            logger.info("Sending message...")
            server.send_message(msg)
            logger.info("Message sent!")
    except Exception as e:
        logger.error(f"SMTP Error: {str(e)}")
        raise  # Re-raise to be caught by the calling function

async def send_email(html_content):
    """Send the email with the report"""
    if not EMAIL_USER or not EMAIL_PASSWORD:
        logger.warning("Email credentials not configured - skipping email send")
        return False
    
    try:
        # Format today's date for the subject
        today = datetime.now().strftime("%m/%d/%Y")
        
        # Create email
        msg = MIMEMultipart()
        msg["From"] = FROM_EMAIL
        msg["To"] = ", ".join(EMAIL_RECIPIENTS)
        msg["Subject"] = f"SnappedII Daily Activity Report - {today}"
        
        msg.attach(MIMEText(html_content, "html"))
        
        # Send email using the same pattern as in contracts.py
        # Use asyncio to prevent blocking, same as in contracts.py
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: send_email_sync(msg))
        
        logger.info("Email sent successfully")
        return True
    except Exception as e:
        logger.error(f"Error sending email: {str(e)}")
        return False

async def main():
    """Main function to generate and send the report"""
    try:
        logger.info("Starting daily activity report generation")
        
        # Get MongoDB connection
        db = await get_mongodb_connection()
        
        # Get data for the report - now returns snap_id_mapping too
        upload_data, snap_id_mapping = await get_daily_uploads(db)
        queue_data = await get_daily_queue_activity(db)
        
        # Get all client IDs from both datasets
        all_client_ids = list(set(list(upload_data.keys()) + list(queue_data.keys())))
        
        logger.info(f"Found activity for {len(all_client_ids)} clients")
        
        # Get additional data - pass snap_id_mapping to help with lookups
        client_names = await get_client_names(db, all_client_ids, snap_id_mapping)
        logger.info(f"Retrieved information for {len(client_names)} clients")
        
        employee_mapping = await get_employee_mapping(db)
        logger.info(f"Retrieved editor assignments for {len(employee_mapping)} clients")
        
        # Generate report
        html_content = generate_html_report(
            upload_data, queue_data, client_names, employee_mapping
        )
        
        # Log a summary of what we found
        total_uploads = sum(data.get("total_files", 0) for data in upload_data.values())
        total_queued = sum(data.get("total_stories", 0) for data in queue_data.values())
        logger.info(f"Report summary: {len(all_client_ids)} clients, {total_uploads} uploads, {total_queued} queued stories")
        
        # Send email
        send_result = await send_email(html_content)
        
        # Close database connection
        db['client'].close()
        
        if send_result:
            logger.info("Daily report completed successfully")
        else:
            logger.warning("Daily report generated but email sending failed")
            
            # Save the report to a file as backup
            backup_file = f"{log_dir}/daily_report_{datetime.now().strftime('%Y%m%d')}.html"
            with open(backup_file, "w") as f:
                f.write(html_content)
            logger.info(f"Saved report backup to {backup_file}")
                
    except Exception as e:
        logger.error(f"Error generating daily report: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 