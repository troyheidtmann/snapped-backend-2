"""
Database Module

This module manages MongoDB database connections and collections
for the application.

Features:
- Connection management
- Collection access
- Database initialization
- Error handling
- Lifecycle management

Data Model:
- Client data
- Messages
- Uploads
- Analytics
- Payments

Security:
- SSL/TLS
- Credentials
- Retry logic
- Error handling
- Connection pooling

Dependencies:
- Motor for async MongoDB
- PyMongo for sync ops
- FastAPI for lifecycle
- certifi for SSL
- urllib for encoding

Author: Snapped Development Team
"""

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.mongo_client import MongoClient
from urllib.parse import quote_plus
import asyncio
from typing import Optional, Dict, Any
from fastapi import FastAPI, Depends
from contextlib import asynccontextmanager
import certifi

# Database Names
DB_NAME = "ClientDb"
UPLOAD_DB_NAME = "UploadDB"

# URL encode the username and password
username = quote_plus("troyheidtmann")
password = quote_plus("Gunit1500!!!!@@@@")

# Build the connection string with encoded credentials
MONGODB_URL = f"mongodb+srv://{username}:{password}@clientdb.fsb2wz0.mongodb.net/?retryWrites=true&w=majority"

# MongoDB Connection Settings
MONGO_SETTINGS = {
    "tlsCAFile": certifi.where(),
    "tls": True,
    "serverSelectionTimeoutMS": 10000,
    "connectTimeoutMS": 20000,
    "maxPoolSize": 100,
    "retryWrites": True
}

# Create clients
client = MongoClient(MONGODB_URL, **MONGO_SETTINGS)
async_client = AsyncIOMotorClient(
    MONGODB_URL,
    **MONGO_SETTINGS
)

# Database references
db = client[DB_NAME]
upload_db = async_client[UPLOAD_DB_NAME]
notif_db = async_client['NotifDB']

# ClientDb Collections
client_info = async_client["ClientDb"]["ClientInfo"]
client_note = async_client["ClientDb"]["ClientNote"]
contracts_collection = async_client["ClientDb"]["Contracts"]
edit_thumb_collection = async_client["UploadDB"]["EditThumb"]
content_data_collection = async_client["ClientDb"]["content_data"]
clients_collection = async_client["ClientDb"]["ClientInfo"]

# Messages Collections
message_store = async_client["Messages"]["message_store"]
model_feedback = async_client["Messages"]["model_feedback"]
model_prompts = async_client["Messages"]["model_prompts"]

# NotifDB Collections
notes_collection = async_client["NotifDB"]["ContentNotes"]
edit_notes = async_client["NotifDB"]["EditNotes"]

# Opps Collections
time_entries_collection = async_client["Opps"]["time_track"]
employees_collection = async_client["Opps"]["Employees"]
tasks_collection = async_client["Opps"]["Tasks"]

# Partners Collections
partners_collection = async_client["Partners"]["PartnerList"]
monetized_by_collection = async_client["Partners"]["MonetizedBy"]
referred_by_collection = async_client["Partners"]["ReferredBy"]
verified_by_collection = async_client["Partners"]["VerifiedBy"]

# UploadDB Collections
upload_collection = async_client['UploadDB']['Uploads']
content_dump_collection = async_client['UploadDB']['Content_Dump']
saved_collection = async_client['UploadDB']['Saved']
spotlight_collection = async_client["UploadDB"]["Spotlights"]

# QueueDB Collections
queue_collection = async_client["QueueDB"]["Queue"]

# AIVideo Collections 
video_analysis_collection = async_client["AIVideo"]["video_analysis"]
analysis_queue_collection = async_client["AIVideo"]["analysis_queue"]
summary_prompt_collection = async_client["AIVideo"]["summary_prompts"]

# Add these collections to the existing database.py
survey_responses = async_client["ClientDb"]["survey_responses"]
survey_questions = async_client["ClientDb"]["survey_questions"]

# Payment Collections
client_payouts = async_client["Payments"]["Payouts"]
payee_info = async_client["Payments"]["payee_info"]
commission_splits = async_client["Payments"]["commission_splits"]
payment_records = async_client["Payments"]["payment_records"]
payment_statements = async_client["Payments"]["payment_statements"]
payment_statements = async_client["Payments"]["Statements"]

async def init_db():
    """
    Initialize database connection.
    
    Returns:
        bool: Connection status
        
    Notes:
        - Retries connection
        - Validates ping
        - Handles errors
        - Logs status
    """
    retry_count = 3
    retry_delay = 5  # seconds
    
    for attempt in range(retry_count):
        try:
            print(f"Database initialization attempt {attempt + 1}/{retry_count}...")
            await async_client.admin.command('ping')
            print("MongoDB ping successful")
            return True
        except Exception as e:
            print(f"Database connection attempt {attempt + 1} failed: {e}")
            if attempt < retry_count - 1:
                print(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                print("All connection attempts failed")
                return False

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage database lifecycle.
    
    Args:
        app: FastAPI application
        
    Yields:
        None
        
    Notes:
        - Initializes DB
        - Handles startup
        - Manages shutdown
        - Error handling
    """
    print("Starting database initialization...")
    success = await init_db()
    if not success:
        raise Exception("Failed to initialize database")
    print("Database initialization complete")
    
    yield
    
    print("Shutting down database connections...")
    async_client.close()
    print("Database connections closed")

# Export collections and utilities
__all__ = [
    'async_client',
    'db',
    'upload_db',
    'notif_db',
    'client_info',
    'client_note',
    'contracts_collection',
    'edit_thumb_collection',
    'content_data_collection',
    'clients_collection',
    'message_store',
    'model_feedback',
    'model_prompts',
    'notes_collection',
    'edit_notes',
    'time_entries_collection',
    'employees_collection',
    'tasks_collection',
    'partners_collection',
    'monetized_by_collection',
    'referred_by_collection',
    'verified_by_collection',
    'upload_collection',
    'content_dump_collection',
    'saved_collection',
    'spotlight_collection',
    'queue_collection',
    'video_analysis_collection',
    'analysis_queue_collection',
    'summary_prompt_collection',
    'survey_responses',
    'survey_questions',
    'client_payouts',
    'payee_info', 
    'commission_splits',
    'payment_records',
    'payment_statements',
    'lifespan',
    'init_db'
]