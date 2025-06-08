"""
Timesheet Management Module

This module handles timesheet functionality including time entry management,
invoice generation, and QuickBooks integration.

Features:
- Time entry management
- Invoice generation
- QuickBooks integration
- Progress tracking
- Client billing
- Status monitoring

Data Model:
- Time entries
- Invoice structure
- Client billing
- Employee rates
- Status tracking

Security:
- Authentication required
- Role-based access
- Data validation
- Error handling

Dependencies:
- FastAPI for routing
- MongoDB for storage
- Pydantic for validation
- requests for QuickBooks

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException, Depends
from datetime import datetime, timedelta
from typing import List
import requests
from app.shared.database import (
    time_entries_collection,
    employees_collection,
    clients_collection
)
from app.shared.auth import get_current_user_group
from .models import TimeEntryItem, DailyTimesheet, TimesheetSession, TimeEntryCreate
import logging
from pydantic import ValidationError
from bson import ObjectId
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

router = APIRouter(
    prefix="/timesheet",
    tags=["timesheet"]
)

# Use the imported collections
time_entries = time_entries_collection
employees = employees_collection
clients = clients_collection

QUICKBOOKS_WEBHOOK = "https://hook.us2.make.com/bcpx0sqimgo9dih97dvumlldjk3fpapk"

logger = logging.getLogger(__name__)

@router.get("/entries")
async def get_entries(auth_data: dict = Depends(get_current_user_group)):
    """
    Retrieve all time entries for the authenticated user.
    
    Args:
        auth_data (dict): User authentication data
        
    Returns:
        list: List of time entries with metadata
        
    Raises:
        HTTPException: For database errors
        
    Notes:
        - Flattens entries from all invoices
        - Includes client information
        - Sorts by date descending
    """
    try:
        user_id = auth_data.get("user_id")
        logger.info(f"Fetching entries for user: {user_id}")
        
        # Get the timesheet document for the user
        timesheet = await time_entries.find_one({"user_id": user_id})
        if not timesheet:
            return []

        # Flatten entries from all invoices
        flattened_entries = []
        for invoice in timesheet.get("invoices", []):
            for day in invoice.get("days", []):
                for entry in day.get("entries", []):
                    entry_with_metadata = {
                        **entry,
                        "date": day["date"],
                        "invoice_submitted": invoice.get("submitted", False)
                    }
                    
                    # Add client name
                    if entry.get("client_id"):
                        try:
                            client = await clients_collection.find_one({"_id": ObjectId(entry["client_id"])})
                            if client:
                                entry_with_metadata["client_name"] = f"{client.get('First_Legal_Name', '')} {client.get('Last_Legal_Name', '')}"
                        except Exception as e:
                            logger.error(f"Error getting client info: {str(e)}")
                            entry_with_metadata["client_name"] = entry["client_id"]
                    
                    flattened_entries.append(entry_with_metadata)
        
        # Sort by date descending
        flattened_entries.sort(key=lambda x: x["date"], reverse=True)
        
        return flattened_entries
        
    except Exception as e:
        logger.error(f"Error in get_entries: {str(e)}")
        logger.exception("Full traceback:")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/entries")
async def create_entry(
    entry: TimeEntryCreate,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Create a new time entry.
    
    Args:
        entry (TimeEntryCreate): Time entry data
        auth_data (dict): User authentication data
        
    Returns:
        dict: Creation status
        
    Raises:
        HTTPException: For validation or database errors
        
    Notes:
        - Calculates earnings
        - Manages invoice periods
        - Updates totals
        - Handles new periods
    """
    try:
        user_id = auth_data.get("user_id")
        logger.info(f"Creating entry for user: {user_id}")
        
        # Get employee rate
        employee = await employees_collection.find_one({"user_id": user_id})
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")
        
        hourly_rate = float(employee.get("rate", 0))
        entry_date = datetime.fromisoformat(entry.date.replace('Z', '+00:00'))
        
        # Calculate earnings
        total_hours = entry.hours + (entry.minutes / 60)
        earnings = total_hours * hourly_rate
        
        # Create entry document
        new_entry = TimeEntryItem(
            client_id=entry.client_id,
            hours=entry.hours,
            minutes=entry.minutes,
            type=entry.type,
            item=entry.item,
            description=entry.description,
            category=entry.category,
            earnings=earnings,
            created_at=datetime.now(),
            status="active"
        ).dict()

        # Find or create employee timesheet document
        timesheet = await time_entries.find_one({"user_id": user_id})
        if not timesheet:
            timesheet = {
                "user_id": user_id,
                "invoices": []
            }
            await time_entries.insert_one(timesheet)
            timesheet = await time_entries.find_one({"user_id": user_id})

        # Find active invoice or create new one
        active_invoice = None
        if timesheet.get("invoices"):
            last_invoice = timesheet["invoices"][-1]
            if not last_invoice.get("submitted"):
                # Count working days in current invoice
                working_days = len(last_invoice.get("days", []))
                if working_days < 14:
                    active_invoice = last_invoice

        if not active_invoice:
            # Create new invoice period and copy qb_id from root level if it exists
            active_invoice = {
                "start_date": entry_date,
                "days": [],
                "submitted": False,
                "total_earnings": 0
            }
            if timesheet.get("qb_id"):
                active_invoice["qb_id"] = timesheet["qb_id"]
                
            await time_entries.update_one(
                {"user_id": user_id},
                {"$push": {"invoices": active_invoice}}
            )
            timesheet = await time_entries.find_one({"user_id": user_id})
            active_invoice = timesheet["invoices"][-1]

        # Find or create day entry
        day_found = False
        for i, day in enumerate(active_invoice.get("days", [])):
            if day["date"] == entry_date.date().isoformat():
                # Add entry to existing day
                await time_entries.update_one(
                    {"user_id": user_id, "invoices.submitted": False},
                    {
                        "$push": {f"invoices.$.days.{i}.entries": new_entry},
                        "$inc": {"invoices.$.total_earnings": earnings}
                    }
                )
                day_found = True
                break

        if not day_found:
            # Create new day with entry
            new_day = {
                "date": entry_date.date().isoformat(),
                "entries": [new_entry]
            }
            await time_entries.update_one(
                {"user_id": user_id, "invoices.submitted": False},
                {
                    "$push": {"invoices.$.days": new_day},
                    "$inc": {"invoices.$.total_earnings": earnings}
                }
            )

        return {"status": "success", "message": "Entry created"}

    except Exception as e:
        logger.error(f"Error creating entry: {str(e)}")
        logger.exception("Full traceback:")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/preview-invoice")
async def preview_invoice(auth_data: dict = Depends(get_current_user_group)):
    """
    Generate a preview of the current invoice.
    
    Args:
        auth_data (dict): User authentication data
        
    Returns:
        dict: Invoice preview data
        
    Raises:
        HTTPException: For database errors
        
    Notes:
        - Includes employee details
        - Formats entries
        - Calculates totals
        - Adds client names
    """
    try:
        user_id = auth_data.get("user_id")
        
        # Get active invoice session
        timesheet = await time_entries.find_one({
            "user_id": user_id,
            "invoices": {
                "$elemMatch": {
                    "submitted": False
                }
            }
        })

        if not timesheet:
            raise HTTPException(status_code=404, detail="No active invoice session found")

        active_invoice = next(
            (inv for inv in timesheet["invoices"] if not inv.get("submitted")), 
            None
        )

        # Add debug logging
        logger.info("Active Invoice Data:")
        logger.info(f"QB ID: {active_invoice.get('qb_id')}")
        logger.info(f"Full Invoice: {active_invoice}")

        if not active_invoice:
            raise HTTPException(status_code=404, detail="No active invoice session found")

        # Get employee details for the preview
        employee = await employees_collection.find_one({"user_id": user_id})
        
        # Format entries for preview with client names
        formatted_days = []
        total_hours = 0
        total_earnings = 0

        for day in active_invoice["days"]:
            day_entries = []
            for entry in day["entries"]:
                # Get client info - handle ObjectId conversion safely
                try:
                    client_id = ObjectId(entry["client_id"]) if entry.get("client_id") else None
                    client = await clients_collection.find_one({"_id": client_id}) if client_id else None
                    client_name = f"{client.get('First_Legal_Name', '')} {client.get('Last_Legal_Name', '')}" if client else entry.get("client_id", "Unknown")
                except Exception as e:
                    logger.error(f"Error converting client_id: {entry.get('client_id')} - {str(e)}")
                    client_name = entry.get("client_id", "Unknown")
                
                entry_hours = entry["hours"] + (entry["minutes"] / 60)
                total_hours += entry_hours
                total_earnings += entry["earnings"]

                day_entries.append({
                    **entry,
                    "client_name": client_name,
                    "total_hours": entry_hours
                })

            formatted_days.append({
                "date": day["date"],
                "entries": day_entries
            })

        # Format for preview
        preview_data = {
            "employee": {
                "name": f"{employee.get('first_name', '')} {employee.get('last_name', '')}",
                "rate": float(employee.get("rate", 0))
            },
            "start_date": active_invoice["start_date"],
            "days": formatted_days,
            "totals": {
                "hours": total_hours,
                "earnings": total_earnings
            }
        }

        return preview_data

    except Exception as e:
        logger.error(f"Error previewing invoice: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/submit-invoice")
async def submit_invoice(auth_data: dict = Depends(get_current_user_group)):
    """
    Submit the current invoice to QuickBooks.
    
    Args:
        auth_data (dict): User authentication data
        
    Returns:
        dict: Submission status and amount
        
    Raises:
        HTTPException: For submission errors
        
    Notes:
        - Validates QuickBooks ID
        - Builds description
        - Sends to webhook
        - Updates status
    """
    try:
        user_id = auth_data.get("user_id")
        
        # Get employee details
        employee = await employees_collection.find_one({"user_id": user_id})
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")

        # Get active invoice session and user's QB ID
        timesheet = await time_entries.find_one({
            "user_id": user_id,
            "invoices": {
                "$elemMatch": {
                    "submitted": False
                }
            }
        })

        if not timesheet:
            raise HTTPException(status_code=404, detail="No active invoice session found")

        active_invoice = next(
            (inv for inv in timesheet["invoices"] if not inv.get("submitted")), 
            None
        )

        # Add debug logging
        logger.info("Active Invoice Data:")
        logger.info(f"QB ID from invoice: {active_invoice.get('qb_id')}")
        logger.info(f"QB ID from root: {timesheet.get('qb_id')}")
        logger.info(f"Full Invoice: {active_invoice}")

        # Check for QuickBooks ID - first in invoice, then at root level
        qb_id = active_invoice.get("qb_id") or timesheet.get("qb_id")
        if not qb_id:
            return JSONResponse(
                status_code=400,
                content={
                    "detail": "QuickBooks vendor ID not set. Please contact your manager to get your QuickBooks ID."
                }
            )

        # If QB ID was found at root but not in invoice, update the invoice
        if not active_invoice.get("qb_id") and timesheet.get("qb_id"):
            await time_entries.update_one(
                {"user_id": user_id, "invoices.submitted": False},
                {"$set": {"invoices.$.qb_id": timesheet["qb_id"]}}
            )
            active_invoice["qb_id"] = timesheet["qb_id"]

        # Build detailed description
        description_lines = []
        total_amount = 0

        for day in active_invoice["days"]:
            for entry in day["entries"]:
                try:
                    client_id = ObjectId(entry["client_id"]) if entry.get("client_id") else None
                    client = await clients_collection.find_one({"_id": client_id}) if client_id else None
                    client_name = f"{client.get('First_Legal_Name', '')} {client.get('Last_Legal_Name', '')}" if client else "Unknown Client"
                except Exception as e:
                    logger.error(f"Error converting client_id: {entry.get('client_id')} - {str(e)}")
                    client_name = "Unknown Client"
                
                # Handle entries that might not have a category
                category = entry.get('category', 'Uncategorized')
                
                description_lines.append(
                    f"{client_name}: [{category}] {entry['item']} - {entry['description']} ({entry['hours']}h {entry['minutes']}m) ${entry['earnings']:.2f}"
                )
                total_amount += entry["earnings"]

        # Format data for Make webhook
        make_data = {
            "data": {
                "vendor_id": int(qb_id),
                "amount": float(total_amount),
                "description": "\n".join(description_lines),
                "date": str(active_invoice["start_date"]),
                "item_id": "1010000001",  # Updated QuickBooks item ID
                "account": "Professional Services"
            },
            "metadata": {
                "type": "bill",
                "source": "timesheet"
            }
        }

        # Send to Make webhook
        logger.info(f"Sending to Make webhook: {make_data}")
        response = requests.post(QUICKBOOKS_WEBHOOK, json=make_data)
        
        if response.status_code != 200:
            logger.error(f"Make webhook failed: {response.text}")
            raise HTTPException(status_code=500, detail="Failed to submit to QuickBooks")

        # Mark invoice as submitted
        await time_entries.update_one(
            {"user_id": user_id, "invoices.submitted": False},
            {"$set": {"invoices.$.submitted": True}}
        )

        return {
            "status": "success",
            "message": "Invoice submitted successfully",
            "amount": total_amount
        }

    except Exception as e:
        logger.error(f"Error submitting invoice: {str(e)}")
        logger.exception("Full traceback:")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/search_assignees")
async def search_assignees(
    query: str,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Search for assignable clients.
    
    Args:
        query (str): Search query
        auth_data (dict): User authentication data
        
    Returns:
        dict: List of matching clients
        
    Raises:
        HTTPException: For database errors
        
    Notes:
        - Searches multiple fields
        - Formats results
        - Limits results
        - Includes metadata
    """
    try:
        logger.info(f"Searching assignees with query: {query}")

        # Search in ClientDb.ClientInfo collection
        client_query = {
            "$or": [
                {"First_Legal_Name": {"$regex": f".*{query}.*", "$options": "i"}},
                {"Last_Legal_Name": {"$regex": f".*{query}.*", "$options": "i"}},
                {"Email_Address": {"$regex": f".*{query}.*", "$options": "i"}},
                {"Stage_Name": {"$regex": f".*{query}.*", "$options": "i"}}
            ]
        }
        
        # Use the correct collection
        clients_cursor = clients_collection.find(client_query)
        clients = await clients_cursor.to_list(length=100)  # Limit to 100 results
        
        logger.info(f"Found {len(clients)} matching clients")

        # Format client results with correct field names
        results = [
            {
                "id": str(client.get("_id")),
                "name": f"{client.get('First_Legal_Name', '')} {client.get('Last_Legal_Name', '')}",
                "type": "client",
                "client_id": str(client.get("_id")),
                "email": client.get("Email_Address"),
                "stage_name": client.get("Stage_Name")
            } for client in clients if client.get("First_Legal_Name") and client.get("Last_Legal_Name")
        ]

        logger.info(f"Formatted {len(results)} results")
        return {"assignees": results}
        
    except Exception as e:
        logger.error(f"Error in search_assignees: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/set-qb-id")
async def set_quickbooks_id(qb_id: str, auth_data: dict = Depends(get_current_user_group)):
    """Set QuickBooks ID for the user"""
    try:
        user_id = auth_data.get("user_id")
        
        # Update the qb_id at the root level of the user's document
        result = await time_entries.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "qb_id": qb_id,
                    "invoices.$[invoice].qb_id": qb_id
                }
            },
            array_filters=[{"invoice.submitted": False}]
        )

        if result.modified_count == 0:
            # If no document was modified, the user might not have a timesheet document yet
            await time_entries.insert_one({
                "user_id": user_id,
                "qb_id": qb_id,
                "invoices": []
            })

        return {"status": "success", "message": "QuickBooks ID set successfully"}

    except Exception as e:
        logger.error(f"Error setting QuickBooks ID: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/entries/{entry_id}")
async def update_entry(
    entry_id: str,
    entry: TimeEntryCreate,
    auth_data: dict = Depends(get_current_user_group)
):
    try:
        user_id = auth_data.get("user_id")
        
        # Get employee rate
        employee = await employees_collection.find_one({"user_id": user_id})
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")
        
        hourly_rate = float(employee.get("rate", 0))
        entry_date = datetime.fromisoformat(entry.date.replace('Z', '+00:00'))
        
        # Calculate new earnings
        total_hours = entry.hours + (entry.minutes / 60)
        earnings = total_hours * hourly_rate

        # First find the entry in the active invoice
        timesheet = await time_entries.find_one({
            "user_id": user_id,
            "invoices.submitted": False,
            "invoices.days.entries": {
                "$elemMatch": {
                    "item": entry_id  # Using the item field as identifier
                }
            }
        })

        if not timesheet:
            raise HTTPException(status_code=404, detail="Entry not found or already submitted")

        # Update the entry
        result = await time_entries.update_one(
            {
                "user_id": user_id,
                "invoices.submitted": False,
                "invoices.days.entries.item": entry_id
            },
            {
                "$set": {
                    "invoices.$[invoice].days.$[day].entries.$[entry].hours": entry.hours,
                    "invoices.$[invoice].days.$[day].entries.$[entry].minutes": entry.minutes,
                    "invoices.$[invoice].days.$[day].entries.$[entry].item": entry.item,
                    "invoices.$[invoice].days.$[day].entries.$[entry].description": entry.description,
                    "invoices.$[invoice].days.$[day].entries.$[entry].category": entry.category,
                    "invoices.$[invoice].days.$[day].entries.$[entry].earnings": earnings
                }
            },
            array_filters=[
                {"invoice.submitted": False},
                {"day.entries.item": entry_id},
                {"entry.item": entry_id}
            ]
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Entry not found or already submitted")

        # Recalculate total earnings for the invoice
        await recalculate_invoice_totals(user_id)

        return {"status": "success", "message": "Entry updated successfully"}

    except Exception as e:
        logger.error(f"Error updating entry: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/entries/{entry_id}")
async def delete_entry(
    entry_id: str,
    auth_data: dict = Depends(get_current_user_group)
):
    try:
        user_id = auth_data.get("user_id")
        
        # Find the entry in the active invoice
        timesheet = await time_entries.find_one({
            "user_id": user_id,
            "invoices.submitted": False,
            "invoices.days.entries": {
                "$elemMatch": {
                    "item": entry_id  # Using the item field as identifier
                }
            }
        })

        if not timesheet:
            raise HTTPException(status_code=404, detail="Entry not found or already submitted")

        # Remove the entry
        result = await time_entries.update_one(
            {
                "user_id": user_id,
                "invoices.submitted": False
            },
            {
                "$pull": {
                    "invoices.$[invoice].days.$[day].entries": {
                        "item": entry_id
                    }
                }
            },
            array_filters=[
                {"invoice.submitted": False},
                {"day.entries.item": entry_id}
            ]
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Entry not found or already submitted")

        # Clean up empty days
        await time_entries.update_one(
            {"user_id": user_id},
            {
                "$pull": {
                    "invoices.$[invoice].days": {
                        "entries": { "$size": 0 }
                    }
                }
            },
            array_filters=[{"invoice.submitted": False}]
        )

        # Recalculate total earnings for the invoice
        await recalculate_invoice_totals(user_id)

        return {"status": "success", "message": "Entry deleted successfully"}

    except Exception as e:
        logger.error(f"Error deleting entry: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def recalculate_invoice_totals(user_id: str):
    """
    Recalculate invoice totals after changes.
    
    Args:
        user_id (str): User identifier
        
    Notes:
        - Updates earnings
        - Handles active invoices
        - Updates database
        - Logs errors
    """
    try:
        timesheet = await time_entries.find_one({
            "user_id": user_id,
            "invoices.submitted": False
        })
        
        if not timesheet:
            return
        
        active_invoice = next(
            (inv for inv in timesheet["invoices"] if not inv.get("submitted")),
            None
        )
        
        if not active_invoice:
            return
            
        total_earnings = sum(
            entry["earnings"]
            for day in active_invoice["days"]
            for entry in day["entries"]
        )
        
        await time_entries.update_one(
            {
                "user_id": user_id,
                "invoices.submitted": False
            },
            {
                "$set": {
                    "invoices.$.total_earnings": total_earnings
                }
            }
        )
    except Exception as e:
        logger.error(f"Error recalculating invoice totals: {str(e)}") 