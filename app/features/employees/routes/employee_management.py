"""
Employee Management API - Snapped Platform Employee Operations

This module provides FastAPI routes for managing employee data, timesheets,
metrics, and invoices in the Snapped platform. It handles employee information
retrieval, time tracking, performance analytics, and financial reporting.

Features:
--------
1. Employee Management:
   - Basic employee information
   - Active/inactive status
   - Role and department tracking

2. Time Tracking:
   - Detailed timesheets
   - Period-based reporting
   - Client work tracking
   - Category management

3. Performance Analytics:
   - Work metrics calculation
   - Productivity scoring
   - Client work analysis
   - Efficiency ratings

4. Financial Management:
   - Invoice generation
   - Payment tracking
   - Statement downloads
   - Financial summaries

Security:
--------
- Authentication via FastAPI dependencies
- Role-based access control
- Data validation
- Error handling

Dependencies:
-----------
- FastAPI: Web framework and routing
- MongoDB: Data storage
- Pydantic: Data validation
- datetime: Time management

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException, Depends, Query, Response
from datetime import datetime, timedelta
from typing import List, Optional
from bson import ObjectId
import logging
from app.shared.database import (
    employees_collection,
    time_entries_collection,
    clients_collection
)
from app.shared.auth import get_current_user_group
from app.features.employees.models.employee import (
    EmployeeBasic,
    EmployeeMetrics,
    EmployeeTimesheet,
    EmployeeInvoices,
    TimesheetEntry,
    ClientWorkSummary,
    InvoiceSummary
)

router = APIRouter(
    prefix="/employees",
    tags=["employee_management"]
)

logger = logging.getLogger(__name__)

def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse date string to datetime object, handling various formats.
    
    Args:
        date_str (Optional[str]): Date string to parse
        
    Returns:
        Optional[datetime]: Parsed datetime object or None
        
    Raises:
        HTTPException: If date format is invalid
    """
    if not date_str:
        return None
    try:
        # Try ISO format with timezone
        if 'T' in date_str:
            try:
                # First try parsing the full string
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except ValueError:
                # If that fails, try removing timezone
                base_dt = date_str.split('.')[0] if '.' in date_str else date_str.split('+')[0]
                return datetime.fromisoformat(base_dt)
        # Try simple date format (YYYY-MM-DD)
        elif len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-':
            return datetime.strptime(date_str, "%Y-%m-%d")
        # Try other formats
        else:
            try:
                return datetime.strptime(date_str, "%m/%d/%Y")
            except ValueError:
                logger.error(f"Could not parse date: {date_str}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid date format: {date_str}. Expected YYYY-MM-DD"
                )
    except Exception as e:
        logger.error(f"Error parsing date '{date_str}': {str(e)}")
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format: {date_str}. Expected YYYY-MM-DD"
        )

@router.get("/", response_model=List[EmployeeBasic])
async def list_employees(
    auth_data: dict = Depends(get_current_user_group),
    active_only: bool = Query(True, description="Filter to active employees only")
):
    """
    Get list of all employees with basic information.
    
    Args:
        auth_data (dict): User authentication data
        active_only (bool): Whether to return only active employees
        
    Returns:
        List[EmployeeBasic]: List of employee records
        
    Raises:
        HTTPException: For database or server errors
    """
    try:
        query = {}
        if active_only:
            query["active"] = {"$ne": False}  # Include docs without active field
        
        cursor = employees_collection.find(query)
        employees = await cursor.to_list(length=None)
        
        result = []
        for emp in employees:
            emp["_id"] = str(emp["_id"])  # Convert ObjectId to string
            result.append(EmployeeBasic(**emp))
        
        return result
        
    except Exception as e:
        logger.error(f"Error listing employees: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{employee_id}/timesheet", response_model=EmployeeTimesheet)
async def get_employee_timesheet(
    employee_id: str,
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Get employee timesheet data for a specific period.
    
    Args:
        employee_id (str): Employee identifier
        start_date (Optional[str]): Period start date
        end_date (Optional[str]): Period end date
        auth_data (dict): User authentication data
        
    Returns:
        EmployeeTimesheet: Timesheet data including:
            - Work entries
            - Total hours
            - Total earnings
            - Days worked
            
    Raises:
        HTTPException: For invalid employee ID or server errors
    """
    try:
        # Get employee info
        employee = await employees_collection.find_one({"user_id": employee_id})
        if not employee:
            logger.warning(f"Employee not found with user_id: {employee_id}")
            raise HTTPException(status_code=404, detail=f"Employee not found with ID: {employee_id}")
        
        # Set default date range if not provided (last 30 days)
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        start_dt = parse_date(start_date)
        end_dt = parse_date(end_date)
        if end_dt:
            end_dt = end_dt + timedelta(days=1)  # Include end date
        
        # Get timesheet data
        timesheet = await time_entries_collection.find_one({"user_id": employee_id})
        if not timesheet:
            return EmployeeTimesheet(
                user_id=employee_id,
                employee_name=f"{employee.get('first_name', '')} {employee.get('last_name', '')}",
                period_start=start_dt,
                period_end=end_dt,
                entries=[],
                total_hours=0,
                total_earnings=0,
                days_worked=0
            )
        
        # Collect entries within date range
        entries = []
        total_hours = 0
        total_earnings = 0
        worked_days = set()
        
        for invoice in timesheet.get("invoices", []):
            for day in invoice.get("days", []):
                try:
                    day_date = parse_date(day["date"])
                    if not day_date:
                        continue
                        
                    if start_dt <= day_date < end_dt:
                        worked_days.add(day["date"])
                        
                        for entry in day.get("entries", []):
                            # Get client info - try multiple lookup methods
                            try:
                                client_id = entry.get("client_id")
                                client = None
                                
                                if client_id:
                                    # First try looking up by client_id directly
                                    client = await clients_collection.find_one({"client_id": client_id})
                                    
                                    # If not found and it looks like an ObjectId, try that
                                    if not client and len(str(client_id)) == 24:
                                        try:
                                            client = await clients_collection.find_one({"_id": ObjectId(str(client_id))})
                                        except Exception as e:
                                            logger.error(f"Error looking up by ObjectId: {str(e)}")
                                
                                # Get client name, falling back to client_id if not found
                                if client and (client.get("First_Legal_Name") or client.get("Last_Legal_Name")):
                                    client_name = f"{client.get('First_Legal_Name', '')} {client.get('Last_Legal_Name', '')}".strip()
                                else:
                                    client_name = client_id if client_id else "Unknown Client"
                                
                            except Exception as e:
                                logger.error(f"Error looking up client: {entry.get('client_id')} - {str(e)}")
                                client_name = entry.get("client_id", "Unknown Client")
                            
                            entry_hours = entry.get("hours", 0) + (entry.get("minutes", 0) / 60)
                            total_hours += entry_hours
                            total_earnings += entry.get("earnings", 0)
                            
                            entries.append(TimesheetEntry(
                                date=day["date"],
                                client_id=entry.get("client_id", ""),
                                client_name=client_name,
                                hours=entry.get("hours", 0),
                                minutes=entry.get("minutes", 0),
                                category=entry.get("category", ""),
                                item=entry.get("item", ""),
                                description=entry.get("description", ""),
                                earnings=entry.get("earnings", 0)
                            ))
                except ValueError as e:
                    logger.warning(f"Invalid date format in timesheet: {day.get('date')}")
                    continue
        
        return EmployeeTimesheet(
            user_id=employee_id,
            employee_name=f"{employee.get('first_name', '')} {employee.get('last_name', '')}",
            period_start=start_dt,
            period_end=end_dt,
            entries=sorted(entries, key=lambda x: x.date, reverse=True),
            total_hours=total_hours,
            total_earnings=total_earnings,
            days_worked=len(worked_days)
        )
        
    except Exception as e:
        logger.error(f"Error getting employee timesheet for {employee_id}: {str(e)}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/{employee_id}/metrics", response_model=EmployeeMetrics)
async def get_employee_metrics(
    employee_id: str,
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Get employee work metrics and analytics.
    
    Args:
        employee_id (str): Employee identifier
        start_date (Optional[str]): Period start date
        end_date (Optional[str]): Period end date
        auth_data (dict): User authentication data
        
    Returns:
        EmployeeMetrics: Performance metrics including:
            - Work totals
            - Client statistics
            - Productivity scores
            - Efficiency ratings
            
    Raises:
        HTTPException: For invalid employee ID or server errors
    """
    try:
        # Get employee info
        employee = await employees_collection.find_one({"user_id": employee_id})
        if not employee:
            logger.warning(f"Employee not found with user_id: {employee_id}")
            raise HTTPException(status_code=404, detail=f"Employee not found with ID: {employee_id}")
        
        # Set default date range if not provided (last 30 days)
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        start_dt = parse_date(start_date)
        end_dt = parse_date(end_date)
        if end_dt:
            end_dt = end_dt + timedelta(days=1)  # Include end date
        
        # Get timesheet data
        timesheet = await time_entries_collection.find_one({"user_id": employee_id})
        if not timesheet:
            return EmployeeMetrics(
                user_id=employee_id,
                period_start=start_dt,
                period_end=end_dt,
                total_hours=0,
                total_earnings=0,
                active_clients=0,
                avg_hours_per_day=0,
                client_work_summary=[]
            )
        
        # Analyze timesheet data
        client_work = {}  # client_id -> {hours, earnings, categories, last_worked}
        category_totals = {}  # category -> hours
        total_hours = 0
        total_earnings = 0
        worked_days = set()
        
        for invoice in timesheet.get("invoices", []):
            for day in invoice.get("days", []):
                try:
                    day_date = parse_date(day["date"])
                    if not day_date:
                        continue
                        
                    if start_dt <= day_date < end_dt:
                        worked_days.add(day["date"])
                        
                    for entry in day.get("entries", []):
                        client_id = entry.get("client_id", "")
                        category = entry.get("category", "Uncategorized")
                        hours = entry.get("hours", 0) + (entry.get("minutes", 0) / 60)
                        earnings = entry.get("earnings", 0)
                        
                        # Update client work summary
                        if client_id not in client_work:
                            client_work[client_id] = {
                                "hours": 0,
                                "earnings": 0,
                                "categories": {},
                                "last_worked": day_date
                            }
                        
                        client_stats = client_work[client_id]
                        client_stats["hours"] += hours
                        client_stats["earnings"] += earnings
                        client_stats["categories"][category] = client_stats["categories"].get(category, 0) + hours
                        client_stats["last_worked"] = max(client_stats["last_worked"], day_date)
                        
                        # Update totals
                        total_hours += hours
                        total_earnings += earnings
                        category_totals[category] = category_totals.get(category, 0) + hours
                except ValueError as e:
                    logger.warning(f"Invalid date format in timesheet: {day.get('date')}")
                    continue
        
        # Calculate metrics
        days_in_period = (end_dt - start_dt).days or 1
        avg_hours_per_day = total_hours / days_in_period
        
        # Get most worked category and client
        most_worked_category = max(category_totals.items(), key=lambda x: x[1])[0] if category_totals else None
        most_worked_client_id = max(client_work.items(), key=lambda x: x[1]["hours"])[0] if client_work else None
        
        # Build client work summary
        client_summaries = []
        for client_id, stats in client_work.items():
            try:
                client_name = client_id if client_id else "Unknown Client"
                if client_id:
                    # First try looking up by client_id directly
                    client = await clients_collection.find_one({"client_id": client_id})
                    
                    # If not found and it looks like an ObjectId, try that
                    if not client and len(str(client_id)) == 24:
                        try:
                            client = await clients_collection.find_one({"_id": ObjectId(str(client_id))})
                        except Exception as e:
                            logger.error(f"Error looking up by ObjectId: {str(e)}")
                    
                    if client:
                        client_name = f"{client.get('First_Legal_Name', '')} {client.get('Last_Legal_Name', '')}".strip()
                
                client_summaries.append(ClientWorkSummary(
                    client_id=client_id,
                    client_name=client_name,
                    total_hours=stats["hours"],
                    total_earnings=stats["earnings"],
                    last_worked=stats["last_worked"],
                    categories=stats["categories"]
                ))
            except Exception as e:
                logger.warning(f"Error processing client {client_id}: {e}")
                continue
        
        # Calculate productivity score (example metric)
        productivity_score = round(min(100, (total_hours / (8 * len(worked_days))) * 100) if worked_days else 0)
        
        # Determine efficiency rating
        efficiency_rating = "Excellent" if productivity_score >= 90 else \
                          "Good" if productivity_score >= 75 else \
                          "Average" if productivity_score >= 60 else \
                          "Needs Improvement"
        
        return EmployeeMetrics(
            user_id=employee_id,
            period_start=start_dt,
            period_end=end_dt,
            total_hours=total_hours,
            total_earnings=total_earnings,
            active_clients=len(client_work),
            avg_hours_per_day=avg_hours_per_day,
            most_worked_client=most_worked_client_id,
            most_worked_category=most_worked_category,
            client_work_summary=sorted(client_summaries, key=lambda x: x.total_hours, reverse=True),
            productivity_score=productivity_score,
            efficiency_rating=efficiency_rating
        )
        
    except Exception as e:
        logger.error(f"Error getting employee metrics for {employee_id}: {str(e)}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/{employee_id}/invoices", response_model=EmployeeInvoices)
async def get_employee_invoices(
    employee_id: str,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Get employee invoice history.
    
    Args:
        employee_id (str): Employee identifier
        auth_data (dict): User authentication data
        
    Returns:
        EmployeeInvoices: Invoice data including:
            - Invoice list
            - Payment status
            - Financial totals
            
    Raises:
        HTTPException: For invalid employee ID or server errors
    """
    try:
        # Get employee info
        employee = await employees_collection.find_one({"user_id": employee_id})
        if not employee:
            logger.warning(f"Employee not found with user_id: {employee_id}")
            raise HTTPException(status_code=404, detail=f"Employee not found with ID: {employee_id}")
        
        # Get timesheet data
        timesheet = await time_entries_collection.find_one({"user_id": employee_id})
        if not timesheet:
            logger.info(f"No timesheet found for employee: {employee_id}")
            return EmployeeInvoices(
                user_id=employee_id,
                employee_name=f"{employee.get('first_name', '')} {employee.get('last_name', '')}",
                invoices=[]
            )
        
        logger.info(f"Found timesheet for employee {employee_id}")
        logger.info(f"Employee name: {employee.get('first_name')} {employee.get('last_name')}")
        logger.info(f"Number of invoices: {len(timesheet.get('invoices', []))}")
        
        invoices = []
        total_unpaid = 0
        total_paid = 0
        
        # Determine status and update totals
        total_draft = 0
        total_sent = 0
        
        for i, invoice in enumerate(timesheet.get("invoices", [])):
            try:
                logger.info(f"Processing invoice {i}")
                logger.info(f"Invoice data: {invoice}")
                
                # Calculate invoice totals
                total_hours = 0
                total_earnings = invoice.get("total_earnings", 0)
                
                # Calculate total hours from entries
                for day in invoice.get("days", []):
                    for entry in day.get("entries", []):
                        total_hours += entry.get("hours", 0) + (entry.get("minutes", 0) / 60)
                
                # Get the start date from the invoice
                start_date = None
                end_date = None
                if invoice.get("days") and len(invoice["days"]) > 0:
                    try:
                        # Sort days by date and get the earliest and latest
                        sorted_days = sorted(invoice["days"], key=lambda x: parse_date(x.get("date")))
                        if sorted_days:
                            start_date = sorted_days[0].get("date")
                            end_date = sorted_days[-1].get("date")
                    except Exception as e:
                        logger.error(f"Error getting date range from days: {str(e)}")

                # If no dates from days, try invoice start_date
                if not start_date and invoice.get("start_date"):
                    start_date = invoice["start_date"]
                if not end_date and invoice.get("end_date"):
                    end_date = invoice["end_date"]

                # Determine status
                status = "draft"
                if invoice.get("submitted"):
                    status = "sent"
                    total_sent += total_earnings
                elif invoice.get("paid"):
                    status = "paid"
                    total_paid += total_earnings
                else:
                    total_draft += total_earnings

                # Parse submitted date
                submitted_date = None
                if invoice.get("submitted_date"):
                    try:
                        submitted_date = parse_date(invoice.get("submitted_date"))
                    except Exception as e:
                        logger.error(f"Error parsing submitted_date: {str(e)}")

                invoice_summary = InvoiceSummary(
                    invoice_id=invoice.get("qb_id", str(i + 1)),  # Use i + 1 to avoid Invoice #0
                    start_date=start_date,
                    end_date=end_date,
                    total_hours=total_hours,
                    total_earnings=total_earnings,
                    status=status,
                    submitted_date=submitted_date,
                    qb_id=invoice.get("qb_id")
                )
                
                logger.info(f"Created invoice summary: {invoice_summary}")
                invoices.append(invoice_summary)
                
            except Exception as e:
                logger.error(f"Error processing invoice {i}: {str(e)}")
                logger.error(f"Invoice data: {invoice}")
                continue
        
        # Sort by start date descending
        invoices.sort(key=lambda x: x.start_date if x.start_date else datetime.min, reverse=True)
        
        logger.info(f"Returning {len(invoices)} invoices for employee {employee_id}")
        logger.info(f"Total draft: {total_draft}, Total sent: {total_sent}, Total paid: {total_paid}")
        
        return EmployeeInvoices(
            user_id=employee_id,
            employee_name=f"{employee.get('first_name', '')} {employee.get('last_name', '')}",
            invoices=invoices,
            total_unpaid=total_sent,  # Change this to show sent amount
            total_paid=total_paid,
            total_draft=total_draft  # Add this new field
        )
        
    except Exception as e:
        logger.error(f"Error getting employee invoices for {employee_id}: {str(e)}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.post("/{employee_id}/invoices/{invoice_id}/download")
async def download_invoice(
    employee_id: str,
    invoice_id: str,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Generate and download invoice statement.
    
    Args:
        employee_id (str): Employee identifier
        invoice_id (str): Invoice identifier
        auth_data (dict): User authentication data
        
    Returns:
        Response: HTML invoice statement with:
            - Work details
            - Time entries
            - Financial totals
            - Status information
            
    Raises:
        HTTPException: For invalid IDs or server errors
    """
    try:
        # Get employee info
        employee = await employees_collection.find_one({"user_id": employee_id})
        if not employee:
            raise HTTPException(status_code=404, detail=f"Employee not found with ID: {employee_id}")

        # Get timesheet data
        timesheet = await time_entries_collection.find_one({"user_id": employee_id})
        if not timesheet:
            raise HTTPException(status_code=404, detail="No timesheet found")

        # Find the specific invoice
        invoice = None
        for i, inv in enumerate(timesheet.get("invoices", [])):
            current_id = inv.get("qb_id", str(i + 1))  # Same logic as in get_employee_invoices
            if current_id == invoice_id:
                invoice = inv
                break

        if not invoice:
            raise HTTPException(status_code=404, detail="Invoice not found")

        # Calculate invoice totals
        total_hours = 0
        total_earnings = invoice.get("total_earnings", 0)
        
        # Calculate total hours from entries
        for day in invoice.get("days", []):
            for entry in day.get("entries", []):
                total_hours += entry.get("hours", 0) + (entry.get("minutes", 0) / 60)

        # Get the start and end dates from the invoice days
        start_date = None
        end_date = None
        if invoice.get("days") and len(invoice["days"]) > 0:
            try:
                # Sort days by date and get the earliest and latest
                sorted_days = sorted(invoice["days"], key=lambda x: parse_date(x.get("date")))
                if sorted_days:
                    start_date = sorted_days[0].get("date")
                    end_date = sorted_days[-1].get("date")
            except Exception as e:
                logger.error(f"Error getting date range from days: {str(e)}")

        # If no dates from days, try invoice start_date
        if not start_date and invoice.get("start_date"):
            start_date = invoice["start_date"]
        if not end_date and invoice.get("end_date"):
            end_date = invoice["end_date"]

        # Format dates for display
        start_display = start_date if start_date else 'N/A'
        end_display = end_date if end_date else start_display

        # Determine status (same logic as in get_employee_invoices)
        status = "draft"
        if invoice.get("submitted"):
            status = "sent"
        elif invoice.get("paid"):
            status = "paid"

        # Format the HTML statement
        html = f"""
        <html>
        <head>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 1000px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                .header {{
                    background-color: #4285f4;
                    color: white;
                    padding: 20px;
                    text-align: center;
                    border-radius: 8px 8px 0 0;
                }}
                .content {{
                    padding: 20px;
                    border: 1px solid #ddd;
                    border-top: none;
                    border-radius: 0 0 8px 8px;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 12px;
                    text-align: left;
                }}
                th {{
                    background-color: #f8f9fa;
                }}
                .total-row {{
                    font-weight: bold;
                    background-color: #f8f9fa;
                }}
                .invoice-info {{
                    display: flex;
                    justify-content: space-between;
                    margin-bottom: 20px;
                }}
                .status-badge {{
                    display: inline-block;
                    padding: 6px 12px;
                    border-radius: 4px;
                    font-weight: bold;
                    text-transform: uppercase;
                    font-size: 0.8em;
                }}
                .status-draft {{
                    background-color: #e9ecef;
                    color: #495057;
                }}
                .status-sent {{
                    background-color: #90EE90;  /* Light green */
                    color: #006400;  /* Dark green text for contrast */
                }}
                .status-paid {{
                    background-color: #34a853;
                    color: white;
                }}
                .footer {{
                    margin-top: 40px;
                    text-align: center;
                    color: #6c757d;
                    font-size: 0.9em;
                }}
                .date-range {{
                    margin-bottom: 10px;
                    font-size: 1.1em;
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Statement of Work</h1>
                <h2>{employee.get('first_name', '')} {employee.get('last_name', '')}</h2>
            </div>
            
            <div class="content">
                <div class="invoice-info">
                    <div>
                        <p><strong>Invoice #:</strong> {invoice_id}</p>
                        <div class="date-range">
                            <strong>Period:</strong> {start_display} - {end_display}
                        </div>
                    </div>
                    <div>
                        <span class="status-badge status-{status}">
                            {status.upper()}
                        </span>
                    </div>
                </div>

                <h3>Time Entries</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Date</th>
                            <th>Client</th>
                            <th>Category</th>
                            <th>Description</th>
                            <th>Hours</th>
                            <th>Amount</th>
                        </tr>
                    </thead>
                    <tbody>
        """

        # Add entries
        for day in sorted(invoice.get("days", []), key=lambda x: parse_date(x.get("date"))):
            date = day.get("date", "")
            for entry in day.get("entries", []):
                # Get client name
                client_name = entry.get("client_id", "Unknown Client")
                if entry.get("client_id"):
                    # First try looking up by client_id directly
                    client = await clients_collection.find_one({"client_id": entry["client_id"]})
                    
                    # If not found and it looks like an ObjectId, try that
                    if not client and len(str(entry["client_id"])) == 24:
                        try:
                            client = await clients_collection.find_one({"_id": ObjectId(str(entry["client_id"]))})
                        except Exception as e:
                            logger.error(f"Error looking up by ObjectId: {str(e)}")
                    
                    if client:
                        client_name = f"{client.get('First_Legal_Name', '')} {client.get('Last_Legal_Name', '')}".strip()

                hours = entry.get("hours", 0)
                minutes = entry.get("minutes", 0)
                time_str = f"{hours}h {minutes}m" if minutes else f"{hours}h"

                html += f"""
                    <tr>
                        <td>{date}</td>
                        <td>{client_name}</td>
                        <td>{entry.get('category', '')}</td>
                        <td>{entry.get('description', '')}</td>
                        <td>{time_str}</td>
                        <td>${entry.get('earnings', 0):.2f}</td>
                    </tr>
                """

        # Add totals
        html += f"""
                    </tbody>
                    <tfoot>
                        <tr class="total-row">
                            <td colspan="4" style="text-align: right"><strong>Totals:</strong></td>
                            <td><strong>{int(total_hours)}h {int((total_hours % 1) * 60)}m</strong></td>
                            <td><strong>${total_earnings:.2f}</strong></td>
                        </tr>
                    </tfoot>
                </table>

                <div class="footer">
                    <p>Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <p>This is an automatically generated statement. Please contact support if you have any questions.</p>
                </div>
            </div>
        </body>
        </html>
        """

        # Return the HTML content with appropriate headers
        return Response(
            content=html,
            media_type="text/html",
            headers={
                "Content-Disposition": f'attachment; filename="statement_{invoice_id}.html"'
            }
        )

    except Exception as e:
        logger.error(f"Error generating invoice statement: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 