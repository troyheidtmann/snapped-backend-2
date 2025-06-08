"""
Employee Data Models - Snapped Platform Employee Management

This module defines the Pydantic models for managing employee data in the Snapped platform.
It includes models for employee creation, basic information, work metrics, timesheets,
and invoice management.

Models Overview:
--------------
1. Employee Information:
   - EmployeeCreate: New employee registration
   - EmployeeBasic: Core employee data
   - EmployeeMetrics: Performance and work statistics

2. Time Tracking:
   - TimesheetEntry: Individual work entries
   - EmployeeTimesheet: Period-based work summary
   - ClientWorkSummary: Client-specific work metrics

3. Financial Management:
   - InvoiceSummary: Invoice status and totals
   - EmployeeInvoices: Employee financial overview

Data Flow:
---------
1. Employee Creation:
   - Registration via EmployeeCreate
   - Automatic user_id generation
   - Basic profile storage

2. Work Management:
   - Time entry via TimesheetEntry
   - Aggregation in EmployeeTimesheet
   - Metrics calculation in EmployeeMetrics

3. Financial Processing:
   - Invoice generation from timesheets
   - Status tracking in InvoiceSummary
   - Payment management in EmployeeInvoices

Dependencies:
-----------
- Pydantic: Data validation
- datetime: Date/time handling
- typing: Type hints

Author: Snapped Development Team
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict
from datetime import date, datetime

class EmployeeCreate(BaseModel):
    """
    Model for creating new employee records.
    
    Attributes:
        email (str): Employee's email address
        user_id (str): Unique identifier (generated from name/DOB)
        first_name (str): Legal first name
        last_name (str): Legal last name
        date_of_birth (str): Birth date in MM/DD/YYYY format
        phone_number (str): Contact number with country code
        company_id (Optional[str]): Associated company identifier
    """
    email: str
    user_id: str
    first_name: str
    last_name: str
    date_of_birth: str
    phone_number: str
    company_id: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "email": "user@example.com",
                "user_id": "th10021994",
                "first_name": "Troy",
                "last_name": "Harrison",
                "date_of_birth": "10/02/1994",
                "phone_number": "+11234567890"
            }
        }
        json_encoders = {
            date: lambda v: v.isoformat()
        }

    def generate_user_id(self) -> str:
        """
        Generate a unique user ID from name and birth date.
        
        Returns:
            str: User ID in format 'fmMMDDYYYY' where:
                - f: First name initial (lowercase)
                - m: Last name initial (lowercase)
                - MMDDYYYY: Birth date
        """
        # Get initials
        first_initial = self.first_name[0].lower() if self.first_name else ''
        last_initial = self.last_name[0].lower() if self.last_name else ''
        
        # Format birthday to MMDDYYYY
        formatted_date = self.date_of_birth.strftime('%m%d%Y')
        
        # Combine initials and date
        return f"{first_initial}{last_initial}{formatted_date}"

class EmployeeBasic(BaseModel):
    """
    Basic employee information model.
    
    Attributes:
        id (Optional[str]): MongoDB document ID
        user_id (str): Unique employee identifier
        first_name (str): Legal first name
        last_name (str): Legal last name
        email (str): Contact email
        date_of_birth (Optional[str]): Birth date
        phone_number (Optional[str]): Contact number
        company_id (Optional[str]): Associated company
        created_at (Optional[datetime]): Account creation timestamp
        clients (Optional[List[str]]): Assigned client IDs
        rate (Optional[str]): Pay rate
        department (Optional[str]): Department assignment
        position (Optional[str]): Job title
        active (bool): Employment status
    """
    id: Optional[str] = Field(None, alias="_id")
    user_id: str
    first_name: str
    last_name: str
    email: str
    date_of_birth: Optional[str] = None
    phone_number: Optional[str] = None
    company_id: Optional[str] = None
    created_at: Optional[datetime] = None
    clients: Optional[List[str]] = []
    rate: Optional[str] = None
    department: Optional[str] = None
    position: Optional[str] = None
    active: bool = True

    class Config:
        populate_by_name = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class ClientWorkSummary(BaseModel):
    """
    Summary of work performed for a specific client.
    
    Attributes:
        client_id (str): Client identifier
        client_name (str): Client's business name
        total_hours (float): Total hours worked
        total_earnings (float): Total earnings for work
        last_worked (datetime): Most recent work date
        categories (Dict[str, float]): Hours by work category
    """
    client_id: str
    client_name: str
    total_hours: float
    total_earnings: float
    last_worked: datetime
    categories: Dict[str, float]  # category -> hours

class EmployeeMetrics(BaseModel):
    """
    Employee performance and productivity metrics.
    
    Attributes:
        user_id (str): Employee identifier
        period_start (datetime): Start of measurement period
        period_end (datetime): End of measurement period
        total_hours (float): Total hours in period
        total_earnings (float): Total earnings in period
        active_clients (int): Number of active clients
        avg_hours_per_day (float): Average daily hours
        most_worked_client (Optional[str]): Top client by hours
        most_worked_category (Optional[str]): Top work category
        client_work_summary (List[ClientWorkSummary]): Per-client metrics
        productivity_score (Optional[float]): Productivity rating
        efficiency_rating (Optional[str]): Efficiency level
    """
    user_id: str
    period_start: datetime
    period_end: datetime
    total_hours: float
    total_earnings: float
    active_clients: int
    avg_hours_per_day: float
    most_worked_client: Optional[str] = None
    most_worked_category: Optional[str] = None
    client_work_summary: List[ClientWorkSummary] = []
    productivity_score: Optional[float] = None
    efficiency_rating: Optional[str] = None

class TimesheetEntry(BaseModel):
    """
    Individual timesheet entry for work performed.
    
    Attributes:
        date (str): Work date
        client_id (str): Client identifier
        client_name (str): Client's business name
        hours (int): Full hours worked
        minutes (int): Additional minutes
        category (str): Work category
        item (str): Specific task
        description (str): Work description
        earnings (float): Entry earnings
    """
    date: str
    client_id: str
    client_name: str
    hours: int
    minutes: int
    category: str
    item: str
    description: str
    earnings: float

class EmployeeTimesheet(BaseModel):
    """
    Complete timesheet for a work period.
    
    Attributes:
        user_id (str): Employee identifier
        employee_name (str): Full employee name
        period_start (datetime): Timesheet start date
        period_end (datetime): Timesheet end date
        entries (List[TimesheetEntry]): Work entries
        total_hours (float): Period total hours
        total_earnings (float): Period total earnings
        days_worked (int): Number of work days
    """
    user_id: str
    employee_name: str
    period_start: datetime
    period_end: datetime
    entries: List[TimesheetEntry] = []
    total_hours: float
    total_earnings: float
    days_worked: int

class InvoiceSummary(BaseModel):
    """
    Summary of an individual invoice.
    
    Attributes:
        invoice_id (str): Unique invoice identifier
        start_date (Optional[datetime]): Period start
        end_date (Optional[datetime]): Period end
        total_hours (float): Invoice total hours
        total_earnings (float): Invoice amount
        status (str): Current status
        submitted_date (Optional[datetime]): Submission date
        qb_id (Optional[str]): QuickBooks reference
    """
    invoice_id: str
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    total_hours: float
    total_earnings: float
    status: str
    submitted_date: Optional[datetime]
    qb_id: Optional[str]

class EmployeeInvoices(BaseModel):
    """
    Collection of employee invoices with totals.
    
    Attributes:
        user_id (str): Employee identifier
        employee_name (str): Full employee name
        invoices (List[InvoiceSummary]): All invoices
        total_unpaid (float): Sum of sent invoices
        total_paid (float): Sum of paid invoices
        total_draft (float): Sum of draft invoices
    """
    user_id: str
    employee_name: str
    invoices: List[InvoiceSummary]
    total_unpaid: float = 0  # Amount in SENT status
    total_paid: float = 0
    total_draft: float = 0  # Amount in DRAFT status 