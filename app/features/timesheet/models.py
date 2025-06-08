"""
Timesheet Data Models Module

This module defines the data models for the timesheet system, including time entries,
daily timesheets, and invoice generation.

Features:
- Time entry tracking
- Daily timesheet organization
- Session management
- Invoice generation
- Earnings calculation

Data Models:
- Time entries
- Daily records
- Timesheet sessions
- Invoice data
- Client tracking

Dependencies:
- Pydantic for validation
- datetime for timestamps
- typing for type hints

Author: Snapped Development Team
"""

from pydantic import BaseModel
from typing import Optional, List, Dict
from datetime import datetime

class TimeEntryItem(BaseModel):
    """
    Individual time entry model.
    
    Represents a single time entry for a client task.
    
    Attributes:
        client_id (str): Client identifier
        hours (int): Hours worked
        minutes (int): Additional minutes
        type (str): Entry type, defaults to "item based"
        item (str): Task item identifier
        description (str): Task description
        category (str): Task category
        earnings (float): Calculated earnings
        created_at (datetime): Entry creation time
        status (str): Entry status
    """
    client_id: str
    hours: int
    minutes: int
    type: str = "item based"
    item: str
    description: str
    category: str
    earnings: float = 0
    created_at: datetime = datetime.now()
    status: str = "active"

class DailyEntries(BaseModel):
    """
    Daily time entries collection model.
    
    Groups time entries by date with totals.
    
    Attributes:
        date (datetime): Entry date
        entries (List[TimeEntryItem]): List of time entries
        total_hours (float): Total hours for the day
        total_earnings (float): Total earnings for the day
    """
    date: datetime
    entries: List[TimeEntryItem] = []
    total_hours: float = 0
    total_earnings: float = 0

class DailyTimesheet(BaseModel):
    """
    Daily timesheet model.
    
    Represents a complete daily timesheet with status.
    
    Attributes:
        user_id (str): User identifier
        date (datetime): Timesheet date
        items (List[TimeEntryItem]): Time entries
        total_hours (float): Total hours
        total_earnings (float): Total earnings
        status (str): Timesheet status
        created_at (datetime): Creation timestamp
        invoice_id (Optional[str]): Associated invoice ID
    """
    user_id: str
    date: datetime
    items: List[TimeEntryItem]
    total_hours: float
    total_earnings: float
    status: str = "draft"  # draft, submitted, paid
    created_at: datetime = datetime.now()
    invoice_id: Optional[str] = None

class TimesheetSession(BaseModel):
    """
    Timesheet session model.
    
    Represents a complete timesheet session period.
    
    Attributes:
        user_id (str): User identifier
        start_date (datetime): Session start date
        end_date (datetime): Session end date
        daily_entries (Dict[str, DailyEntries]): Entries by date
        total_earnings (float): Total session earnings
        is_locked (bool): Session lock status
        status (str): Session status
        created_at (datetime): Creation timestamp
        invoice_id (Optional[str]): Associated invoice ID
    """
    user_id: str
    start_date: datetime
    end_date: datetime
    daily_entries: Dict[str, DailyEntries] = {}  # Key is ISO date string
    total_earnings: float = 0
    is_locked: bool = False
    status: str = "active"  # active, locked, submitted
    created_at: datetime = datetime.now()
    invoice_id: Optional[str] = None

class TimeEntryCreate(BaseModel):
    """
    Time entry creation model.
    
    Used for creating new time entries.
    
    Attributes:
        date (str): Entry date
        client_id (str): Client identifier
        hours (int): Hours worked
        minutes (int): Additional minutes
        type (str): Entry type
        item (str): Task item
        description (str): Task description
        category (str): Task category
    """
    date: str
    client_id: str
    hours: int
    minutes: int
    type: str
    item: str
    description: str
    category: str

class TimeEntryResponse(TimeEntryCreate):
    """
    Time entry response model.
    
    Extends creation model with additional fields.
    
    Attributes:
        id (str): Entry identifier
        created_at (datetime): Creation timestamp
        invoice_id (Optional[str]): Associated invoice ID
        invoice_status (str): Invoice status
    """
    id: str
    created_at: datetime
    invoice_id: Optional[str] = None
    invoice_status: str = 'draft'

class InvoiceGenerate(BaseModel):
    """
    Invoice generation model.
    
    Used for generating new invoices.
    
    Attributes:
        start_date (datetime): Invoice period start
        end_date (datetime): Invoice period end
        user_id (str): User identifier
    """
    start_date: datetime
    end_date: datetime
    user_id: str 