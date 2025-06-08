"""
Employee Onboarding API - New Employee Data Management

This module provides FastAPI routes for managing the employee onboarding process
in the Snapped platform. It handles the collection and storage of essential
employee information during the onboarding workflow.

Features:
--------
1. Employee Data Collection:
   - Personal information
   - Employment details
   - Department assignment
   - Reporting structure

2. Compliance Tracking:
   - W4 form completion
   - I9 documentation
   - Employee handbook
   - NDA status

Data Model:
----------
Employee Document Structure:
- Personal Info: Name, email, phone, DOB
- Employment: Position, department, start date
- Reporting: Manager assignment
- Compliance: Required document status
- Metadata: Creation timestamp, IDs

Security:
--------
- Input validation via Pydantic
- Error handling
- Debug logging
- Database verification

Dependencies:
-----------
- FastAPI: Web framework
- MongoDB: Data storage
- Pydantic: Data validation
- datetime: Timestamp handling

Author: Snapped Development Team
"""

from fastapi import HTTPException, APIRouter
from pydantic import BaseModel, Field
from datetime import datetime, timezone
from typing import Optional
from app.shared.database import async_client

router = APIRouter()

# Initialize database collection with correct database name
employees = async_client["Opps"]["Employees"]

class EmployeeOnboardingData(BaseModel):
    """
    Employee onboarding data model with required fields.
    
    Attributes:
        employee_id (str): Unique employee identifier
        first_name (str): Legal first name
        last_name (str): Legal last name
        email (str): Corporate email address
        phone (str): Contact phone number
        date_of_birth (str): Date of birth
        start_date (str): Employment start date
        department (str): Assigned department
        position (str): Job title/position
        reporting_to (str): Manager's employee ID
        completed_w4 (bool): W4 form completion status
        completed_i9 (bool): I9 form completion status
        signed_handbook (bool): Employee handbook signature status
        signed_nda (bool): NDA signature status
        
    Notes:
        - All fields are required except compliance flags
        - Compliance flags default to False
        - Dates should be in string format
    """
    employee_id: str = Field(...)
    first_name: str = Field(...)
    last_name: str = Field(...)
    email: str = Field(...)
    phone: str = Field(...)
    date_of_birth: str = Field(...)
    start_date: str = Field(...)
    department: str = Field(...)
    position: str = Field(...)
    reporting_to: str = Field(...)
    completed_w4: bool = Field(default=False)
    completed_i9: bool = Field(default=False)
    signed_handbook: bool = Field(default=False)
    signed_nda: bool = Field(default=False)

@router.post("/onboarding")
async def submit_onboarding_form(form_data: EmployeeOnboardingData):
    """
    Submit and process employee onboarding information.
    
    Handles the submission of new employee onboarding data, including:
    - Personal and employment information
    - Department assignment
    - Compliance document status
    
    Args:
        form_data (EmployeeOnboardingData): Complete onboarding information
        
    Returns:
        dict: Submission status with:
            - status: Success indicator
            - message: Status description
            
    Raises:
        HTTPException: For validation or database errors
        
    Notes:
        - Automatically adds creation timestamp
        - Performs database verification after save
        - Includes detailed debug logging
    """
    try:
        print("=== DEBUG ===")
        print("1. Raw form data received:", form_data.dict())
        print("2. Employee ID received:", form_data.employee_id)
        
        form_dict = form_data.dict()
        form_dict["created_at"] = datetime.now(timezone.utc)
        
        print("3. Data being saved to DB:", form_dict)
        
        result = await employees.insert_one(form_dict)
        print("4. MongoDB result:", result)
        
        # Verify what was actually saved
        saved_doc = await employees.find_one({"_id": result.inserted_id})
        print("5. Saved document:", saved_doc)
        
        if result.inserted_id:
            return {"status": "success", "message": "Employee onboarding completed successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to save employee data")
            
    except Exception as e:
        print(f"ERROR in onboarding:", str(e))
        raise HTTPException(status_code=500, detail=str(e)) 