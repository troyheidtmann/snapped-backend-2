"""
Employee Signup API - Employee Registration and Partner Management

This module provides FastAPI routes for managing employee registration and partner
search functionality in the Snapped platform. It handles employee creation,
partner search, and system diagnostics.

Features:
--------
1. Employee Registration:
   - New employee creation
   - Data validation
   - Automatic timestamp assignment
   - Client list initialization

2. Partner Management:
   - Partner search functionality
   - Case-insensitive name matching
   - Result limiting and pagination
   - Collection diagnostics

Data Flow:
---------
1. Employee Creation:
   - Validate input data
   - Add metadata (created_at, clients)
   - Insert into MongoDB
   - Verify insertion

2. Partner Search:
   - Validate search query
   - Perform regex search
   - Transform results
   - Handle errors

Security:
--------
- Input validation via Pydantic
- Error handling
- Debug logging
- Collection access control

Dependencies:
-----------
- FastAPI: Web framework and routing
- MongoDB: Data storage
- Pydantic: Data validation
- datetime: Timestamp management

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException
from app.features.employees.models.employee import EmployeeCreate
from app.shared.database import async_client
from datetime import datetime
from pydantic import ValidationError

router = APIRouter()

@router.post("/api/employees/signup")
async def create_employee(employee: EmployeeCreate):
    """
    Create a new employee record in the system.
    
    This endpoint handles the registration of new employees, including:
    - Data validation
    - Timestamp assignment
    - Client list initialization
    - Database insertion
    
    Args:
        employee (EmployeeCreate): Employee creation model containing:
            - email: Employee's email address
            - user_id: Unique identifier
            - first_name: Legal first name
            - last_name: Legal last name
            - date_of_birth: Birth date
            - phone_number: Contact number
            - company_id: Associated company (optional)
            
    Returns:
        dict: Creation status containing:
            - message: Success message
            - id: MongoDB document ID
            - user_id: Employee's user ID
            
    Raises:
        HTTPException: For validation or database errors
    """
    try:
        print(f"Received employee data: {employee.dict()}")
        
        # Get the employees collection
        employees_collection = async_client["Opps"]["Employees"]
        
        # Transform to dict and add created_at and empty clients array
        employee_dict = employee.dict()
        employee_dict["created_at"] = datetime.utcnow()
        employee_dict["clients"] = []  # Initialize empty clients array
        employee_dict["company_id"] = employee.company_id  # Make sure this is set
        
        print(f"About to insert document: {employee_dict}")
        
        # Insert into existing collection
        result = await employees_collection.insert_one(employee_dict)
        
        print(f"Insert result: {result.inserted_id}")  # Debug log
        
        # Verify the insert by fetching the document
        inserted_doc = await employees_collection.find_one({"_id": result.inserted_id})
        print(f"Verified inserted document: {inserted_doc}")  # Debug log
        
        if result.inserted_id:
            return {
                "message": "Employee created successfully", 
                "id": str(result.inserted_id),
                "user_id": employee.user_id
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to create employee")
            
    except ValidationError as e:
        print(f"Validation error: {str(e)}")  # Debug log
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        print(f"Error creating employee: {str(e)}")  # Debug log
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/partners/search/{query}")
async def search_partners(query: str):
    """
    Search for partners by name with case-insensitive matching.
    
    Args:
        query (str): Search string (minimum 3 characters)
        
    Returns:
        dict: Search results containing:
            - results: List of matching partners with:
                - id: Partner identifier
                - name: Partner name
                
    Raises:
        HTTPException: For database errors
        
    Notes:
        - Returns empty list for queries shorter than 3 characters
        - Results are limited to 10 matches
        - Matches are case-insensitive and prefix-based
    """
    print(f"Received search query: {query}")  # Debug log
    
    if len(query) < 3:
        return {"results": []}
        
    try:
        # Get the correct collection
        partners_collection = async_client["Partners"]["PartnerList"]
        
        # Case-insensitive search for partners by name
        search_regex = {"name": {"$regex": f"^{query}", "$options": "i"}}
        print(f"Search regex: {search_regex}")  # Debug log
        
        cursor = partners_collection.find(
            search_regex,
            {"_id": 1, "name": 1}
        ).limit(10)
        
        # Convert cursor to list
        partners = await cursor.to_list(length=None)
        print(f"Found partners: {partners}")  # Debug log
        
        # Transform ObjectId to string for JSON response
        results = [{"id": str(p["_id"]), "name": p["name"]} for p in partners]
        print(f"Returning results: {results}")  # Debug log
        
        return {"results": results}
        
    except Exception as e:
        print(f"Search error: {str(e)}")  # Debug log
        raise HTTPException(
            status_code=500,
            detail=f"Search failed: {str(e)}"
        )

@router.get("/api/partners/test")
async def test_partners_collection():
    """
    Test the partners collection connection and retrieve basic statistics.
    
    Returns:
        dict: Collection status containing:
            - status: Operation status
            - total_documents: Number of documents in collection
            - sample_document: Example document with:
                - id: Document identifier
                - name: Partner name
                
    Raises:
        HTTPException: For database connection errors
    """
    try:
        partners_collection = async_client["Partners"]["PartnerList"]
        count = await partners_collection.count_documents({})
        sample = await partners_collection.find_one({})
        return {
            "status": "success",
            "total_documents": count,
            "sample_document": {
                "id": str(sample["_id"]),
                "name": sample.get("name", "N/A")
            } if sample else None
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Collection test failed: {str(e)}"
        ) 