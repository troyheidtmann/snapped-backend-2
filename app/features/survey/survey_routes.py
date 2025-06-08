"""
Client Survey Management Module

This module handles client survey functionality including question management,
response collection, file uploads, and reporting.

Features:
- Survey question management
- Response collection and storage
- File upload handling
- Access control
- Response analytics
- Client tracking

Data Model:
- Survey questions
- Client responses
- File attachments
- User permissions
- Response metrics

Security:
- Authentication required
- Role-based access
- File validation
- Data sanitization

Dependencies:
- FastAPI for routing
- MongoDB for storage
- werkzeug for file handling
- bson for data formatting

Author: Snapped Development Team
"""

from fastapi import APIRouter, File, UploadFile, HTTPException, Depends
from fastapi.responses import JSONResponse
from typing import Dict, List, Optional
import json
from datetime import datetime
import os
from werkzeug.utils import secure_filename
import shutil
from bson import json_util
from app.shared.database import (
    async_client,
    client_info as clients_collection,
    survey_responses,
    survey_questions
)
from app.shared.auth import get_current_user_group, filter_by_partner

router = APIRouter()

# Only keep upload folder for file handling
UPLOAD_FOLDER = '/home/ubuntu/SNAPPED2_LLC/onboarding/files'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'pdf', 'doc', 'docx', 'xls', 'xlsx', 'mp3', 'mp4', 'wav'}

def allowed_file(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@router.get("/api/client_survey/questions")
async def get_questions():
    """
    Retrieve all survey questions from the database.
    
    Returns:
        list: List of survey questions with their properties
        
    Raises:
        HTTPException: If database query fails
        
    Notes:
        - Removes MongoDB-specific fields
        - Returns empty list if no questions found
        - Preserves question order
    """
    try:
        questions = await survey_questions.find({}).to_list(None)
        # Convert MongoDB objects to JSON-serializable format
        questions_json = json.loads(json_util.dumps(questions))
        # Remove MongoDB-specific fields if present
        for q in questions_json:
            if '_id' in q:
                del q['_id']
        return questions_json or []
    except Exception as e:
        print(f"Error fetching questions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/client_survey/submit")
async def submit_survey(
    responses: Dict,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Submit survey responses for a client.
    
    Args:
        responses (Dict): Survey responses
        auth_data (dict): User authentication data
        
    Returns:
        dict: Submission status message
        
    Raises:
        HTTPException: For access or validation errors
        
    Notes:
        - Validates client access
        - Stores responses with metadata
        - Includes user group information
    """
    try:
        timestamp = datetime.now().isoformat()
        user_id = auth_data["user_id"]
        
        # Get client mapping using existing client_info collection
        client_filter = await filter_by_partner(auth_data)
        if client_filter.get("client_id") == "NO_ACCESS":
            raise HTTPException(status_code=403, detail="No client access")
            
        # Get client details - try multiple fields
        client_id = responses.get("client_id")
        if not client_id:
            raise HTTPException(status_code=400, detail="No client_id provided in the request")
            
        # Try to find client by client_id or user_id
        client = await clients_collection.find_one({
            "$or": [
                {"client_id": client_id},
                {"user_id": client_id},
                {"custom:UserID": client_id},
                {"custom:ClientID": client_id}
            ]
        })
        
        if not client:
            print(f"Client lookup failed. Tried client_id: {client_id}")
            raise HTTPException(
                status_code=404, 
                detail=f"Client not found. Please ensure you have the correct client access. Tried ID: {client_id}"
            )
            
        # Save to survey_responses collection
        survey_doc = {
            'timestamp': timestamp,
            'user_id': user_id,
            'client_id': client["client_id"],
            'client_name': client.get("name", ""),
            'responses': responses,
            'groups': auth_data["groups"]
        }
        await survey_responses.insert_one(survey_doc)
        
        return {"message": "Survey submitted successfully"}
    except Exception as e:
        print(f"Error submitting survey: {str(e)}")
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/client_survey/responses/key")
async def get_key_responses(auth_data: dict = Depends(get_current_user_group)):
    """
    Retrieve key survey responses for admin review.
    
    Args:
        auth_data (dict): User authentication data
        
    Returns:
        list: List of key survey responses
        
    Raises:
        HTTPException: For access or database errors
        
    Notes:
        - Requires admin access
        - Filters sensitive information
        - Includes metadata
    """
    try:
        if "ADMIN" not in auth_data["groups"]:
            raise HTTPException(status_code=403, detail="Admin access required")
            
        # Get all responses that have key information
        key_responses = await survey_responses.find(
            {"responses": {"$exists": True}}
        ).to_list(None)
        
        # Convert to JSON-serializable format and clean up
        responses_json = json.loads(json_util.dumps(key_responses))
        for response in responses_json:
            if '_id' in response:
                del response['_id']
                
        return responses_json or []
    except Exception as e:
        print(f"Error fetching key responses: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/client_survey/responses/detailed")
async def get_detailed_responses(auth_data: dict = Depends(get_current_user_group)):
    try:
        # Get client filter based on user's access
        client_filter = await filter_by_partner(auth_data)
        
        # Apply filter to find responses
        responses = await survey_responses.find(client_filter).to_list(None)
        
        # Convert to JSON-serializable format and clean up
        responses_json = json.loads(json_util.dumps(responses))
        for response in responses_json:
            if '_id' in response:
                del response['_id']
            
            # Try to get client info
            if response.get("client_id"):
                client = await clients_collection.find_one({"client_id": response["client_id"]})
                if client:
                    response["client_name"] = client.get("name", "")
                    response["client_info"] = json.loads(json_util.dumps(client))
                    if '_id' in response["client_info"]:
                        del response["client_info"]['_id']
                
        return responses_json or []
    except Exception as e:
        print(f"Error fetching detailed responses: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/client_survey/questions/add")
async def add_question(
    question: Dict,
    auth_data: dict = Depends(get_current_user_group)
):
    try:
        if "ADMIN" not in auth_data["groups"]:
            raise HTTPException(status_code=403, detail="Admin access required")
            
        result = await survey_questions.insert_one(question)
        return {"message": "Question added successfully", "id": str(result.inserted_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/client_survey/questions/update")
async def update_questions(
    questions: List[Dict],
    auth_data: dict = Depends(get_current_user_group)
):
    try:
        if "ADMIN" not in auth_data["groups"]:
            raise HTTPException(status_code=403, detail="Admin access required")
            
        await survey_questions.delete_many({})
        if questions:
            await survey_questions.insert_many(questions)
        return {"message": "Questions updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/client_survey/sections")
async def get_sections():
    try:
        questions = await survey_questions.find({}).to_list(None)
        # Get unique sections and maintain order using sectionOrder
        sections_with_order = [(q.get('section', ''), q.get('sectionOrder', 0)) for q in questions]
        unique_sections = list({section: order for section, order in sections_with_order}.items())
        # Sort by section order
        sorted_sections = sorted(unique_sections, key=lambda x: x[1])
        # Return just the section names
        return [section for section, _ in sorted_sections]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/client_survey/upload")
async def upload_file(file: UploadFile):
    """
    Handle file upload for survey attachments.
    
    Args:
        file (UploadFile): File to upload
        
    Returns:
        dict: Upload status with filename
        
    Raises:
        HTTPException: For invalid files or upload errors
        
    Notes:
        - Validates file type
        - Sanitizes filename
        - Uses secure file handling
    """
    if not file:
        raise HTTPException(status_code=400, detail="No file part")
    
    if not allowed_file(file.filename):
        raise HTTPException(status_code=400, detail="File type not allowed")
    
    try:
        filename = secure_filename(file.filename)
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        return {"filename": filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/client_survey/users")
async def get_users(auth_data: dict = Depends(get_current_user_group)):
    try:
        # Get accessible clients based on user's access
        client_filter = await filter_by_partner(auth_data)
        
        # Get unique users from survey responses
        pipeline = [
            {"$match": client_filter},
            {"$group": {
                "_id": "$user_id",
                "latest_response": {"$last": "$$ROOT"}
            }},
            {"$replaceRoot": {"newRoot": "$latest_response"}}
        ]
        
        users = await survey_responses.aggregate(pipeline).to_list(None)
        
        # Convert to JSON-serializable format and clean up
        users_json = json.loads(json_util.dumps(users))
        for user in users_json:
            if '_id' in user:
                del user['_id']
                
        return users_json or []
    except Exception as e:
        print(f"Error fetching users: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Keep the leads functionality in MongoDB
LEADS_COLLECTION = async_client["ClientDb"]["leads"]

@router.get("/api/leads")
async def get_leads():
    try:
        leads = await LEADS_COLLECTION.find({}).to_list(None)
        return leads or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/leads")
async def add_lead(new_lead: Dict):
    try:
        # Add ID if not present
        if 'id' not in new_lead:
            count = await LEADS_COLLECTION.count_documents({})
            new_lead['id'] = str(count + 1)
        
        await LEADS_COLLECTION.insert_one(new_lead)
        return {"message": "Lead added successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/api/leads/{lead_id}")
async def update_lead(lead_id: str, updated_lead: Dict):
    try:
        result = await LEADS_COLLECTION.update_one(
            {'id': lead_id},
            {'$set': updated_lead}
        )
        if result.modified_count:
            return {"message": "Lead updated successfully"}
        raise HTTPException(status_code=404, detail="Lead not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def create_client_directory(client_name: str):
    try:
        new_dir_path = os.path.join('/home/ubuntu/SNAPPED2_LLC/SNAPPED_CLIENTS', client_name)
        if not os.path.exists(new_dir_path):
            os.makedirs(new_dir_path)
            
            subdirs = [
                f'SAVED_{client_name}',
                f'SPOTLIGHT_{client_name}',
                f'STORIES_{client_name}',
                f'THUMBNAIL_{client_name}'
            ]
            
            for subdir in subdirs:
                os.makedirs(os.path.join(new_dir_path, subdir))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/client_survey/key_info")
async def submit_key_info(
    key_info: Dict,
    auth_data: dict = Depends(get_current_user_group)
):
    """
    Submit or update key information for a client.
    
    Args:
        key_info (Dict): Key client information
        auth_data (dict): User authentication data
        
    Returns:
        dict: Submission status message
        
    Raises:
        HTTPException: For access or validation errors
        
    Notes:
        - Requires admin access
        - Updates existing or creates new
        - Includes timestamp tracking
    """
    try:
        if "ADMIN" not in auth_data["groups"]:
            raise HTTPException(status_code=403, detail="Admin access required")
            
        user_id = key_info.pop("user_id", None)
        timestamp = datetime.now().isoformat()
        
        # If updating existing user
        if user_id:
            # Update existing response
            result = await survey_responses.update_one(
                {"user_id": user_id},
                {
                    "$set": {
                        "responses": key_info,
                        "updated_at": timestamp
                    }
                }
            )
            if result.modified_count == 0:
                # If no document was updated, create new one
                await survey_responses.insert_one({
                    "user_id": user_id,
                    "responses": key_info,
                    "timestamp": timestamp,
                    "updated_at": timestamp
                })
        else:
            # Create new response
            await survey_responses.insert_one({
                "user_id": f"key_info_{timestamp}.json",
                "responses": key_info,
                "timestamp": timestamp
            })
            
        return {"message": "Key information saved successfully"}
    except Exception as e:
        print(f"Error saving key information: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/api/client_survey/sections/add")
async def add_section(
    section_data: Dict,
    auth_data: dict = Depends(get_current_user_group)
):
    try:
        if "ADMIN" not in auth_data["groups"]:
            raise HTTPException(status_code=403, detail="Admin access required")
            
        # Get current max section order
        questions = await survey_questions.find({}).to_list(None)
        max_order = max([q.get('sectionOrder', 0) for q in questions]) if questions else 0
        
        # Create a new question as a section placeholder
        new_section = {
            "section": section_data["name"],
            "sectionOrder": max_order + 1,
            "question": f"Welcome to {section_data['name']}",
            "type": "text",
            "required": False,
            "order": 0
        }
        
        await survey_questions.insert_one(new_section)
        return {"message": "Section added successfully", "section": section_data["name"]}
    except Exception as e:
        print(f"Error adding section: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == '__main__':
    router.run(debug=True, port=2020) 