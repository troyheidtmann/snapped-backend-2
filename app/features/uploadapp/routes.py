"""
Upload App Routes Module

This module defines the FastAPI routes for the upload application,
handling file uploads, session management, and status updates.

Features:
- File upload endpoints
- Session management
- Status tracking
- Error handling
- Client integration

Data Model:
- Upload sessions
- File metadata
- Response formats
- Status codes

Security:
- Authentication required
- File validation
- Size limits
- Type checking

Dependencies:
- FastAPI for routing
- MongoDB for storage
- Pydantic for validation
- typing for type hints

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException
from datetime import datetime

router = APIRouter()

@router.get("/uploadapp/editor-notes/{client_id}")
async def get_editor_notes(client_id: str):
    """Get all editor notes for a client"""
    try:
        # Find all notes for this client across all users
        pipeline = [
            {
                "$unwind": "$notes"
            },
            {
                "$match": {
                    "notes.client_id": client_id,
                }
            },
            {
                "$project": {
                    "editor_note": "$notes.editor_note",
                    "folder_id": "$notes.folder_id",
                    "created_at": "$notes.created_at",
                    "user_id": "$user_id"
                }
            },
            {
                "$sort": {"created_at": -1}
            }
        ]
        
        notes = await notif_db.EditNotes.aggregate(pipeline).to_list(None)
        return {"notes": notes}
    except Exception as e:
        print(f"Error getting editor notes: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/uploadapp/update-editor-note")
async def update_editor_note(note_data: dict):
    """Add/Update editor note"""
    try:
        user_id = note_data.get("user_id")
        client_id = note_data.get("client_id")
        folder_id = note_data.get("folder_id")
        editor_note = note_data.get("editor_note")

        if not all([user_id, client_id, folder_id, editor_note]):
            raise HTTPException(status_code=400, detail="Missing required fields")

        # Add new note to the user's notes array
        await notif_db.EditNotes.update_one(
            {"user_id": user_id},
            {
                "$push": {
                    "notes": {
                        "client_id": client_id,
                        "folder_id": folder_id,
                        "editor_note": editor_note,
                        "created_at": datetime.utcnow()
                    }
                }
            },
            upsert=True
        )

        return {"status": "success"}
    except Exception as e:
        print(f"Error updating editor note: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 