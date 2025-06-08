"""
Upload App Models Module

This module defines the data models used in the upload application
for handling file uploads and sessions.

Features:
- Session data models
- File metadata models
- Response models
- Status tracking models

Data Model:
- Session metadata
- File information
- Upload status
- Response formats

Dependencies:
- pydantic for data validation
- typing for type hints
- datetime for timestamps

Author: Snapped Development Team
"""

from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class FileMetadata(BaseModel):
    """
    File metadata model.
    
    Attributes:
        filename (str): Name of the file
        size (int): Size in bytes
        type (str): MIME type
        last_modified (datetime): Last modification timestamp
    """
    filename: str
    size: int
    type: str
    last_modified: datetime

class SessionData(BaseModel):
    """
    Session data model.
    
    Attributes:
        client_ID (str): Client identifier
        date (str): Session date
        files (List[FileMetadata]): List of files in session
        status (str): Current session status
    """
    client_ID: str
    date: str
    files: List[FileMetadata]
    status: str

class UploadResponse(BaseModel):
    """
    Upload response model.
    
    Attributes:
        success (bool): Operation success status
        message (str): Response message
        data (Optional[dict]): Additional response data
    """
    success: bool
    message: str
    data: Optional[dict] 