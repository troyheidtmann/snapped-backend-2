"""
CDN Service Data Models

This module defines the Pydantic models used for request/response validation in the CDN service.
These models ensure type safety and data validation for operations involving content management,
file operations, and folder organization.

Models Overview:
--------------
1. Content Management:
   - ListContentsRequest: Pagination for content listing
   - SyncRequest: Batch operations synchronization
   - MoveFileRequest: File relocation between sessions

2. File Operations:
   - UpdateCaptionRequest: Content caption management
   - FileOrder: Sequence control for content display
   - ReorderFilesRequest: Batch sequence updates

3. Folder Operations:
   - FolderOperationRequest: Directory management and organization

Usage Context:
------------
These models are used in FastAPI endpoints to:
- Validate incoming request data
- Ensure type safety during operations
- Maintain consistent data structures
- Support batch operations
- Handle optional parameters
"""

from pydantic import BaseModel
from typing import Dict, List, Optional

class ListContentsRequest(BaseModel):
    """
    Request model for paginated content listing.
    
    Attributes:
        path: Directory path to list contents from
        page: Page number for pagination (starts at 1)
        limit: Maximum items per page (default 50)
    """
    path: str
    page: Optional[int] = 1
    limit: Optional[int] = 50

class SyncRequest(BaseModel):
    """
    Request model for batch synchronization operations.
    
    Used for synchronizing multiple operations across sessions:
    - File moves
    - Metadata updates
    - Content reorganization
    
    Attributes:
        session_id: Target session identifier
        operations: Dictionary of operation lists by type
                   Example: {"move": [{...}], "update": [{...}]}
    """
    session_id: str
    operations: Dict[str, List[Dict]]

class MoveFileRequest(BaseModel):
    """
    Request model for file movement operations.
    
    Handles file transfers between:
    - Different sessions
    - Content types (Stories/Spotlight/etc.)
    - Directory locations
    
    Attributes:
        source_path: Original file location
        destination_path: Target file location
        file_name: Name of file to move
        target_session: Destination session ID
        is_thumbnail: Whether file is a thumbnail
        seq_number: Optional display sequence position
    """
    source_path: str
    destination_path: str
    file_name: str
    target_session: str
    is_thumbnail: Optional[bool] = False
    seq_number: Optional[int] = None

class UpdateCaptionRequest(BaseModel):
    """
    Request model for updating file captions.
    
    Used for:
    - Adding new captions
    - Updating existing captions
    - Managing content descriptions
    
    Attributes:
        session_id: Session containing the file
        file_name: Target file name
        caption: New caption text
    """
    session_id: str
    file_name: str
    caption: str

class FileOrder(BaseModel):
    """
    Model for file sequence ordering.
    
    Used in batch reordering operations to:
    - Set display sequence
    - Organize content presentation
    - Manage viewing order
    
    Attributes:
        file_name: Target file name
        seq_number: New sequence position
    """
    file_name: str
    seq_number: int

class ReorderFilesRequest(BaseModel):
    """
    Request model for batch file reordering.
    
    Handles:
    - Multiple file reordering
    - Sequence management
    - Display order updates
    
    Attributes:
        session_id: Target session
        file_order: List of file ordering instructions
    """
    session_id: str
    file_order: List[FileOrder]

class FolderOperationRequest(BaseModel):
    """
    Request model for folder operations.
    
    Supports:
    - Folder creation
    - Content movement
    - Directory organization
    - Content type management
    
    Attributes:
        operation: Operation type ('move' or 'create_folder')
        destination_path: Target directory path
        items: Optional list of items to process
        target_session: Optional target session ID
        content_type: Content category (default: 'STORIES')
    """
    operation: str  # 'move', 'create_folder'
    destination_path: str
    items: Optional[List[str]] = None
    target_session: Optional[str] = None
    content_type: Optional[str] = "STORIES"  # Add default content type 