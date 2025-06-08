"""
Upload App Constants Module

This module defines constants used throughout the upload application
for configuration, status codes, and file handling.

Features:
- Status codes
- File type definitions
- Configuration values
- Error messages

Dependencies:
- None (pure Python)

Author: Snapped Development Team
"""

# Upload status codes
UPLOAD_STATUS_PENDING = "pending"  # Initial upload status
UPLOAD_STATUS_PROCESSING = "processing"  # Files being processed
UPLOAD_STATUS_COMPLETE = "complete"  # Upload successfully completed
UPLOAD_STATUS_ERROR = "error"  # Error occurred during upload

# File type definitions
ALLOWED_FILE_TYPES = [  # List of allowed file MIME types
    "image/jpeg",
    "image/png",
    "application/pdf",
    "text/plain"
]

# Configuration values
MAX_FILE_SIZE = 10 * 1024 * 1024  # Maximum file size in bytes (10MB)
MAX_FILES_PER_SESSION = 100  # Maximum number of files per upload session

# Error messages
ERROR_FILE_TOO_LARGE = "File exceeds maximum size limit"
ERROR_INVALID_FILE_TYPE = "File type not allowed"
ERROR_SESSION_FULL = "Session has reached maximum file limit" 