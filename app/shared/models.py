"""
Shared Models Module

This module contains shared data models and enums used across
the application.

Features:
- Status enums
- Shared models
- Type definitions
- Constants
- Validators

Author: Snapped Development Team
"""

from enum import Enum

class QueueStatus(str, Enum):
    """
    Queue processing status.
    
    Attributes:
        PENDING: Not yet processed
        PROCESSING: Currently being processed
        COMPLETED: Successfully processed
        FAILED: Processing failed
        SKIPPED: Processing skipped
    """
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped" 