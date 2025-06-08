from fastapi import APIRouter

# Create router at module level with the correct prefix
router = APIRouter(prefix="/api/uploadapp")

# Import routes to register them
from .routes_uploadapp import *  # This will register the routes with our router

