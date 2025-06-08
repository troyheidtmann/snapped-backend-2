from fastapi import APIRouter

router = APIRouter()

# Import routes to register them
from .routes_callform import *  # This will register the routes with our router 