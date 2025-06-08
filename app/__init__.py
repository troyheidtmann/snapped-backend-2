from flask import Blueprint
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Create Blueprint for API routes
api_bp = Blueprint('api', __name__)

# Import routes after Blueprint creation to avoid circular imports
from app.shared import database 

# Empty file to make app a package 