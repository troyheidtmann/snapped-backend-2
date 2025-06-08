"""
Main Entry Module

This module serves as the application entry point, configuring FastAPI
and running the development server.

Features:
- Server configuration
- CORS setup
- Router mounting
- Development server
- Debug options

Data Model:
- API routes
- CORS settings
- Server config
- Debug flags
- Router tags

Security:
- CORS policies
- Origin validation
- Header control
- Method control
- Credential handling

Dependencies:
- FastAPI for API
- CORS middleware
- uvicorn for server
- Router modules
- Logging

Author: Snapped Development Team
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.features.timesheet.routes_timesheet import router as timesheet_router
from app.features.privacy.routes_privacy import router as privacy_router
from app.features.cdn.cdn_mongo import router as cdn_router
# ... other imports ...

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add routers
app.include_router(timesheet_router)
app.include_router(privacy_router)
app.include_router(cdn_router, prefix="/api/cdn-mongo")
# ... other routers ...

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,  # Make sure we're using port 8000
        reload=True,
        log_level="debug"
    )