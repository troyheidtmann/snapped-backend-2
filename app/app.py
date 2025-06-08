"""
Main Application Module

This module serves as the primary FastAPI application entry point,
configuring routes, middleware, and static file serving.

Features:
- Route management
- CORS configuration
- Static file serving
- Error handling
- Request logging

Data Model:
- API routes
- Static files
- Request data
- Response data
- Error logs

Security:
- CORS policies
- Error handling
- Request validation
- Access control
- Secure headers

Dependencies:
- FastAPI for routing
- CORS middleware
- Static files
- Logging
- Database

Author: Snapped Development Team
"""

# First import database to ensure it's initialized first
from .shared.database import lifespan

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
import os
import logging


# Now import all routers
from .features.messages.routes_messages import router as messages_router
from .features.callform.routes_callform import router as callform_router
from .features.posting.queue_builder import router as queue_router
from .features.posting.post_processor import router as posting_router
from .features.uploadapp.routes_uploadapp import router as upload_router
from .features.posting.make_processor import router as make_router
from .features.uploadtracker.routes_uploadtracker import router as uploadtracker_router
from .features.lead.route_lead import router as lead_router, router_singular as lead_router_singular
from .features.lead.route_dashboard import router as dashboard_router
from .features.onboarding.routes_onboarding import router as onboarding_router
from .features.tasks.routes_tasks import router as tasks_router
from .features.contracts.routes_contracts import router as contracts_router
from .features.tiktok.routes_tiktok import router as tiktok_router
from .features.posting.spot_queue_builder import router as spot_queue_router
from .features.posting.spot_make_processor import router as spot_make_router
from .features.partners.routes_partners import router as partners_router
from .features.cdn.routes_cdn import router as cdn_router, content_dump_router
from .features.bunnyscan.bunny_scanner import router as bunnyscan_router
from .features.employees.routes.signup import router as employees_router
from .features.employees.routes.employee_management import router as employee_management_router
from .features.analytics.route_analytics import router as analytics_router
from .features.analytics.route_data_endpoint import router as data_router
from .features.timesheet.routes_timesheet import router as timesheet_router
from .features.demosite.routes_demosite import router as demosite_router
from .features.support.routes_support import router as support_router
from .features.posting.saved_queue_builder import router as saved_queue_router
from .features.posting.saved_make_processor import router as saved_make_router
from .features.videosummary.services.twelve_labs import router as twelve_labs_router
from .features.videosummary.VideoSummaryReview import router as video_summary_router
from .features.survey.survey_routes import router as survey_router
from .features.AIChat.routes_AIChat import router as aichat_router
from .features.desktop_upload import router as desktop_upload_router
from .features.payments.routes_payments import router as payments_router
from .features.payments.quickbooks_webhook import router as quickbooks_router
from .features.payments.routes_splits import router as splits_router
from .features.cdn.cdn_mongo import router as cdn_mongo_router
from .features.clients.routes_clients import router as clients_router
from .features.social.routes_social import router as social_router

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(lifespan=lifespan)

# CORS middleware setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*", "Authorization"],  # Explicitly allow Authorization header
    expose_headers=["*"],
)

# Mount static files from the React build directory using absolute paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILD_DIR = os.path.join(BASE_DIR, "snapped-web", "build")
STATIC_DIR = os.path.join(BUILD_DIR, "static")

logger.info(f"Static directory: {STATIC_DIR}")
logger.info(f"Build directory: {BUILD_DIR}")

# Ensure directories exist
if not os.path.exists(STATIC_DIR):
    logger.error(f"Static directory does not exist: {STATIC_DIR}")
    raise RuntimeError(f"Static directory does not exist: {STATIC_DIR}")

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# Include the API routers
logger.info("Mounting API routers...")

# Mount messages router first to ensure it takes precedence
app.include_router(messages_router)

# Then mount other routers
app.include_router(upload_router)
app.include_router(callform_router)
app.include_router(queue_router)
app.include_router(payments_router)
app.include_router(quickbooks_router)
app.include_router(splits_router)
app.include_router(clients_router, prefix="/api")
app.include_router(cdn_mongo_router, prefix="/api/cdn-mongo", tags=["cdn-mongo"])
app.include_router(
    timesheet_router,
    prefix="/api",
    tags=["timesheet"]
)
app.include_router(posting_router)
app.include_router(make_router)
app.include_router(uploadtracker_router)
app.include_router(lead_router, prefix="/api")
app.include_router(lead_router_singular, prefix="/api")
app.include_router(dashboard_router, prefix="/api")
app.include_router(contracts_router)
app.include_router(onboarding_router, prefix="/api")
app.include_router(tasks_router, prefix="/api")
app.include_router(tiktok_router)
app.include_router(spot_queue_router)
app.include_router(spot_make_router)
app.include_router(partners_router)
app.include_router(cdn_router, tags=["cdn"])
app.include_router(content_dump_router, prefix="/api", tags=["content-dump"])
app.include_router(bunnyscan_router)
app.include_router(employees_router)
app.include_router(employee_management_router, prefix="/api", tags=["employee_management"])
app.include_router(analytics_router)
app.include_router(data_router)
app.include_router(demosite_router)
app.include_router(support_router)
app.include_router(saved_queue_router)
app.include_router(saved_make_router)
app.include_router(twelve_labs_router)
app.include_router(video_summary_router, tags=["ai-review"])
app.include_router(survey_router, tags=["survey"])
app.include_router(aichat_router, prefix="/api", tags=["ai-chat"])
app.include_router(
    desktop_upload_router,
    prefix="/api",
    tags=["desktop-upload"]
)
app.include_router(social_router, prefix="/api")

# Catch-all route LAST
@app.api_route("/{full_path:path}", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
async def serve_app(full_path: str):
    """
    Serve React app for non-API routes.
    
    Args:
        full_path: Request path
        
    Returns:
        FileResponse: React app
        
    Raises:
        HTTPException: For errors
        
    Notes:
        - Handles all paths
        - Serves index.html
        - API fallback
        - Error handling
    """
    # Skip this handler for API routes
    if full_path.startswith("api") or full_path.startswith("/api"):
        raise HTTPException(status_code=404, detail="API route not found")
        
    # Serve index.html for all other routes
    index_path = os.path.join(BUILD_DIR, "index.html")
    if not os.path.exists(index_path):
        logger.error(f"index.html not found at {index_path}")
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(index_path)

@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    """
    Log HTTP requests and responses.
    
    Args:
        request: HTTP request
        call_next: Next handler
        
    Returns:
        Response: HTTP response
        
    Notes:
        - Logs requests
        - Logs responses
        - Error handling
        - Status tracking
    """
    logger.info(f"Incoming request: {request.method} {request.url}")
    try:
        response = await call_next(request)
        logger.info(f"Response status: {response.status_code}")
        return response
    except Exception as e:
        logger.error(f"Request failed: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"detail": str(e)}
        )