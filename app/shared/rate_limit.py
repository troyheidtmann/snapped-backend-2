"""
Rate Limiting Module

This module provides rate limiting functionality using Redis as a backend.
It implements a distributed rate limiter that can be used across multiple server instances.

Features:
- Distributed rate limiting with Redis
- Per-IP and per-endpoint limiting
- Configurable rate limits
- Automatic cleanup
- Burst handling
- Penalty system for repeated violations

Security:
- DoS protection
- Burst protection
- Redis security
- Error handling
- Logging
"""

from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import redis
import os
from datetime import datetime, timedelta
import logging
from .auth.cognito import get_user_from_token
import json

# Configure logging
logger = logging.getLogger(__name__)

# Get Redis URL from environment or use default
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Initialize Redis client
try:
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    redis_client.ping()  # Test connection
    logger.info("Successfully connected to Redis")
except redis.ConnectionError as e:
    logger.error(f"Failed to connect to Redis: {e}")
    # Fallback to in-memory storage if Redis is not available
    redis_client = None

# Initialize rate limiter
limiter = Limiter(
    key_func=get_remote_address,
    storage_uri=REDIS_URL if redis_client else "memory://"
)

# Default rate limits (can be adjusted based on needs)
RATE_LIMITS = {
    "default": "100/minute",  # Default limit for all endpoints
    "auth": "5/minute",       # Stricter limit for auth endpoints
    "upload": "50/minute",    # Limit for upload endpoints
    "high_volume": "200/minute",  # Higher limit for endpoints that need it
    "authenticated": "200/minute"  # Higher limit for authenticated users
}

# Penalty system configuration
PENALTY_CONFIG = {
    "violation_threshold": 25,      # Need 25 violations before penalty
    "violation_window": 300,        # Window to count violations (5 minutes)
    "penalty_duration": 1200,       # Penalty duration (20 minutes)
    "penalty_rate_limit": 20,       # Reduced rate limit during penalty
    "cooldown": 30,                # 30 second cooldown between violations
    "requests_after_limit": 10,     # Need 10 requests after rate limit for violation
    "config_version": "2"          # Increment this when changing penalty config
}

def extract_token_from_header(auth_header: str) -> str:
    """
    Extract JWT token from Authorization header.
    
    Args:
        auth_header: Authorization header value
        
    Returns:
        str: JWT token or None
        
    Notes:
        - Handles Bearer token format
        - Returns None if invalid format
    """
    if not auth_header or not auth_header.startswith('Bearer '):
        return None
    return auth_header.split(' ')[1]

def get_rate_limit(request: Request, user_info: dict = None) -> str:
    """
    Get the appropriate rate limit for a request based on the endpoint and auth status.
    
    Args:
        request: FastAPI request object
        user_info: Optional validated user info
        
    Returns:
        str: Rate limit string (e.g. "100/minute")
    """
    path = request.url.path.lower()
    
    # Use authenticated rate limit if valid user
    if user_info:
        logger.info(f"Authenticated request from user: {user_info['user_id']}")
        return RATE_LIMITS["authenticated"]
    
    # Special cases based on endpoint
    if "/auth" in path or "/login" in path:
        return RATE_LIMITS["auth"]
    elif "/upload" in path:
        return RATE_LIMITS["upload"]
    elif any(high_vol in path for high_vol in ["/analytics", "/dashboard"]):
        return RATE_LIMITS["high_volume"]
    
    return RATE_LIMITS["default"]

class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Middleware class for rate limiting requests.
    Inherits from FastAPI's BaseHTTPMiddleware.
    """
    
    async def clear_penalties(self, identifier: str = None):
        """
        Clear all penalty-related data for an identifier or all users.
        
        Args:
            identifier: Optional user ID or IP to clear. If None, clears all.
        """
        if not redis_client:
            return
            
        if identifier:
            # Clear specific user's data
            keys_to_clear = [
                f"violations:{identifier}",
                f"penalty:{identifier}",
                f"cooldown:{identifier}",
                f"post_limit:{identifier}",
                f"rate_limit:*:{identifier}:*",
                f"config_version:{identifier}"
            ]
            for key in keys_to_clear:
                redis_client.delete(key)
        else:
            # Clear all penalty data
            for key in redis_client.scan_iter("violations:*"):
                redis_client.delete(key)
            for key in redis_client.scan_iter("penalty:*"):
                redis_client.delete(key)
            for key in redis_client.scan_iter("cooldown:*"):
                redis_client.delete(key)
            for key in redis_client.scan_iter("post_limit:*"):
                redis_client.delete(key)
            for key in redis_client.scan_iter("config_version:*"):
                redis_client.delete(key)
    
    async def check_config_version(self, identifier: str):
        """
        Check if stored penalties are from current config version.
        Clears old data if config has changed.
        
        Args:
            identifier: User ID or IP address
        """
        if not redis_client:
            return
            
        version_key = f"config_version:{identifier}"
        stored_version = redis_client.get(version_key)
        
        if stored_version != PENALTY_CONFIG["config_version"]:
            # Config changed, clear old data
            await self.clear_penalties(identifier)
            redis_client.set(version_key, PENALTY_CONFIG["config_version"])
    
    async def check_penalty_status(self, identifier: str) -> dict:
        """
        Check if user/IP is under penalty and get violation count.
        
        Args:
            identifier: User ID or IP address
            
        Returns:
            dict: Penalty status including violation count and penalty expiry
        """
        if not redis_client:
            return {"violations": 0, "penalty_until": None}
        
        # Check config version first
        await self.check_config_version(identifier)
            
        violations_key = f"violations:{identifier}"
        penalty_key = f"penalty:{identifier}"
        
        # Get current violations count
        violations = redis_client.get(violations_key)
        violations = int(violations) if violations else 0
        
        # Check if under penalty
        penalty_until = redis_client.get(penalty_key)
        if penalty_until:
            penalty_until = float(penalty_until)
            if datetime.now().timestamp() > penalty_until:
                # Penalty expired, clear it
                redis_client.delete(penalty_key)
                # Also clear violations since penalty expired
                redis_client.delete(violations_key)
                violations = 0
                penalty_until = None
        
        return {
            "violations": violations,
            "penalty_until": penalty_until
        }
    
    async def record_violation(self, identifier: str):
        """
        Record a rate limit violation and apply penalty if threshold reached.
        Only counts as violation if user continues making requests after being rate limited.
        
        Args:
            identifier: User ID or IP address
        """
        if not redis_client:
            return
            
        violations_key = f"violations:{identifier}"
        penalty_key = f"penalty:{identifier}"
        cooldown_key = f"cooldown:{identifier}"
        post_limit_key = f"post_limit:{identifier}"
        
        # Check if in cooldown period
        if redis_client.get(cooldown_key):
            return
            
        # Get/increment post-limit request counter
        post_limit_count = redis_client.incr(post_limit_key)
        redis_client.expire(post_limit_key, 60)  # Reset counter after 1 minute
        
        # Only count as violation if enough post-limit requests
        if post_limit_count >= PENALTY_CONFIG["requests_after_limit"]:
            # Start cooldown period
            redis_client.setex(cooldown_key, PENALTY_CONFIG["cooldown"], "1")
            
            # Reset post-limit counter
            redis_client.delete(post_limit_key)
            
            # Increment violations count
            violations = redis_client.incr(violations_key)
            redis_client.expire(violations_key, PENALTY_CONFIG["violation_window"])
            
            logger.warning(f"Rate limit violation {violations}/{PENALTY_CONFIG['violation_threshold']} for {identifier}")
            
            # Check if we should apply penalty
            if violations >= PENALTY_CONFIG["violation_threshold"]:
                penalty_until = datetime.now().timestamp() + PENALTY_CONFIG["penalty_duration"]
                redis_client.setex(
                    penalty_key,
                    PENALTY_CONFIG["penalty_duration"],
                    str(penalty_until)
                )
                logger.warning(f"Applied penalty to {identifier} until {datetime.fromtimestamp(penalty_until)}")
    
    async def dispatch(self, request: Request, call_next):
        """
        Process each request through rate limiting.
        
        Args:
            request: FastAPI request object
            call_next: Next middleware/handler in chain
            
        Returns:
            Response: FastAPI response
            
        Raises:
            HTTPException: When rate limit is exceeded
        """
        try:
            # Get client IP
            client_ip = get_remote_address(request)
            
            # Get user info from token if available
            user_info = None
            auth_header = request.headers.get('Authorization')
            if auth_header:
                try:
                    token = extract_token_from_header(auth_header)
                    if token:
                        user_info = get_user_from_token(token)
                except Exception as e:
                    logger.warning(f"Failed to validate token: {str(e)}")
                    # Continue with unauthenticated rate limit
            
            # Use user_id or IP as identifier
            identifier = user_info['user_id'] if user_info else client_ip
            
            # Check penalty status
            penalty_status = await self.check_penalty_status(identifier)
            if penalty_status["penalty_until"]:
                penalty_expiry = datetime.fromtimestamp(penalty_status["penalty_until"])
                logger.warning(f"Request from penalized {identifier}, penalty until: {penalty_expiry}")
                return JSONResponse(
                    status_code=429,
                    content={
                        "detail": f"Rate limit exceeded. Try again in {PENALTY_CONFIG['cooldown']} seconds.",
                        "retry_after": PENALTY_CONFIG['cooldown']
                    }
                )
            
            # Get appropriate rate limit
            rate_limit = get_rate_limit(request, user_info)
            limit = int(rate_limit.split("/")[0])
            
            # If under penalty but penalty expired, use reduced limit
            if penalty_status["violations"] >= PENALTY_CONFIG["violation_threshold"]:
                limit = PENALTY_CONFIG["penalty_rate_limit"]
            
            # Check rate limit
            if redis_client:
                key = f"rate_limit:{identifier}:{request.url.path}"
                
                # Get current count atomically and increment
                current = redis_client.get(key)
                
                if current is None:
                    # First request in window
                    redis_client.setex(key, 60, 1)  # 1 minute expiry
                    current_count = 1
                else:
                    current_count = int(current)
                    if current_count >= limit:
                        # Only record violation for requests after hitting limit
                        await self.record_violation(identifier)
                        
                        logger.warning(
                            f"Rate limit exceeded for {identifier} "
                            f"on {request.url.path}"
                        )
                        return JSONResponse(
                            status_code=429,
                            content={
                                "detail": "Too many requests. Please try again later.",
                                "retry_after": 60
                            }
                        )
                    
                    # Increment counter
                    current_count = redis_client.incr(key)
                
                # Log request details with proper count
                logger.info(
                    f"Request from {identifier}: "
                    f"{current_count}/{limit} requests"
                )
            
            # Process the request
            response = await call_next(request)
            return response
            
        except Exception as e:
            logger.error(f"Error in rate limit middleware: {str(e)}")
            # Return 429 for rate limit errors, 500 for other errors
            if isinstance(e, RateLimitExceeded) or str(e).startswith("429"):
                return JSONResponse(
                    status_code=429,
                    content={
                        "detail": "Too many requests. Please try again later.",
                        "retry_after": 60
                    }
                )
            return JSONResponse(
                status_code=500,
                content={"detail": "Internal server error"}
            ) 