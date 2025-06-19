"""
Security Middleware Module

This module provides security middleware for adding HTTP security headers
to protect against common web vulnerabilities.

Features:
- HSTS (HTTP Strict Transport Security)
- XSS Protection
- Content Security Policy
- Frame Options
- Content Type Options
- Referrer Policy
"""

from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
import logging

logger = logging.getLogger(__name__)

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """
    Middleware for adding security headers to all responses.
    Implements various security headers to protect against common vulnerabilities.
    """
    
    async def dispatch(self, request: Request, call_next):
        """
        Add security headers to response.
        
        Args:
            request: The incoming request
            call_next: The next middleware/handler
            
        Returns:
            Response with security headers
        """
        response = await call_next(request)
        
        # HSTS: Force HTTPS
        response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
        
        # XSS Protection
        response.headers['X-XSS-Protection'] = '1; mode=block'
        
        # Content Security Policy
        csp = [
            "default-src 'self'",
            "img-src 'self' data: https: http:",  # Allow images from HTTPS/HTTP sources
            "script-src 'self' 'unsafe-inline' 'unsafe-eval'",  # Required for some UI frameworks
            "style-src 'self' 'unsafe-inline'",
            "font-src 'self' data: https:",
            f"connect-src 'self' https://*.amazonaws.com https://*.snapped.cc https://*.b-cdn.net https://*.cloudfront.net",
            "frame-ancestors 'none'",  # Prevent clickjacking
            "base-uri 'self'",
            "form-action 'self'"
        ]
        response.headers['Content-Security-Policy'] = '; '.join(csp)
        
        # Prevent MIME type sniffing
        response.headers['X-Content-Type-Options'] = 'nosniff'
        
        # Referrer Policy
        response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
        
        # Frame Options (prevent clickjacking)
        response.headers['X-Frame-Options'] = 'DENY'
        
        # Permissions Policy (formerly Feature-Policy)
        permissions = [
            'accelerometer=()',
            'camera=()',
            'geolocation=()',
            'gyroscope=()',
            'magnetometer=()',
            'microphone=()',
            'payment=()',
            'usb=()'
        ]
        response.headers['Permissions-Policy'] = ', '.join(permissions)
        
        return response 