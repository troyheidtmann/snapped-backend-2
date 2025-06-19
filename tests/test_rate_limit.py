"""
Test Rate Limiting Module

This module tests the rate limiting functionality including:
- Basic rate limiting
- Penalty system
- Different rate limits for different endpoints
- Authentication handling
"""

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
import time
from datetime import datetime, timedelta

from app.shared.rate_limit import (
    RateLimitMiddleware,
    get_rate_limit,
    extract_token_from_header
)

# Test app
app = FastAPI()
app.add_middleware(RateLimitMiddleware)

@app.get("/test")
async def test_endpoint():
    return {"status": "success"}

@app.get("/auth/test")
async def auth_test_endpoint():
    return {"status": "success"}

@pytest.fixture
def client():
    return TestClient(app)

def test_extract_token_from_header():
    """Test token extraction from Authorization header"""
    # Test valid bearer token
    valid_header = "Bearer abc123"
    assert extract_token_from_header(valid_header) == "abc123"
    
    # Test invalid formats
    assert extract_token_from_header("abc123") is None
    assert extract_token_from_header("bearer abc123") is None
    assert extract_token_from_header(None) is None
    assert extract_token_from_header("") is None

def test_get_rate_limit(mock_auth):
    """Test rate limit selection based on endpoint and auth status"""
    # Test authenticated endpoint
    auth_request = Request({"type": "http", "path": "/test", "headers": []})
    auth_limit = get_rate_limit(auth_request, mock_auth)
    assert auth_limit == "200/minute"  # Authenticated rate limit
    
    # Test auth endpoint
    auth_path_request = Request({"type": "http", "path": "/auth/login", "headers": []})
    auth_path_limit = get_rate_limit(auth_path_request)
    assert auth_path_limit == "5/minute"  # Auth endpoint limit
    
    # Test upload endpoint
    upload_request = Request({"type": "http", "path": "/upload", "headers": []})
    upload_limit = get_rate_limit(upload_request)
    assert upload_limit == "50/minute"  # Upload endpoint limit
    
    # Test default endpoint
    default_request = Request({"type": "http", "path": "/api/other", "headers": []})
    default_limit = get_rate_limit(default_request)
    assert default_limit == "100/minute"  # Default limit

@pytest.mark.asyncio
async def test_basic_rate_limiting(client, mock_redis):
    """Test basic rate limiting functionality"""
    # Make requests up to limit
    for _ in range(100):  # Default limit is 100/minute
        response = client.get("/test")
        assert response.status_code == 200
    
    # Next request should be rate limited
    response = client.get("/test")
    assert response.status_code == 429
    assert "retry_after" in response.json()

@pytest.mark.asyncio
async def test_penalty_system(client, mock_redis):
    """Test penalty system for repeated violations"""
    # Trigger violations
    for _ in range(PENALTY_CONFIG["violation_threshold"] + 1):
        # Make requests past limit
        for _ in range(110):  # 10 over limit
            client.get("/test")
        time.sleep(1)  # Wait for cooldown
    
    # Should now be under penalty
    response = client.get("/test")
    assert response.status_code == 429
    assert "penalty_until" in response.json()

@pytest.mark.asyncio
async def test_auth_endpoint_limits(client, mock_redis):
    """Test stricter limits on auth endpoints"""
    # Make requests up to auth endpoint limit
    for _ in range(5):  # Auth limit is 5/minute
        response = client.get("/auth/test")
        assert response.status_code == 200
    
    # Next request should be rate limited
    response = client.get("/auth/test")
    assert response.status_code == 429

@pytest.mark.asyncio
async def test_authenticated_limits(client, mock_redis, mock_auth):
    """Test higher limits for authenticated users"""
    headers = {"Authorization": f"Bearer {mock_auth['token']}"}
    
    # Make requests up to authenticated limit
    for _ in range(200):  # Authenticated limit is 200/minute
        response = client.get("/test", headers=headers)
        assert response.status_code == 200
    
    # Next request should be rate limited
    response = client.get("/test", headers=headers)
    assert response.status_code == 429 