"""
Pytest Configuration File

This module provides fixtures and configuration for all tests.
"""

import pytest
import os
import sys
from fastapi.testclient import TestClient
from datetime import datetime, timedelta
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import certifi
from typing import Dict, Any

# Add app directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import app modules
from app.main import app
from app.shared.database import async_client

@pytest.fixture
def test_client():
    """Fixture for FastAPI test client"""
    return TestClient(app)

@pytest.fixture
async def test_db():
    """Fixture for test database connection"""
    try:
        # Use test database
        test_client = AsyncIOMotorClient(
            os.getenv("MONGODB_TEST_URL", "mongodb://localhost:27017"),
            tlsCAFile=certifi.where(),
            tls=True
        )
        await test_client.admin.command('ping')
        return test_client
    except Exception as e:
        pytest.fail(f"Failed to connect to test database: {str(e)}")

@pytest.fixture
def mock_redis():
    """Fixture for mocking Redis"""
    class MockRedis:
        def __init__(self):
            self.data = {}
            
        async def get(self, key):
            return self.data.get(key)
            
        async def set(self, key, value, ex=None):
            self.data[key] = value
            
        async def delete(self, key):
            if key in self.data:
                del self.data[key]
                
        async def incr(self, key):
            if key not in self.data:
                self.data[key] = 1
            else:
                self.data[key] = int(self.data[key]) + 1
            return self.data[key]
    
    return MockRedis()

@pytest.fixture
def mock_auth():
    """Fixture for mocking authentication"""
    return {
        "user_id": "test_user",
        "email": "test@example.com",
        "roles": ["admin"]
    }

@pytest.fixture
def sample_data():
    """Fixture providing sample test data"""
    return {
        "users": [
            {
                "id": "user1",
                "name": "Test User 1",
                "email": "user1@test.com"
            },
            {
                "id": "user2", 
                "name": "Test User 2",
                "email": "user2@test.com"
            }
        ],
        "content": [
            {
                "id": "content1",
                "title": "Test Content 1",
                "user_id": "user1"
            },
            {
                "id": "content2",
                "title": "Test Content 2", 
                "user_id": "user2"
            }
        ]
    }

@pytest.fixture(autouse=True)
def setup_test_env():
    """Automatically set up test environment variables"""
    os.environ["TESTING"] = "true"
    os.environ["ENVIRONMENT"] = "test"
    yield
    os.environ.pop("TESTING", None)
    os.environ.pop("ENVIRONMENT", None) 