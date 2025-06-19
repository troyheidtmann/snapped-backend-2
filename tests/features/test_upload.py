"""
Test Upload System

This module tests the upload functionality including:
- Session initialization
- File uploads
- Content management
- Spotlight integration
- Content dump processing
"""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timezone
import json
from app.features.uploadapp.routes_uploadapp import router, SessionData
from app.shared.database import upload_collection, client_info, spotlight_collection
import os
from pathlib import Path

# Test data
TEST_CLIENT_ID = "test_client"
TEST_SESSION = {
    "client_ID": TEST_CLIENT_ID,
    "snap_ID": "test_snap",
    "timezone": "America/New_York",
    "date": datetime.now().strftime("%Y-%m-%d"),
    "content_type": "STORIES",
    "folder_id": "test_folder",
    "folder_path": "/test/path",
    "total_files": 2
}

@pytest.fixture
def test_image():
    """Create a test image file"""
    image_path = Path("tests/data/test_image.jpg")
    image_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create a small test image
    with open(image_path, "wb") as f:
        f.write(b"fake image data")
    
    yield image_path
    
    # Cleanup
    if image_path.exists():
        image_path.unlink()

@pytest.fixture
async def test_client_setup(test_db):
    """Set up test client data"""
    client_data = {
        "client_ID": TEST_CLIENT_ID,
        "name": "Test Client",
        "email": "test@example.com",
        "status": "active"
    }
    await client_info.insert_one(client_data)
    yield
    await client_info.delete_many({"client_ID": TEST_CLIENT_ID})

@pytest.mark.asyncio
async def test_init_session(test_client, test_db, test_client_setup):
    """Test session initialization"""
    response = test_client.post(
        "/api/uploadapp/init-session",
        json=TEST_SESSION
    )
    assert response.status_code == 200
    data = response.json()
    assert data["client_ID"] == TEST_CLIENT_ID
    assert data["status"] == "success"
    assert "session_id" in data

    # Verify session in database
    session = await upload_collection.find_one({"client_ID": TEST_CLIENT_ID})
    assert session is not None
    assert session["sessions"][-1]["content_type"] == "STORIES"

@pytest.mark.asyncio
async def test_upload_file(test_client, test_db, test_client_setup, test_image):
    """Test file upload functionality"""
    # First init session
    session_response = test_client.post(
        "/api/uploadapp/init-session",
        json=TEST_SESSION
    )
    session_data = session_response.json()
    session_id = session_data["session_id"]
    
    # Now upload file
    with open(test_image, "rb") as f:
        files = {"file": ("test_image.jpg", f, "image/jpeg")}
        response = test_client.post(
            f"/api/uploadapp/upload/{TEST_CLIENT_ID}/{session_id}",
            files=files
        )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "file_id" in data
    
    # Verify file in database
    session = await upload_collection.find_one(
        {"client_ID": TEST_CLIENT_ID, "sessions.session_id": session_id}
    )
    assert session is not None
    assert len(session["sessions"][-1]["files"]) == 1
    assert session["sessions"][-1]["files"][0]["file_name"] == "test_image.jpg"

@pytest.mark.asyncio
async def test_content_notes(test_client, test_db, test_client_setup):
    """Test content notes functionality"""
    # First create a session with a file
    session_response = test_client.post(
        "/api/uploadapp/init-session",
        json=TEST_SESSION
    )
    session_id = session_response.json()["session_id"]
    
    # Add content notes
    notes_data = {
        "client_ID": TEST_CLIENT_ID,
        "session_id": session_id,
        "notes": "Test content notes",
        "editor": "test_editor"
    }
    response = test_client.post(
        "/api/uploadapp/content-notes",
        json=notes_data
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    
    # Verify notes in database
    session = await upload_collection.find_one(
        {"client_ID": TEST_CLIENT_ID, "sessions.session_id": session_id}
    )
    assert session is not None
    assert session["sessions"][-1]["notes"] == "Test content notes"

@pytest.mark.asyncio
async def test_spotlight_upload(test_client, test_db, test_client_setup, test_image):
    """Test spotlight content upload"""
    # Init spotlight session
    spotlight_session = TEST_SESSION.copy()
    spotlight_session["content_type"] = "SPOTLIGHT"
    
    session_response = test_client.post(
        "/api/uploadapp/init-session",
        json=spotlight_session
    )
    session_id = session_response.json()["session_id"]
    
    # Upload spotlight file
    with open(test_image, "rb") as f:
        files = {"file": ("spotlight.jpg", f, "image/jpeg")}
        response = test_client.post(
            f"/api/uploadapp/upload/{TEST_CLIENT_ID}/{session_id}",
            files=files,
            params={"type": "spotlight"}
        )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    
    # Verify in spotlight collection
    spotlight = await spotlight_collection.find_one(
        {"client_ID": TEST_CLIENT_ID}
    )
    assert spotlight is not None
    assert len(spotlight["files"]) == 1
    assert spotlight["files"][0]["file_name"] == "spotlight.jpg"

@pytest.mark.asyncio
async def test_delete_content(test_client, test_db, test_client_setup):
    """Test content deletion"""
    # First create a session
    session_response = test_client.post(
        "/api/uploadapp/init-session",
        json=TEST_SESSION
    )
    session_id = session_response.json()["session_id"]
    
    # Delete the session
    response = test_client.delete(
        f"/api/uploadapp/delete-content/{TEST_CLIENT_ID}/{session_id}"
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    
    # Verify deletion in database
    session = await upload_collection.find_one(
        {"client_ID": TEST_CLIENT_ID, "sessions.session_id": session_id}
    )
    assert session is None

@pytest.mark.asyncio
async def test_error_handling(test_client, test_db):
    """Test error handling scenarios"""
    # Test invalid client ID
    response = test_client.post(
        "/api/uploadapp/init-session",
        json={**TEST_SESSION, "client_ID": "invalid_client"}
    )
    assert response.status_code == 404
    
    # Test invalid session ID
    response = test_client.post(
        "/api/uploadapp/content-notes",
        json={
            "client_ID": TEST_CLIENT_ID,
            "session_id": "invalid_session",
            "notes": "Test notes",
            "editor": "test_editor"
        }
    )
    assert response.status_code == 404
    
    # Test invalid file upload
    response = test_client.post(
        f"/api/uploadapp/upload/{TEST_CLIENT_ID}/invalid_session",
        files={"file": ("test.txt", b"invalid data", "text/plain")}
    )
    assert response.status_code == 404 