"""
Test Lead Management System

This module tests the lead management functionality including:
- Lead creation and updates
- Analytics tracking
- Employee assignments
- Status management
- Algorithm scoring
"""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timezone
from app.features.lead.route_lead import router
from app.shared.database import (
    client_info,
    async_client,
    partners_collection,
    monetized_by_collection,
    referred_by_collection,
    client_payouts
)
from bson import ObjectId

# Test data
TEST_LEAD = {
    "name": "Test Creator",
    "email": "test@creator.com",
    "phone": "1234567890",
    "stage_name": "TestCreator",
    "dob": "2000-01-01",
    "timezone": "America/New_York",
    "instagram": {
        "username": "testcreator",
        "followers": 100000,
        "verified": False
    },
    "tiktok": {
        "username": "testcreator",
        "followers": 200000,
        "verified": False
    },
    "youtube": {
        "username": "TestCreator",
        "subscribers": 50000,
        "verified": False
    },
    "snapchat": {
        "username": "testcreator",
        "followers": 150000,
        "star_status": False
    }
}

@pytest.fixture
async def test_lead_setup(test_db):
    """Set up test lead data"""
    # Insert test lead
    result = await client_info.insert_one(TEST_LEAD)
    lead_id = str(result.inserted_id)
    
    yield lead_id
    
    # Cleanup
    await client_info.delete_many({"_id": ObjectId(lead_id)})
    await monetized_by_collection.delete_many({"lead_id": lead_id})
    await referred_by_collection.delete_many({"lead_id": lead_id})

@pytest.mark.asyncio
async def test_create_lead(test_client, test_db):
    """Test lead creation"""
    response = test_client.post(
        "/api/leads/create",
        json=TEST_LEAD
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "lead_id" in data
    
    # Verify in database
    lead = await client_info.find_one({"_id": ObjectId(data["lead_id"])})
    assert lead is not None
    assert lead["email"] == TEST_LEAD["email"]

@pytest.mark.asyncio
async def test_update_lead(test_client, test_db, test_lead_setup):
    """Test lead update"""
    lead_id = test_lead_setup
    update_data = {
        "instagram": {
            "followers": 150000,
            "verified": True
        }
    }
    
    response = test_client.put(
        f"/api/leads/{lead_id}",
        json=update_data
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    
    # Verify update in database
    lead = await client_info.find_one({"_id": ObjectId(lead_id)})
    assert lead["instagram"]["followers"] == 150000
    assert lead["instagram"]["verified"] is True

@pytest.mark.asyncio
async def test_lead_analytics(test_client, test_db, test_lead_setup):
    """Test lead analytics calculation"""
    lead_id = test_lead_setup
    
    response = test_client.get(f"/api/leads/{lead_id}/analytics")
    
    assert response.status_code == 200
    data = response.json()
    assert "total_followers" in data
    assert "platform_breakdown" in data
    assert "engagement_metrics" in data
    
    # Verify calculations
    total_followers = (
        TEST_LEAD["instagram"]["followers"] +
        TEST_LEAD["tiktok"]["followers"] +
        TEST_LEAD["youtube"]["subscribers"] +
        TEST_LEAD["snapchat"]["followers"]
    )
    assert data["total_followers"] == total_followers

@pytest.mark.asyncio
async def test_assign_employee(test_client, test_db, test_lead_setup):
    """Test employee assignment to lead"""
    lead_id = test_lead_setup
    assignment_data = {
        "employee_id": "test_employee",
        "role": "account_manager"
    }
    
    response = test_client.post(
        f"/api/leads/{lead_id}/assign",
        json=assignment_data
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    
    # Verify assignment in database
    lead = await client_info.find_one({"_id": ObjectId(lead_id)})
    assert lead["assigned_to"] == assignment_data["employee_id"]
    assert lead["assignment_role"] == assignment_data["role"]

@pytest.mark.asyncio
async def test_monetization_status(test_client, test_db, test_lead_setup):
    """Test monetization status updates"""
    lead_id = test_lead_setup
    monetization_data = {
        "status": "monetized",
        "platform": "snapchat",
        "date": datetime.now().isoformat(),
        "revenue": 5000
    }
    
    response = test_client.post(
        f"/api/leads/{lead_id}/monetization",
        json=monetization_data
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    
    # Verify monetization in database
    monetization = await monetized_by_collection.find_one({"lead_id": lead_id})
    assert monetization is not None
    assert monetization["platform"] == "snapchat"
    assert monetization["revenue"] == 5000

@pytest.mark.asyncio
async def test_algorithm_score(test_client, test_db, test_lead_setup):
    """Test algorithm score calculation"""
    lead_id = test_lead_setup
    
    response = test_client.get(f"/api/leads/{lead_id}/score")
    
    assert response.status_code == 200
    data = response.json()
    assert "total_score" in data
    assert "breakdown" in data
    
    # Verify score components
    breakdown = data["breakdown"]
    assert "follower_score" in breakdown
    assert "engagement_score" in breakdown
    assert "platform_score" in breakdown
    assert "verification_score" in breakdown

@pytest.mark.asyncio
async def test_lead_search(test_client, test_db, test_lead_setup):
    """Test lead search functionality"""
    # Test search by name
    response = test_client.get("/api/leads/search", params={"name": "Test Creator"})
    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) > 0
    assert data["results"][0]["name"] == TEST_LEAD["name"]
    
    # Test search by platform metrics
    response = test_client.get(
        "/api/leads/search",
        params={"min_followers": 100000, "platform": "instagram"}
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) > 0
    
    # Test search by monetization status
    response = test_client.get(
        "/api/leads/search",
        params={"monetized": True, "platform": "snapchat"}
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) > 0

@pytest.mark.asyncio
async def test_error_handling(test_client, test_db):
    """Test error handling scenarios"""
    # Test invalid lead ID
    response = test_client.get("/api/leads/invalid_id")
    assert response.status_code == 404
    
    # Test invalid update data
    response = test_client.put(
        "/api/leads/some_id",
        json={"invalid_field": "value"}
    )
    assert response.status_code == 400
    
    # Test duplicate email
    await test_create_lead(test_client, test_db)  # Create first lead
    response = test_client.post(
        "/api/leads/create",
        json=TEST_LEAD  # Try to create duplicate
    )
    assert response.status_code == 409  # Conflict 