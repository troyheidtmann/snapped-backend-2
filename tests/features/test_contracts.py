"""
Test Contract Management System

This module tests the contract management functionality including:
- Contract creation and versioning
- Digital signing workflow
- PDF generation
- Email distribution
- Status tracking
"""

import pytest
from fastapi.testclient import TestClient
from datetime import datetime, timezone
from app.features.contracts.routes_contracts import router
from app.shared.database import client_info, contracts_collection
import base64
from pathlib import Path
import json

# Test data
TEST_CONTRACT = {
    "client_id": "test_client",
    "contract_type": "standard",
    "terms": {
        "revenue_share": 80,
        "term_length": 12,
        "platform": "snapchat",
        "minimum_guarantee": 1000
    },
    "client_name": "Test Creator",
    "client_email": "test@creator.com",
    "representative": "test_rep"
}

@pytest.fixture
async def test_contract_setup(test_db):
    """Set up test contract data"""
    # Insert test client first
    client_data = {
        "client_ID": TEST_CONTRACT["client_id"],
        "name": TEST_CONTRACT["client_name"],
        "email": TEST_CONTRACT["client_email"],
        "status": "active"
    }
    await client_info.insert_one(client_data)
    
    # Insert test contract
    contract_data = {**TEST_CONTRACT, "status": "draft", "created_at": datetime.now()}
    result = await contracts_collection.insert_one(contract_data)
    contract_id = str(result.inserted_id)
    
    yield contract_id
    
    # Cleanup
    await client_info.delete_many({"client_ID": TEST_CONTRACT["client_id"]})
    await contracts_collection.delete_many({"_id": result.inserted_id})

@pytest.mark.asyncio
async def test_create_contract(test_client, test_db):
    """Test contract creation"""
    response = test_client.post(
        "/api/contracts/create",
        json=TEST_CONTRACT
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "contract_id" in data
    
    # Verify in database
    contract = await contracts_collection.find_one({"client_id": TEST_CONTRACT["client_id"]})
    assert contract is not None
    assert contract["status"] == "draft"
    assert contract["terms"]["revenue_share"] == TEST_CONTRACT["terms"]["revenue_share"]

@pytest.mark.asyncio
async def test_generate_pdf(test_client, test_db, test_contract_setup):
    """Test PDF generation"""
    contract_id = test_contract_setup
    
    response = test_client.get(f"/api/contracts/{contract_id}/pdf")
    
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/pdf"
    
    # Verify PDF content
    pdf_content = response.content
    assert len(pdf_content) > 0
    assert pdf_content.startswith(b"%PDF")  # PDF magic number

@pytest.mark.asyncio
async def test_client_signature(test_client, test_db, test_contract_setup):
    """Test client signature process"""
    contract_id = test_contract_setup
    signature_data = {
        "name": TEST_CONTRACT["client_name"],
        "date": datetime.now().isoformat(),
        "ip_address": "127.0.0.1",
        "signature": base64.b64encode(b"test signature").decode()
    }
    
    response = test_client.post(
        f"/api/contracts/{contract_id}/sign/client",
        json=signature_data
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    
    # Verify in database
    contract = await contracts_collection.find_one({"_id": contract_id})
    assert contract["status"] == "client_signed"
    assert contract["client_signature"]["name"] == signature_data["name"]
    assert contract["client_signature"]["ip_address"] == signature_data["ip_address"]

@pytest.mark.asyncio
async def test_representative_signature(test_client, test_db, test_contract_setup):
    """Test representative signature process"""
    contract_id = test_contract_setup
    
    # First get client signature
    await test_client_signature(test_client, test_db, contract_id)
    
    # Now rep signature
    signature_data = {
        "name": "Test Representative",
        "date": datetime.now().isoformat(),
        "ip_address": "127.0.0.1",
        "signature": base64.b64encode(b"rep signature").decode()
    }
    
    response = test_client.post(
        f"/api/contracts/{contract_id}/sign/representative",
        json=signature_data
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    
    # Verify in database
    contract = await contracts_collection.find_one({"_id": contract_id})
    assert contract["status"] == "fully_executed"
    assert contract["representative_signature"]["name"] == signature_data["name"]

@pytest.mark.asyncio
async def test_email_distribution(test_client, test_db, test_contract_setup):
    """Test contract email distribution"""
    contract_id = test_contract_setup
    
    response = test_client.post(
        f"/api/contracts/{contract_id}/send",
        json={"email": TEST_CONTRACT["client_email"]}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert "email_sent" in data
    
    # Verify contract status update
    contract = await contracts_collection.find_one({"_id": contract_id})
    assert contract["status"] == "sent"
    assert "sent_at" in contract

@pytest.mark.asyncio
async def test_contract_versioning(test_client, test_db, test_contract_setup):
    """Test contract versioning"""
    contract_id = test_contract_setup
    
    # Update contract terms
    update_data = {
        "terms": {
            **TEST_CONTRACT["terms"],
            "revenue_share": 85  # Changed from 80
        }
    }
    
    response = test_client.put(
        f"/api/contracts/{contract_id}",
        json=update_data
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    
    # Verify version tracking
    contract = await contracts_collection.find_one({"_id": contract_id})
    assert contract["version"] == 2
    assert contract["terms"]["revenue_share"] == 85
    assert "version_history" in contract
    assert len(contract["version_history"]) == 1
    assert contract["version_history"][0]["terms"]["revenue_share"] == 80

@pytest.mark.asyncio
async def test_contract_search(test_client, test_db, test_contract_setup):
    """Test contract search functionality"""
    # Test search by client
    response = test_client.get(
        "/api/contracts/search",
        params={"client_name": TEST_CONTRACT["client_name"]}
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) > 0
    assert data["results"][0]["client_name"] == TEST_CONTRACT["client_name"]
    
    # Test search by status
    response = test_client.get(
        "/api/contracts/search",
        params={"status": "draft"}
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) > 0
    assert data["results"][0]["status"] == "draft"

@pytest.mark.asyncio
async def test_error_handling(test_client, test_db):
    """Test error handling scenarios"""
    # Test invalid contract ID
    response = test_client.get("/api/contracts/invalid_id")
    assert response.status_code == 404
    
    # Test invalid signature (wrong name)
    contract_id = await test_contract_setup(test_db)
    signature_data = {
        "name": "Wrong Name",  # Doesn't match client name
        "date": datetime.now().isoformat(),
        "ip_address": "127.0.0.1",
        "signature": base64.b64encode(b"test signature").decode()
    }
    response = test_client.post(
        f"/api/contracts/{contract_id}/sign/client",
        json=signature_data
    )
    assert response.status_code == 400
    
    # Test invalid contract update
    response = test_client.put(
        f"/api/contracts/{contract_id}",
        json={"invalid_field": "value"}
    )
    assert response.status_code == 400 