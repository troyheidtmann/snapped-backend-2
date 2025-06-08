"""
Contract Management API - Digital Contract Lifecycle Management

This module provides FastAPI routes for managing the complete lifecycle of digital contracts
in the Snapped platform, including creation, signing, distribution, and storage.

Features:
--------
1. Contract Management:
   - Draft creation and versioning
   - Digital signing (client and representative)
   - PDF generation
   - Email distribution
   - Version history

2. Signature Workflow:
   - Client signature collection
   - Representative countersigning
   - IP address tracking
   - Timestamp recording
   - Name verification

3. Document Generation:
   - Dynamic PDF creation
   - Signature embedding
   - Automatic formatting
   - Page management

4. Email Integration:
   - Automated notifications
   - PDF attachments
   - Signing link generation
   - Copy distribution

Security:
--------
- Digital signature validation
- Name matching verification
- IP address tracking
- Access control
- Audit trail

Contract States:
--------------
- DRAFT: Initial creation
- SENT: Awaiting client signature
- CLIENT_SIGNED: Client has signed
- REP_SIGNED: Representative has signed
- FULLY_EXECUTED: Both parties have signed

Dependencies:
-----------
- FastAPI: Web framework
- ReportLab: PDF generation
- MongoDB: Document storage
- SMTP: Email delivery
- Base64: Data encoding

Author: Snapped Development Team
"""

from fastapi import APIRouter, HTTPException, Depends
from app.shared.database import client_info, contracts_collection
from app.shared.auth import get_filtered_query
from datetime import datetime
from bson import ObjectId
from reportlab.pdfgen import canvas
from io import BytesIO
import base64
from fastapi.responses import StreamingResponse, FileResponse
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from app.shared.config import (
    SMTP_SERVER,
    SMTP_PORT,
    SMTP_USERNAME,
    SMTP_PASSWORD,
    FROM_EMAIL
)
import asyncio
from reportlab.lib.pagesizes import letter
from textwrap import wrap

router = APIRouter(
    prefix="/api/contracts",
    tags=["contracts"]
)

# Contract statuses
CONTRACT_STATUS = {
    'DRAFT': 'draft',
    'SENT': 'sent_for_signature',
    'CLIENT_SIGNED': 'client_signed',
    'REP_SIGNED': 'rep_signed',
    'FULLY_EXECUTED': 'fully_executed'
}

async def send_contract_email(to_email: str, subject: str, body: str, pdf_buffer: BytesIO):
    """Send contract-related emails with PDF attachments."""
    try:
        print(f"Attempting to send email to: {to_email}")  # Debug log
        msg = MIMEMultipart()
        msg['From'] = FROM_EMAIL
        msg['To'] = to_email
        msg['Subject'] = subject
        msg['Reply-To'] = FROM_EMAIL
        msg.attach(MIMEText(body, 'plain'))

        print(f"Created email message with From: {FROM_EMAIL}, To: {to_email}")  # Debug log

        # Attach PDF
        pdf_attachment = MIMEApplication(pdf_buffer.getvalue())
        pdf_attachment.add_header('Content-Disposition', 'attachment', filename='contract.pdf')
        msg.attach(pdf_attachment)

        print("Attached PDF")  # Debug log

        # Send email - use asyncio to prevent blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: send_email_sync(msg))
        
        print("Email sent successfully")  # Debug log
        return True
    except Exception as e:
        print(f"Failed to send email: {str(e)}")  # Detailed error log
        print(f"Email details - To: {to_email}, Subject: {subject}, From: {FROM_EMAIL}")  # More context
        return False

def send_email_sync(msg):
    """Synchronous email sending function for use with asyncio."""
    try:
        print("Connecting to SMTP server...")  # Debug log
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            print("Starting TLS...")  # Debug log
            server.starttls()
            print(f"Logging in as {SMTP_USERNAME}")  # Debug log
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            print(f"Sending message to {msg['To']} from {msg['From']}")  # Debug log
            
            # Add message ID header for tracking
            msg['Message-ID'] = f"<contract-{datetime.now().strftime('%Y%m%d%H%M%S')}@snapped.cc>"
            
            server.send_message(msg)
            print(f"Message sent! Message-ID: {msg['Message-ID']}")  # Debug log
    except Exception as e:
        print(f"SMTP Error: {str(e)}")  # Detailed SMTP error
        print(f"Full message headers: {dict(msg.items())}")  # Print all headers
        raise  # Re-raise to be caught by the calling function

@router.post("/save_draft")
async def save_contract_draft(data: dict):
    """Save or update a contract draft with version tracking."""
    try:
        # Extract client_id - handle both string and dict cases
        client_id = data["client_id"]
        if isinstance(client_id, dict):
            # If client_id is a dict, try to get the _id or id field
            client_id = client_id.get('_id') or client_id.get('id')
        
        # Convert to ObjectId if it's a valid ObjectId string
        try:
            lookup_id = ObjectId(client_id)
        except:
            # If conversion fails, use the client_id as is
            lookup_id = client_id

        # Find client data
        client_data = await client_info.find_one({"$or": [
            {"_id": lookup_id} if isinstance(lookup_id, ObjectId) else {"client_id": lookup_id},
            {"client_id": lookup_id}
        ]})

        if not client_data:
            raise HTTPException(status_code=404, detail="Client not found")

        # Get or generate client_id
        actual_client_id = client_data.get("client_id") or f"th{client_data.get('DOB', '').replace('-', '')}"
        current_time = datetime.utcnow()

        # Get current versions to determine next version number
        current_contract = await contracts_collection.find_one({"client_id": actual_client_id})
        next_version = 1
        if current_contract and 'versions' in current_contract:
            next_version = len(current_contract['versions']) + 1

        # Create new version entry with version number
        new_version = {
            "content": data["content"],
            "timestamp": current_time,
            "edited_by": "user",
            "version": next_version
        }

        # Update contract document
        result = await contracts_collection.update_one(
            {"client_id": actual_client_id},
            {
                "$set": {
                    "client_id": actual_client_id,
                    "client_info": {
                        "first_name": client_data.get("First_Legal_Name"),
                        "last_name": client_data.get("Last_Legal_Name"),
                        "email": client_data.get("Email_Address"),
                        "phone": client_data.get("Phone_Number"),
                        "client_id": actual_client_id
                    },
                    "status": CONTRACT_STATUS['DRAFT'],
                    "current_content": data["content"],
                    "updated_at": current_time,
                },
                "$push": {
                    "versions": new_version
                },
                "$setOnInsert": {
                    "created_at": current_time
                }
            },
            upsert=True
        )

        saved_contract = await contracts_collection.find_one({"client_id": actual_client_id})
        
        return {
            "status": "success",
            "message": "Contract draft saved",
            "contract_id": str(saved_contract["_id"]),
            "client_id": actual_client_id,
            "version_count": len(saved_contract.get("versions", []))
        }

    except Exception as e:
        print(f"Error saving draft: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to save draft: {str(e)}")

@router.post("/sign")
async def sign_contract(data: dict):
    """Process client signature for a contract."""
    try:
        print(f"Received signing data: {data}")  # Debug log
        
        # Get the contract - using client_id from the URL
        client_id = data.get('client_id')
        if not client_id:
            raise HTTPException(status_code=400, detail="Missing client_id")

        # Try different ways to find the contract
        contract = None
        
        # First try direct client_id lookup
        contract = await contracts_collection.find_one({"client_id": client_id})
        
        # If not found, try looking up by _id
        if not contract:
            try:
                contract = await contracts_collection.find_one({"_id": ObjectId(client_id)})
            except:
                pass

        if not contract:
            raise HTTPException(status_code=404, detail=f"Contract not found for client_id: {client_id}")

        # Verify the typed name matches the legal name
        typed_name = data.get('name', '').lower().strip()
        legal_name = f"{contract['client_info']['first_name']} {contract['client_info']['last_name']}".lower().strip()
        
        if typed_name != legal_name:
            raise HTTPException(
                status_code=400, 
                detail=f"Typed name '{typed_name}' does not match legal name '{legal_name}'"
            )

        # Create signature data
        signature_data = {
            "signature": data['signature'],
            "typed_name": data['name'],
            "timestamp": datetime.utcnow().isoformat(),
            "ip_address": data.get('ip_address', 'unknown')
        }

        # Update fields
        update_fields = {
            "signed_at": datetime.utcnow().isoformat(),
            "client_signature": signature_data,
            "status": CONTRACT_STATUS['CLIENT_SIGNED'],
            "signer_name": data['name'],
            "current_content": data.get('content', contract.get('current_content', ''))
        }

        # Update the contract
        result = await contracts_collection.update_one(
            {"client_id": client_id},
            {"$set": update_fields}
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=500, detail="Failed to update contract")

        # Get the updated contract with rep signature
        updated_contract = await contracts_collection.find_one(
            {"client_id": client_id},
            {
                "client_info": 1,
                "current_content": 1,
                "client_signature": 1,
                "rep_signature": 1  # Explicitly include rep_signature field
            }
        )

        # If there's no rep signature yet, try to find it from a previous version
        if "rep_signature" not in updated_contract:
            previous_contract = await contracts_collection.find_one(
                {
                    "client_id": client_id,
                    "rep_signature": {"$exists": True}
                }
            )
            if previous_contract and "rep_signature" in previous_contract:
                # Update current contract with rep signature from previous version
                await contracts_collection.update_one(
                    {"client_id": client_id},
                    {"$set": {"rep_signature": previous_contract["rep_signature"]}}
                )
                updated_contract["rep_signature"] = previous_contract["rep_signature"]

        # Generate PDF with signatures
        buffer = BytesIO()
        p = canvas.Canvas(buffer, pagesize=letter)
        
        # Add contract content with page management
        y_position = 750  # Starting position on first page
        margin_left = 50
        margin_right = 550  # This creates about 1 inch right margin
        line_height = 15

        # Split content into lines and wrap long lines
        content_lines = updated_contract["current_content"].split('\n')
        wrapped_lines = []
        for line in content_lines:
            # Wrap any line that's too long (about 90 characters per line)
            if line.strip():  # Only wrap non-empty lines
                wrapped = wrap(line, width=90)
                wrapped_lines.extend(wrapped)
            else:
                wrapped_lines.append(line)  # Keep empty lines for spacing

        # Draw the wrapped lines
        for line in wrapped_lines:
            # Check if we need a new page
            if y_position < 50:  # Leave margin at bottom
                p.showPage()  # Add a new page
                y_position = 750  # Reset position for new page
            
            # Draw the line
            p.drawString(margin_left, y_position, line)
            y_position -= line_height

        # Add signature section (on new page if needed)
        if y_position < 200:  # Need at least 200 points for signatures
            p.showPage()
            y_position = 750

        # Add signature section
        y_position -= 30
        p.drawString(50, y_position, "SIGNATURES")
        
        # Add client signature
        sig_data = updated_contract["client_signature"]
        y_position -= 30
        p.drawString(50, y_position, f"Client: {sig_data.get('typed_name', '')}")
        y_position -= 15
        p.drawString(50, y_position, f"Signed on: {sig_data.get('timestamp', '')}")
        y_position -= 15
        p.drawString(50, y_position, f"IP Address: {sig_data.get('ip_address', 'unknown')}")

        # Add representative signature if it exists
        if "rep_signature" in updated_contract:
            rep_sig = updated_contract["rep_signature"]
            y_position -= 30
            p.drawString(50, y_position, f"Snapped Representative: {rep_sig.get('typed_name', '')}")
            y_position -= 15
            p.drawString(50, y_position, f"Signed on: {rep_sig.get('timestamp', '')}")
            y_position -= 15
            p.drawString(50, y_position, f"IP Address: {rep_sig.get('ip_address', 'unknown')}")

        # Save the final page
        p.showPage()
        p.save()
        buffer.seek(0)

        # Send email to client
        client_email_body = f"""
Hello {contract['client_info']['first_name']},

Thank you for signing your contract with Snapped. Please find your signed contract attached.

Best regards,
Snapped, LLC
        """

        await send_contract_email(
            to_email=contract['client_info']['email'],
            subject="Your Signed Snapped Contract",
            body=client_email_body,
            pdf_buffer=buffer
        )

        # Send copy to Snapped team
        buffer.seek(0)  # Reset buffer for second email
        team_email_body = f"""
A contract has been signed by {contract['client_info']['first_name']} {contract['client_info']['last_name']}.

Client Details:
- Name: {contract['client_info']['first_name']} {contract['client_info']['last_name']}
- Email: {contract['client_info']['email']}
- Client ID: {client_id}

The signed contract is attached.
        """

        await send_contract_email(
            to_email=FROM_EMAIL,  # Your team's email
            subject=f"Contract Signed - {contract['client_info']['first_name']} {contract['client_info']['last_name']}",
            body=team_email_body,
            pdf_buffer=buffer
        )

        return {
            "status": "success",
            "message": "Contract signed successfully and emails sent"
        }

    except Exception as e:
        print(f"Error in sign_contract: {str(e)}")  # Debug log
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/sign/representative")
async def sign_contract_representative(data: dict):
    """Process representative signature for a contract."""
    try:
        # Convert to async operation
        contract = await contracts_collection.find_one({"_id": ObjectId(data["contract_id"])})
        if not contract:
            raise HTTPException(status_code=404, detail="Contract not found")
        
        # Determine final status based on whether client has signed
        new_status = CONTRACT_STATUS['REP_SIGNED']
        if contract.get('client_signature'):
            new_status = CONTRACT_STATUS['FULLY_EXECUTED']

        # Update with async operation
        result = await contracts_collection.update_one(
            {"_id": ObjectId(data["contract_id"])},
            {
                "$set": {
                    "status": new_status,
                    "rep_signature": {
                        "signature": data["signature"],  # Changed from signature_image
                        "ip_address": data.get("ip_address", "unknown"),
                        "timestamp": datetime.utcnow().isoformat(),
                        "typed_name": data["name"],
                        "signed_by": f"Snapped Representative - {data['name']}"
                    }
                }
            }
        )

        if result.modified_count == 0:
            raise HTTPException(status_code=404, detail="Failed to update contract")

        # Get updated contract
        updated_contract = await contracts_collection.find_one({"_id": ObjectId(data["contract_id"])})
        
        return {
            "status": "success",
            "message": "Contract signed by representative",
            "contract": {
                "id": str(updated_contract["_id"]),
                "status": updated_contract["status"],
                "rep_signature": updated_contract["rep_signature"]
            }
        }

    except Exception as e:
        print(f"Error in rep signing: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/download/{id}")
async def download_contract(id: str):
    """Generate and download a contract PDF."""
    try:
        # Try to find contract by client_id first
        contract = contracts_collection.find_one({"client_id": id})
        
        # If not found, try to find by ObjectId
        if not contract:
            try:
                contract = contracts_collection.find_one({"_id": ObjectId(id)})
            except:
                pass

        if not contract:
            raise HTTPException(status_code=404, detail="Contract not found")

        # Generate PDF
        buffer = BytesIO()
        p = canvas.Canvas(buffer, pagesize=letter)
        
        # Add contract content with page management
        y_position = 750  # Starting position on first page
        margin_left = 50
        margin_right = 550  # This creates about 1 inch right margin
        line_height = 15

        # Split content into lines and wrap long lines
        content_lines = contract.get("current_content", "").split('\n')
        wrapped_lines = []
        for line in content_lines:
            # Wrap any line that's too long (about 90 characters per line)
            if line.strip():  # Only wrap non-empty lines
                wrapped = wrap(line, width=90)
                wrapped_lines.extend(wrapped)
            else:
                wrapped_lines.append(line)  # Keep empty lines for spacing

        # Draw the wrapped lines
        for line in wrapped_lines:
            # Check if we need a new page
            if y_position < 50:  # Leave margin at bottom
                p.showPage()  # Add a new page
                y_position = 750  # Reset position for new page
            
            # Draw the line
            p.drawString(margin_left, y_position, line)
            y_position -= line_height

        # Add signature section (on new page if needed)
        if y_position < 200:  # Need at least 200 points for signatures
            p.showPage()
            y_position = 750

        # Add signature section
        y_position -= 30
        p.drawString(50, y_position, "SIGNATURES")
        
        # Add client signature if exists
        if "client_signature" in contract:
            sig_data = contract["client_signature"]
            y_position -= 30
            p.drawString(50, y_position, f"Client: {sig_data.get('typed_name', '')}")
            y_position -= 15
            p.drawString(50, y_position, f"Date: {sig_data.get('timestamp', '')}")
            y_position -= 15
            p.drawString(50, y_position, f"IP Address: {sig_data.get('ip_address', 'unknown')}")

        # Add representative signature if exists
        if "rep_signature" in contract:
            rep_sig = contract["rep_signature"]
            y_position -= 30
            p.drawString(50, y_position, f"Snapped Representative: {rep_sig.get('typed_name', '')}")
            y_position -= 15
            p.drawString(50, y_position, f"Date: {rep_sig.get('timestamp', '')}")
            y_position -= 15
            p.drawString(50, y_position, f"IP Address: {rep_sig.get('ip_address', 'unknown')}")
        
        # Save the final page
        p.showPage()
        p.save()
        buffer.seek(0)
        
        return FileResponse(
            buffer, 
            media_type="application/pdf",
            filename=f"signed_contract_{id}.pdf",
            headers={"Content-Disposition": f"attachment; filename=signed_contract_{id}.pdf"}
        )

    except Exception as e:
        print(f"Error generating PDF: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/send_to_client")
async def send_contract_to_client(data: dict):
    """Send a contract to a client for signing."""
    try:
        print(f"Received data: {data}")  # Debug log
        
        # Validate required fields
        if not data.get("contract_id"):
            raise HTTPException(status_code=400, detail="Missing contract_id")
        if not data.get("content"):
            raise HTTPException(status_code=400, detail="Missing content")
        if not data.get("client_id"):
            raise HTTPException(status_code=400, detail="Missing client_id")

        # First save/update the contract content
        try:
            contract_update = await contracts_collection.update_one(
                {"_id": ObjectId(data["contract_id"])},
                {
                    "$set": {
                        "current_content": data["content"],
                        "status": CONTRACT_STATUS['SENT'],
                        "sent_at": datetime.utcnow().isoformat()
                    }
                }
            )
            print(f"Contract update result: {contract_update.modified_count}")  # Debug log
        except Exception as e:
            print(f"Error updating contract: {str(e)}")  # Debug log
            raise HTTPException(status_code=500, detail=f"Failed to update contract: {str(e)}")

        # Get the updated contract
        contract = await contracts_collection.find_one({"_id": ObjectId(data["contract_id"])})
        if not contract:
            raise HTTPException(status_code=404, detail="Contract not found after update")

        # Get client info
        client_lookup = None
        try:
            client_lookup = {"_id": ObjectId(data["client_id"])}
        except:
            client_lookup = {"client_id": data["client_id"]}
        
        client_data = await client_info.find_one(client_lookup)
        if not client_data:
            raise HTTPException(status_code=404, detail="Client not found")

        # Generate PDF
        buffer = BytesIO()
        p = canvas.Canvas(buffer, pagesize=letter)
        
        # Add contract content with page management
        y_position = 750  # Starting position on first page
        margin_left = 50
        margin_right = 550  # This creates about 1 inch right margin
        line_height = 15

        # Split content into lines and wrap long lines
        content_lines = contract["current_content"].split('\n')
        wrapped_lines = []
        for line in content_lines:
            # Wrap any line that's too long (about 90 characters per line)
            if line.strip():  # Only wrap non-empty lines
                wrapped = wrap(line, width=90)
                wrapped_lines.extend(wrapped)
            else:
                wrapped_lines.append(line)  # Keep empty lines for spacing

        # Draw the wrapped lines
        for line in wrapped_lines:
            # Check if we need a new page
            if y_position < 50:  # Leave margin at bottom
                p.showPage()  # Add a new page
                y_position = 750  # Reset position for new page
            
            # Draw the line
            p.drawString(margin_left, y_position, line)
            y_position -= line_height

        # Add signature section (on new page if needed)
        if y_position < 200:  # Need at least 200 points for signatures
            p.showPage()
            y_position = 750

        # Add signature section
        y_position -= 30
        p.drawString(50, y_position, "SIGNATURES")
        
        # Add client signature if exists
        if "client_signature" in contract:
            sig_data = contract["client_signature"]
            y_position -= 30
            p.drawString(50, y_position, f"Client: {sig_data.get('typed_name', '')}")
            y_position -= 15
            p.drawString(50, y_position, f"Date: {sig_data.get('timestamp', '')}")
            y_position -= 15
            p.drawString(50, y_position, f"IP Address: {sig_data.get('ip_address', 'unknown')}")

        # Add representative signature if exists
        if "rep_signature" in contract:
            rep_sig = contract["rep_signature"]
            y_position -= 30
            p.drawString(50, y_position, f"Snapped Representative: {rep_sig.get('typed_name', '')}")
            y_position -= 15
            p.drawString(50, y_position, f"Date: {rep_sig.get('timestamp', '')}")
            y_position -= 15
            p.drawString(50, y_position, f"IP Address: {rep_sig.get('ip_address', 'unknown')}")
        
        # Save the final page
        p.showPage()
        p.save()
        buffer.seek(0)

        # Update the signing link to use track.snapped.cc
        signing_link = f"https://track.snapped.cc/sign-contract/{client_data['client_id']}"

        email_body = f"""
Hello {client_data.get('First_Legal_Name')},

Please review and sign your contract using the link below:
{signing_link}

Thank you,
Snapped, LLC
        """

        msg = MIMEMultipart()
        msg['From'] = FROM_EMAIL
        msg['To'] = client_data["Email_Address"]
        msg['Subject'] = "Your Contract is Ready for Signature"
        msg.attach(MIMEText(email_body, 'plain'))

        # Attach PDF
        pdf_attachment = MIMEApplication(buffer.getvalue())
        pdf_attachment.add_header('Content-Disposition', 'attachment', filename='contract.pdf')
        msg.attach(pdf_attachment)

        # Send email asynchronously
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: send_email_sync(msg))

        return {
            "status": "success",
            "message": f"Contract sent to {client_data['Email_Address']}",
            "signing_link": signing_link,
            "contract_status": CONTRACT_STATUS['SENT']  # Add status to response
        }

    except Exception as e:
        print(f"Error sending contract: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to send contract")

@router.get("/contract/{client_id}")
async def get_contract_for_signing(client_id: str):
    """Retrieve a contract for client signing."""
    try:
        print(f"Looking up contract with client_id: {client_id}")  # Debug log
        
        # Try different ways to find the contract
        contract = None
        
        # First try direct client_id lookup
        contract = await contracts_collection.find_one({"client_id": client_id})
        
        # If not found, try looking up by _id (in case client_id is actually an _id)
        if not contract:
            try:
                contract = await contracts_collection.find_one({"_id": ObjectId(client_id)})
            except:
                pass

        # If still not found, try looking up the client first
        if not contract:
            client = await client_info.find_one({"client_id": client_id})
            if client:
                contract = await contracts_collection.find_one({
                    "$or": [
                        {"client_id": client_id},
                        {"client_info.client_id": client_id},
                        {"client_id": str(client.get("_id"))}
                    ]
                })

        print(f"Found contract: {contract is not None}")  # Debug log
        print(f"Contract data: {contract}")  # Debug log

        if not contract:
            return {
                "status": "success",
                "content": "",
                "client_name": "",
                "contract_id": None,
                "contract_status": "new"
            }

        # Ensure proper response structure
        return {
            "status": "success",
            "content": contract.get("current_content", ""),
            "client_name": f"{contract['client_info']['first_name']} {contract['client_info']['last_name']}",
            "contract_id": str(contract["_id"]),
            "client_id": contract.get("client_id"),  # Add this
            "contract_status": contract.get("status", "draft"),
            "client_signature": contract.get("client_signature"),
            "rep_signature": contract.get("rep_signature")
        }

    except Exception as e:
        print(f"Error fetching contract: {str(e)}")
        print(f"Client ID received: {client_id}")  # Debug log
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/versions/{client_id}")
async def get_contract_versions(client_id: str):
    """Retrieve version history for a contract."""
    try:
        contract = await contracts_collection.find_one({"client_id": client_id})
        if not contract:
            return {
                "status": "success",
                "versions": []
            }
        
        # Get all versions and sort by version number
        versions = contract.get("versions", [])
        versions.sort(key=lambda x: x.get('version', 0), reverse=True)  # Newest first
        
        return {
            "status": "success",
            "versions": versions  # Just return the versions array
        }
    except Exception as e:
        print(f"Error getting versions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("")
async def get_contracts(filter_query: dict = Depends(get_filtered_query)):
    """Retrieve filtered list of contracts."""
    contracts = await contracts_collection.find(filter_query).to_list(length=None)
    return contracts 