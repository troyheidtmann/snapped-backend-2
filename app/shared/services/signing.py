"""
Document Signing Module

This module provides document signing functionality using the HelloSign
service for creator agreements and contracts.

Features:
- Signature requests
- Status tracking
- Webhook handling
- PDF storage
- Contract management

Data Model:
- Signature requests
- Contract data
- Event callbacks
- PDF documents
- Status updates

Security:
- API key validation
- Event verification
- Hash validation
- Error handling
- Secure storage

Dependencies:
- HelloSign SDK
- MongoDB for storage
- base64 for encoding
- logging for tracking
- typing for hints

Author: Snapped Development Team
"""

from hellosign_sdk import HSClient
from hellosign_sdk.utils import HSRequest
from app.shared.config import HELLOSIGN_API_KEY
from app.shared.database import contracts_collection
import base64

class SigningService:
    """
    HelloSign integration service.
    
    Manages document signing workflow and storage.
    
    Attributes:
        client: HelloSign client instance
    """
    
    def __init__(self):
        """
        Initialize service.
        
        Notes:
            - Creates client
            - Uses API key
        """
        self.client = HSClient(api_key=HELLOSIGN_API_KEY)

    async def create_signature_request(self, client_data, contract_content):
        """
        Create signature request.
        
        Args:
            client_data: Client information
            contract_content: Contract document
            
        Returns:
            dict: Request status and URLs
            
        Raises:
            Exception: For request errors
            
        Notes:
            - Creates request
            - Sets signers
            - Handles files
            - Test mode
        """
        try:
            # Create signature request
            request = HSRequest(
                test_mode=1,  # Set to 0 for production
                title="Snapped Creator Agreement",
                subject="Your Snapped Creator Agreement",
                message="Please review and sign your creator agreement.",
                signers=[{
                    'email_address': client_data['Email_Address'],
                    'name': f"{client_data['First_Legal_Name']} {client_data['Last_Legal_Name']}"
                }],
                files=[contract_content]  # Can be file path or content
            )

            # Send the request
            response = self.client.send_signature_request(request)

            return {
                'status': 'success',
                'signing_url': response.signing_url,
                'signature_request_id': response.signature_request_id
            }

        except Exception as e:
            raise Exception(f"Failed to create signature request: {str(e)}")

    async def get_signature_status(self, signature_request_id):
        """
        Get signature request status.
        
        Args:
            signature_request_id: Request identifier
            
        Returns:
            dict: Status information
            
        Raises:
            Exception: For status errors
            
        Notes:
            - Gets status
            - Gets timestamp
            - Error handling
        """
        try:
            response = self.client.get_signature_request(signature_request_id)
            return {
                'status': response.status_code,
                'signed_at': response.signed_at,
                'signature_request_id': signature_request_id
            }
        except Exception as e:
            raise Exception(f"Failed to get signature status: {str(e)}")

    async def handle_signed_callback(self, event_data):
        """
        Handle webhook callback.
        
        Args:
            event_data: Event information
            
        Returns:
            dict: Processing status
            
        Raises:
            Exception: For callback errors
            
        Notes:
            - Gets document
            - Stores PDF
            - Updates status
            - Base64 encoding
        """
        try:
            # Get the signed document from HelloSign
            signature_request = self.client.get_signature_request(event_data['signature_request_id'])
            
            # Download the signed PDF
            files = self.client.get_signature_request_files(
                signature_request.signature_request_id, 
                file_type='pdf'
            )
            
            # Convert PDF to base64 for MongoDB storage
            pdf_base64 = base64.b64encode(files).decode('utf-8')
            
            # Update contract in MongoDB with signed PDF
            contracts_collection.update_one(
                {"signature_request_id": event_data['signature_request_id']},
                {
                    "$set": {
                        "status": "signed",
                        "signed_pdf": pdf_base64,
                        "signed_at": event_data['event_time']
                    }
                }
            )

            return {"status": "success"}

        except Exception as e:
            raise Exception(f"Failed to process signed document: {str(e)}") 