"""
Cognito JWT Validation Module

This module handles JWT validation for AWS Cognito tokens.
It implements proper JWK validation, caching, and token verification.

Features:
- JWK caching and auto-refresh
- Token validation
- Signature verification
- Claims validation
- Error handling
"""

import jwt
import requests
import json
from datetime import datetime
import time
from typing import Dict, Optional, List
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Cognito configuration
COGNITO_REGION = 'us-east-2'
COGNITO_USER_POOL_ID = 'us-east-2_iIfwSsdCU'  # Primary user pool
COGNITO_APP_CLIENT_ID = '1rv7iijlcgv4cortina322ntri'

# JWK URL for the user pool
COGNITO_JWK_URL = f'https://cognito-idp.{COGNITO_REGION}.amazonaws.com/{COGNITO_USER_POOL_ID}/.well-known/jwks.json'

# Cache for JWKs
jwks_cache = {
    'keys': None,
    'last_updated': 0,
    'cache_duration': 3600  # Cache for 1 hour
}

def get_jwks() -> Dict:
    """
    Fetch and cache JWKs from Cognito.
    
    Returns:
        Dict: JWKs from Cognito
        
    Notes:
        - Caches JWKs for 1 hour
        - Auto-refreshes when expired
        - Handles connection errors
    """
    current_time = time.time()
    
    # Return cached keys if still valid
    if (jwks_cache['keys'] is not None and 
        current_time - jwks_cache['last_updated'] < jwks_cache['cache_duration']):
        return jwks_cache['keys']
    
    try:
        logger.info("Fetching fresh JWKs from Cognito")
        response = requests.get(COGNITO_JWK_URL)
        response.raise_for_status()
        jwks = response.json()
        
        # Update cache
        jwks_cache['keys'] = jwks
        jwks_cache['last_updated'] = current_time
        
        return jwks
    except Exception as e:
        logger.error(f"Error fetching JWKs: {str(e)}")
        # Return cached keys if available, even if expired
        if jwks_cache['keys'] is not None:
            logger.warning("Using expired JWKs from cache")
            return jwks_cache['keys']
        raise

def get_public_key(kid: str) -> Optional[Dict]:
    """
    Get public key for token verification.
    
    Args:
        kid: Key ID from token header
        
    Returns:
        Dict: Public key if found
        
    Notes:
        - Matches kid from token
        - Returns None if not found
        - Handles missing kid
    """
    jwks = get_jwks()
    for key in jwks['keys']:
        if key['kid'] == kid:
            return key
    return None

def validate_token(token: str) -> Dict:
    """
    Validate and decode a Cognito JWT token.
    
    Args:
        token: JWT token to validate
        
    Returns:
        Dict: Decoded token claims
        
    Raises:
        jwt.InvalidTokenError: For invalid tokens
        
    Notes:
        - Verifies signature
        - Validates claims
        - Checks expiration
        - Verifies issuer
    """
    try:
        # First decode headers without verification to get kid
        headers = jwt.get_unverified_header(token)
        if 'kid' not in headers:
            raise jwt.InvalidTokenError("Token missing kid in headers")
            
        # Get public key
        public_key = get_public_key(headers['kid'])
        if not public_key:
            raise jwt.InvalidTokenError("Unable to find public key for token")
            
        # Convert JWK to PEM format
        public_key_pem = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(public_key))
        
        # Decode and verify token
        decoded = jwt.decode(
            token,
            key=public_key_pem,
            algorithms=['RS256'],
            options={
                'verify_signature': True,
                'verify_exp': True,
                'verify_iat': True,
                'verify_aud': True,
                'verify_iss': True
            },
            audience=COGNITO_APP_CLIENT_ID,
            issuer=f'https://cognito-idp.{COGNITO_REGION}.amazonaws.com/{COGNITO_USER_POOL_ID}'
        )
        
        return decoded
        
    except jwt.ExpiredSignatureError:
        logger.warning("Token has expired")
        raise jwt.InvalidTokenError("Token has expired")
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error validating token: {str(e)}")
        raise jwt.InvalidTokenError(f"Token validation failed: {str(e)}")

def get_user_from_token(token: str) -> Dict:
    """
    Extract user information from validated token.
    
    Args:
        token: JWT token
        
    Returns:
        Dict: User information including:
            - user_id: User identifier
            - groups: User groups
            - email: User email
            
    Notes:
        - Validates token first
        - Extracts standard claims
        - Handles custom attributes
    """
    try:
        # Validate and decode token
        decoded = validate_token(token)
        
        # Extract user information
        user_info = {
            'user_id': (
                decoded.get('custom:UserID') or 
                decoded.get('cognito:username') or 
                decoded.get('sub')
            ),
            'groups': [
                g.upper() for g in (
                    decoded.get('cognito:groups', []) or 
                    decoded.get('groups', []) or 
                    ['DEFAULT']
                )
            ],
            'email': decoded.get('email'),
            'given_name': decoded.get('given_name'),
            'family_name': decoded.get('family_name')
        }
        
        return user_info
        
    except jwt.InvalidTokenError as e:
        logger.error(f"Invalid token in get_user_from_token: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error in get_user_from_token: {str(e)}")
        raise jwt.InvalidTokenError(f"Failed to extract user info: {str(e)}") 