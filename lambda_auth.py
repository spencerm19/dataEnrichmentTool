import json
import boto3
import requests
from typing import Tuple, Dict, Any
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

logger = Logger()

# Constants
ZOOMINFO_SECRET_NAME = "zoominfo/credentials-dev"
ZOOMINFO_AUTH_URL = "https://api.zoominfo.com/authenticate"

class AuthError(Exception):
    """Custom exception for authentication errors"""
    pass

@logger.inject_lambda_context
def get_zoominfo_credentials(event: Dict = None, context: Any = None) -> Tuple[str, str]:
    """
    Retrieve ZoomInfo credentials from AWS Secrets Manager
    
    Args:
        event: Lambda event object (optional)
        context: Lambda context object (optional)
    
    Returns:
        Tuple containing username and password
    
    Raises:
        AuthError: If credentials cannot be retrieved
    """
    try:
        session = boto3.session.Session()
        client = session.client('secretsmanager')
        
        response = client.get_secret_value(
            SecretId=ZOOMINFO_SECRET_NAME
        )
        secret = json.loads(response['SecretString'])
        
        if 'username' not in secret or 'password' not in secret:
            raise AuthError("Invalid secret format: missing username or password")
            
        return secret['username'], secret['password']
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"Failed to retrieve secret: {error_code}")
        raise AuthError(f"Failed to retrieve credentials: {str(e)}")
    except json.JSONDecodeError as e:
        logger.error("Failed to parse secret value as JSON")
        raise AuthError(f"Invalid secret format: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error retrieving credentials: {str(e)}")
        raise AuthError(f"Failed to retrieve credentials: {str(e)}")

@logger.inject_lambda_context
def authenticate() -> str:
    """
    Authenticate with ZoomInfo API using credentials from Secrets Manager
    
    Returns:
        str: JWT token for API access
    
    Raises:
        AuthError: If authentication fails
    """
    try:
        username, password = get_zoominfo_credentials()
        
        payload = json.dumps({
            "username": username,
            "password": password
        })
        headers = {
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            ZOOMINFO_AUTH_URL,
            headers=headers,
            data=payload,
            timeout=10  # Add timeout for the request
        )
        
        if response.status_code != 200:
            logger.error(f"Authentication failed with status {response.status_code}")
            raise AuthError(f"Authentication failed: {response.text}")
            
        return response.json()["jwt"]
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request to ZoomInfo API failed: {str(e)}")
        raise AuthError(f"Failed to connect to ZoomInfo API: {str(e)}")
    except KeyError as e:
        logger.error("JWT token not found in response")
        raise AuthError("Invalid response format from ZoomInfo API")
    except Exception as e:
        logger.error(f"Unexpected error during authentication: {str(e)}")
        raise AuthError(f"Authentication failed: {str(e)}")

# Optional: Add token caching if needed
_cached_token = None
_token_expiry = None

def get_valid_token() -> str:
    """
    Get a valid JWT token, using cached token if available and not expired
    
    Returns:
        str: Valid JWT token
    """
    global _cached_token, _token_expiry
    
    # TODO: Implement token caching logic here
    # For now, just get a new token each time
    return authenticate() 