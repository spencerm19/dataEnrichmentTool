import pytest
import os
import csv
import json
import time
from unittest.mock import patch, MagicMock
from lambda_enrichment import EnrichmentProcessor

@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file for testing"""
    csv_path = tmp_path / "test_input.csv"
    data = [
        {
            "Supplier Company": "Test Company",
            "Supplier First Name": "John",
            "Supplier Last Name": "Doe",
            "Supplier Email": "john@test.com",
            "Supplier Phone": "1234567890",
            "Supplier Street": "123 Test St",
            "Supplier City": "Test City",
            "Supplier State": "TS",
            "Supplier Zip Code": "12345",
            "Supplier Country": "Test Country",
            "Site Name": "Test Site",
            "Site ID": "SITE001",
            "Additional Contact Info": "Test Info"
        }
    ]
    
    with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    
    return str(csv_path)

@pytest.fixture
def output_csv(tmp_path):
    """Create path for output CSV file"""
    return str(tmp_path / "test_output.csv")

@pytest.fixture
def mock_auth():
    """Mock the authentication module"""
    with patch('lambda_auth.get_valid_token') as mock:
        mock.return_value = "test_token"
        yield mock

@pytest.fixture
def mock_requests():
    """Mock requests module"""
    with patch('requests.post') as mock:
        yield mock

def test_csv_to_json_conversion(sample_csv, output_csv, mock_auth):
    """Test CSV to JSON conversion"""
    processor = EnrichmentProcessor(sample_csv, output_csv)
    processor._csv_to_json()
    
    assert len(processor.data) == 1
    record = processor.data[0]
    
    # Check mapped fields
    assert record["companyName"] == "Test Company"
    assert record["firstName"] == "John"
    assert record["lastName"] == "Doe"
    assert record["emailAddress"] == "john@test.com"
    
    # Check additional fields were added
    assert "zi_c_name" in record
    assert "zi_c_company_id" in record
    assert record["enrichmentStatus"] == "Success"

def test_json_to_csv_conversion(sample_csv, output_csv, mock_auth):
    """Test JSON to CSV conversion"""
    processor = EnrichmentProcessor(sample_csv, output_csv)
    processor._csv_to_json()
    processor._json_to_csv()
    
    assert os.path.exists(output_csv)
    
    with open(output_csv, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        data = list(reader)
        
    assert len(data) == 1
    record = data[0]
    
    # Check mapped fields
    assert record["Supplier Company"] == "Test Company"
    assert record["Supplier First Name"] == "John"
    assert record["Supplier Last Name"] == "Doe"
    assert record["Supplier Email"] == "john@test.com"
    
    # Check additional fields
    assert "Zoominfo Company Name" in record
    assert "Zoominfo Company ID" in record
    assert record["Enrichment Status"] == "Success"

def test_contact_enrichment(sample_csv, output_csv, mock_auth, mock_requests):
    """Test contact enrichment"""
    # Mock successful API response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "success": True,
        "data": {
            "result": [{
                "matchStatus": "FULL_MATCH",
                "data": {
                    "firstName": "John Updated",
                    "lastName": "Doe Updated",
                    "email": "john.updated@test.com",
                    "phone": "9876543210",
                    "jobTitle": "Test Manager"
                }
            }]
        }
    }
    mock_requests.return_value = mock_response
    
    processor = EnrichmentProcessor(sample_csv, output_csv)
    processor._csv_to_json()
    entry = processor.data[0]
    entry["firstName"] = ""  # Clear existing data to test enrichment
    
    enriched_entry = processor._enrich_contact(entry)
    
    assert enriched_entry["firstName"] == "John Updated"
    assert enriched_entry["jobTitle"] == "Test Manager"
    assert mock_requests.called

def test_company_enrichment(sample_csv, output_csv, mock_auth, mock_requests):
    """Test company enrichment"""
    # Mock successful API response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "success": True,
        "data": {
            "result": [{
                "data": {
                    "zi_c_name": "Test Company Inc",
                    "zi_c_company_id": "12345",
                    "zi_c_url": "www.testcompany.com",
                    "zi_c_linkedin_url": "linkedin.com/company/testcompany",
                    "zi_c_naics6": "123456",
                    "zi_c_employees": "1000"
                }
            }]
        }
    }
    mock_requests.return_value = mock_response
    
    processor = EnrichmentProcessor(sample_csv, output_csv)
    processor._csv_to_json()
    entry = processor.data[0]
    
    enriched_entry = processor._enrich_company(entry)
    
    assert enriched_entry["zi_c_name"] == "Test Company Inc"
    assert enriched_entry["zi_c_company_id"] == "12345"
    assert enriched_entry["zi_c_url"] == "www.testcompany.com"
    assert mock_requests.called

def test_needs_contact_update(sample_csv, output_csv, mock_auth):
    """Test needs contact flag update"""
    processor = EnrichmentProcessor(sample_csv, output_csv)
    processor._csv_to_json()
    entry = processor.data[0]
    
    # Test with contact info present
    updated_entry = processor._update_needs_contact(entry)
    assert updated_entry["needsContact"] == "No"
    
    # Test with no contact info
    entry["firstName"] = ""
    entry["lastName"] = ""
    entry["emailAddress"] = ""
    entry["phone"] = ""
    updated_entry = processor._update_needs_contact(entry)
    assert updated_entry["needsContact"] == "Yes"

def test_token_refresh(sample_csv, output_csv, mock_auth):
    """Test token refresh logic"""
    processor = EnrichmentProcessor(sample_csv, output_csv)
    processor.jwt_token = "old_token"
    processor.last_auth_time = time.time() - (56 * 60)  # 56 minutes ago
    
    processor._check_token()
    
    assert mock_auth.called
    assert processor.jwt_token == "test_token"

def test_full_process_with_enrichment(sample_csv, output_csv, mock_auth, mock_requests):
    """Test the complete enrichment process"""
    # Mock successful API responses
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "success": True,
        "data": {
            "result": [{
                "matchStatus": "FULL_MATCH",
                "data": {
                    "firstName": "John Updated",
                    "lastName": "Doe Updated",
                    "email": "john.updated@test.com",
                    "phone": "9876543210",
                    "jobTitle": "Test Manager",
                    "zi_c_name": "Test Company Inc",
                    "zi_c_company_id": "12345"
                }
            }]
        }
    }
    mock_requests.return_value = mock_response
    
    processor = EnrichmentProcessor(sample_csv, output_csv)
    processor.process()
    
    assert os.path.exists(output_csv)
    assert mock_auth.called
    assert mock_requests.called
    
    with open(output_csv, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        data = list(reader)
        
    assert len(data) == 1
    record = data[0]
    assert record["Supplier Company"] == "Test Company"
    assert record["Needs New Contact"] in ["Yes", "No"]

def test_error_handling(sample_csv, output_csv, mock_auth, mock_requests):
    """Test error handling during processing"""
    # Mock API error
    mock_requests.side_effect = Exception("API Error")
    
    processor = EnrichmentProcessor(sample_csv, output_csv)
    processor._csv_to_json()
    
    # Test contact enrichment error handling
    entry = processor.data[0]
    enriched_entry = processor._enrich_contact(entry)
    assert enriched_entry == entry  # Should return original entry on error
    
    # Test company enrichment error handling
    enriched_entry = processor._enrich_company(entry)
    assert enriched_entry == entry  # Should return original entry on error

def test_invalid_input_file(tmp_path, mock_auth):
    """Test handling of invalid input file"""
    invalid_path = str(tmp_path / "nonexistent.csv")
    output_path = str(tmp_path / "output.csv")
    
    processor = EnrichmentProcessor(invalid_path, output_path)
    
    with pytest.raises(Exception):
        processor.process()

def test_aws_secret_auth():
    """Test authentication using actual AWS Secrets Manager credentials"""
    from lambda_auth import get_zoominfo_credentials, authenticate
    
    # Create mock event and context
    event = {}
    context = type('LambdaContext', (), {
        'function_name': 'test-function',
        'function_version': '$LATEST',
        'invoked_function_arn': 'arn:aws:lambda:us-west-2:123456789012:function:test-function',
        'memory_limit_in_mb': 128,
        'aws_request_id': 'test-request-id',
        'log_group_name': '/aws/lambda/test-function',
        'log_stream_name': '2024/04/24/[$LATEST]test123',
        'identity': None,
        'client_context': None
    })()
    
    # Test credentials retrieval
    username, password = get_zoominfo_credentials(event, context)
    assert username == "zoominfo@avetta.com"
    
    # Test authentication
    token = authenticate()
    assert token is not None and len(token) > 0 