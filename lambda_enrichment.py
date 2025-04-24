import json
import csv
import os
import time
import requests
from typing import Dict, List, Any
from aws_lambda_powertools import Logger
import lambda_auth

logger = Logger()

class EnrichmentProcessor:
    """
    Handles the data enrichment process for CSV files in Lambda
    """
    
    def __init__(self, input_path: str, output_path: str):
        """
        Initialize the enrichment processor
        
        Args:
            input_path: Path to input CSV file
            output_path: Path where output CSV will be saved
        """
        self.input_path = input_path
        self.output_path = output_path
        self.jwt_token = None
        self.last_auth_time = None
        self.data = []
        
    def process(self) -> None:
        """
        Execute the full enrichment process
        """
        try:
            # Get authentication token
            self.jwt_token = lambda_auth.get_valid_token()
            
            # Convert CSV to JSON
            self._csv_to_json()
            
            # Enrich the data
            self._enrich_data()
            
            # Convert back to CSV
            self._json_to_csv()
            
        except Exception as e:
            logger.exception("Error during enrichment process")
            raise
            
    def _csv_to_json(self) -> None:
        """
        Convert input CSV to JSON format
        """
        header_mapping = {
            "Supplier Company": "companyName",
            "Supplier First Name": "firstName",
            "Supplier Last Name": "lastName",
            "Supplier Email": "emailAddress",
            "Supplier Phone": "phone",
            "Supplier Street": "companyStreet",
            "Supplier City": "companyCity",
            "Supplier State": "companyState",
            "Supplier Zip Code": "companyZipCode",
            "Supplier Country": "companyCountry",
            "Site Name": "siteName",
            "Site ID": "siteID",
            "Additional Contact Info": "additionalContactInfo"
        }
        
        new_json_values = {
            "zi_c_name": "",
            "zi_c_company_id": "",
            "zi_c_company_name": "",
            "jobTitle": "",
            "zi_c_phone": "",
            "zi_c_url": "",
            "zi_c_linkedin_url": "",
            "zi_c_naics6": "",
            "sectorTitle": "",
            "primaryIndustry": "",
            "zi_c_employees": "",
            "zi_c_street": "",
            "zi_c_city": "",
            "zi_c_state": "",
            "zi_c_zip": "",
            "zi_c_country": "",
            "zi_c_location_id": "",
            "needsContact": "",
            "newContactFound": "",
            "personId": "",
            "contactMatchCriteria": "",
            "enrichmentStatus": "Success",
            "errorMessage": ""
        }
        
        try:
            with open(self.input_path, 'r', encoding='utf-8-sig') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    mapped_row = {}
                    
                    # Map CSV headers to JSON fields
                    for key, value in row.items():
                        new_key = header_mapping.get(key, key)
                        mapped_row[new_key] = value.strip()
                    
                    # Add additional fields
                    mapped_row.update(new_json_values)
                    self.data.append(mapped_row)
                    
            logger.info(f"Converted {len(self.data)} records to JSON format")
            
        except Exception as e:
            logger.error(f"Error converting CSV to JSON: {str(e)}")
            raise
            
    def _check_token(self) -> None:
        """Check if token needs refresh and get new one if needed"""
        current_time = time.time()
        if not self.jwt_token or not self.last_auth_time or \
           (current_time - self.last_auth_time) > (55 * 60):  # Refresh if older than 55 minutes
            self.jwt_token = lambda_auth.get_valid_token()
            self.last_auth_time = current_time

    def _enrich_contact(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich contact information using ZoomInfo API
        
        Args:
            entry: Dictionary containing contact information
            
        Returns:
            Dictionary with enriched contact information
        """
        try:
            self._check_token()
            
            # Prepare the API request
            url = "https://api.zoominfo.com/enrich/contact"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.jwt_token}"
            }
            
            payload = {
                "firstName": entry.get("firstName", ""),
                "lastName": entry.get("lastName", ""),
                "email": entry.get("emailAddress", ""),
                "phone": entry.get("phone", ""),
                "companyName": entry.get("companyName", "")
            }
            
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get("success") and response_data.get("data", {}).get("result"):
                    result = response_data["data"]["result"][0]
                    if result.get("matchStatus") == "FULL_MATCH" and result.get("data"):
                        contact_data = result["data"]
                        entry.update({
                            "firstName": contact_data.get("firstName", entry.get("firstName", "")),
                            "lastName": contact_data.get("lastName", entry.get("lastName", "")),
                            "emailAddress": contact_data.get("email", entry.get("emailAddress", "")),
                            "phone": contact_data.get("phone", entry.get("phone", "")),
                            "jobTitle": contact_data.get("jobTitle", ""),
                            "enrichmentStatus": "Success"
                        })
                    else:
                        entry["enrichmentStatus"] = "No Match Found"
                else:
                    entry["enrichmentStatus"] = "No Data Available"
            else:
                logger.error(f"Contact enrichment failed with status {response.status_code}: {response.text}")
                entry["enrichmentStatus"] = f"API Error: {response.status_code}"
                
        except Exception as e:
            logger.error(f"Error enriching contact: {str(e)}")
            entry["enrichmentStatus"] = f"Error: {str(e)}"
            
        return entry

    def _enrich_company(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich company information using ZoomInfo API
        
        Args:
            entry: Dictionary containing company information
            
        Returns:
            Dictionary with enriched company information
        """
        try:
            self._check_token()
            
            # Prepare the API request
            url = "https://api.zoominfo.com/enrich/company"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.jwt_token}"
            }
            
            payload = {
                "companyName": entry.get("companyName", ""),
                "street": entry.get("companyStreet", ""),
                "city": entry.get("companyCity", ""),
                "state": entry.get("companyState", ""),
                "zipCode": entry.get("companyZipCode", ""),
                "country": entry.get("companyCountry", "")
            }
            
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code == 200:
                response_data = response.json()
                if response_data.get("success") and response_data.get("data", {}).get("result"):
                    result = response_data["data"]["result"][0]
                    if result.get("data"):
                        company_data = result["data"]
                        entry.update({
                            "zi_c_name": company_data.get("zi_c_name", ""),
                            "zi_c_company_id": company_data.get("zi_c_company_id", ""),
                            "zi_c_url": company_data.get("zi_c_url", ""),
                            "zi_c_linkedin_url": company_data.get("zi_c_linkedin_url", ""),
                            "zi_c_naics6": company_data.get("zi_c_naics6", ""),
                            "zi_c_employees": company_data.get("zi_c_employees", ""),
                            "enrichmentStatus": "Success"
                        })
                    else:
                        entry["enrichmentStatus"] = "No Data Available"
                else:
                    entry["enrichmentStatus"] = "No Match Found"
            else:
                logger.error(f"Company enrichment failed with status {response.status_code}: {response.text}")
                entry["enrichmentStatus"] = f"API Error: {response.status_code}"
                
        except Exception as e:
            logger.error(f"Error enriching company: {str(e)}")
            entry["enrichmentStatus"] = f"Error: {str(e)}"
            
        return entry

    def _update_needs_contact(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Update the needsContact flag based on contact information presence"""
        has_contact = all([
            entry.get("firstName", "").strip(),
            entry.get("lastName", "").strip(),
            entry.get("emailAddress", "").strip(),
            entry.get("phone", "").strip()
        ])
        entry["needsContact"] = "No" if has_contact else "Yes"
        return entry

    def _enrich_data(self) -> None:
        """
        Enrich the data using various enrichment modules
        """
        try:
            for entry in self.data:
                # Check and refresh token if needed
                self._check_token()
                
                # Enrich company data
                entry = self._enrich_company(entry)
                
                # Enrich contact data if needed
                if entry["needsContact"] == "Yes":
                    entry = self._enrich_contact(entry)
                    
        except Exception as e:
            logger.error(f"Error during data enrichment: {str(e)}")
            raise
            
    def _json_to_csv(self) -> None:
        """
        Convert enriched JSON data back to CSV format
        """
        csv_mapping = {
            "companyName": "Supplier Company",
            "companyStreet": "Supplier Street",
            "companyCity": "Supplier City",
            "companyState": "Supplier State",
            "companyZipCode": "Supplier Zip Code",
            "companyCountry": "Supplier Country",
            "firstName": "Supplier First Name",
            "lastName": "Supplier Last Name",
            "emailAddress": "Supplier Email",
            "phone": "Supplier Phone",
            "siteName": "Site Name",
            "siteID": "Site ID",
            "additionalContactInfo": "Additional Contact Info",
            "zi_c_name": "Zoominfo Company Name",
            "zi_c_company_id": "Zoominfo Company ID",
            "zi_c_company_name": "Company HQ Name",
            "zi_c_phone": "Company Phone",
            "zi_c_url": "Website",
            "zi_c_linkedin_url": "Company LinkedIn URL",
            "jobTitle": "Contact Job Title",
            "zi_c_naics6": "6-digit NAICS Code",
            "sectorTitle": "Sector Title",
            "primaryIndustry": "Primary Industry",
            "zi_c_employees": "Number of Employees",
            "zi_c_street": "Company Street",
            "zi_c_city": "Company City",
            "zi_c_state": "Company State",
            "zi_c_zip": "Company Zip Code",
            "zi_c_country": "Company Country",
            "zi_c_location_id": "Company Location ID",
            "needsContact": "Needs New Contact",
            "newContactFound": "New Contact Found",
            "personId": "Contact Person ID",
            "contactMatchCriteria": "Contact Match Criteria",
            "company_match_criteria": "Company Match Criteria",
            "enrichmentStatus": "Enrichment Status",
            "errorMessage": "Error Message"
        }
        
        try:
            # Get all fields from the data
            all_keys = set()
            for entry in self.data:
                all_keys.update(entry.keys())
                
            # Create headers list
            mapped_keys = [k for k in csv_mapping if k in all_keys]
            unmapped_keys = [k for k in all_keys if k not in mapped_keys]
            headers = [csv_mapping.get(key, key) for key in mapped_keys + unmapped_keys]
            
            # Write CSV file
            with open(self.output_path, 'w', newline='', encoding='utf-8-sig') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=headers)
                writer.writeheader()
                
                for entry in self.data:
                    row = {csv_mapping.get(k, k): v for k, v in entry.items()}
                    writer.writerow(row)
                    
            logger.info(f"Converted {len(self.data)} records to CSV format")
            
        except Exception as e:
            logger.error(f"Error converting JSON to CSV: {str(e)}")
            raise 