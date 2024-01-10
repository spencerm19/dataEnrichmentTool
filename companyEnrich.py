import requests
import json
import time
import auth


def get_company_enrichment_data(entry, jwt_token):
    """
    Retrieves company enrichment data from the ZoomInfo API using the provided entry and JWT token.

    Args:
        entry (dict): A dictionary containing the company information to be enriched.
        jwt_token (str): A JWT token used for authentication with the ZoomInfo API.

    Returns:
        dict: A dictionary containing the enriched company data, or None if an error occurred.
    """
    
    url = "https://api.zoominfo.com/enrich/company-master" 
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {jwt_token}' 
    }
    
    match_company_input = { 
        "zi_c_name": entry["companyName"],
        "phone": {
            "zi_c_phone": entry["phone"]
        },
        "address": {
            "zi_c_street": entry["companyStreet"],
            "zi_c_city": entry["companyCity"],
            "zi_c_state": entry["companyState"],
            "zi_c_zip": entry["companyZipCode"],
            "zi_c_country": entry["companyCountry"]
        },
        "match_reasons": [{"zi_c_country": "E", "zi_c_name": "F"}] 
    }   
    if entry["emailAddress"]:
        match_company_input["email"] = entry["emailAddress"]
    else:
        pass
    
    payload = {
        "matchCompanyInput": [match_company_input],
        "outputFields": [
            "zi_c_location_id", "zi_c_name", "zi_c_company_name",
            "zi_c_phone", "zi_c_url", "zi_c_company_url", "zi_c_naics6",
            "zi_c_industry_primary", "zi_c_sub_industry_primary", "zi_c_employees",
            "zi_c_street", "zi_c_city", "zi_c_state", "zi_c_zip", "zi_c_country", "zi_c_company_id"
        ]
    }

    response = requests.post(url, headers=headers, json=payload)
    

    if response.status_code != 200: # 200 is the HTTP status code for 'OK'
        print(f"Error: Received status code {response.status_code}")
        print(response.text)
        entry['enrichmentStatus'] = 'Failed'
        entry['errorMessage'] = response.text
        return None

    return response.json()


def update_company_data(entry, new_data_item):
    """
    Updates the company data in the given entry with the new data item.

    Args:
    entry (dict): A dictionary containing the company data to be updated.
    new_data_item (dict): A dictionary containing the new data item to update the company data with.

    Returns:
    dict: The updated company data.
    """
    
    if 'data' in new_data_item and new_data_item['data'].get('result'):
        company_data = new_data_item['data']['result'][0]['data']

        fields_to_update = [
            'zi_c_location_id', 'zi_c_company_name', 'zi_c_phone', 'zi_c_url',
            'zi_c_naics6', 'zi_c_industry_primary', 'zi_c_sub_industry_primary',
            'zi_c_employees', 'zi_c_street', 'zi_c_city', 'zi_c_state',
            'zi_c_zip', 'zi_c_country', 'zi_c_name', 'zi_c_company_id'
        ]

        for field in fields_to_update:
            if entry[field] == '' and field in company_data:
                entry[field] = company_data[field]

    else:
        print("No 'data' key in the response or 'result' list is empty.")

    return entry


def company_enrich(input_filename, jwt_token, last_auth_time, username, password):
    """
    Enriches the company data in the input file using scrict matching.

    Parameters:
    input_filename (str): The path to the input file containing the company data.
    jwt_token (str): The JWT token used for authentication.
    last_auth_time (float): The timestamp of the last authentication.
    username (str): The username for authentication.
    password (str): The password for authentication.

    Returns:
    tuple: A tuple containing the updated JWT token and the updated last authentication time.
    """
    
    with open(input_filename, 'r', encoding='utf-8') as file:
        old_data = json.load(file)

    merged_data = []
    
    for entry in old_data:

        if time.time() - last_auth_time >= 55*60:
            jwt_token = auth.authenticate(username, password)
            last_auth_time = time.time()

        new_data = get_company_enrichment_data(entry, jwt_token)
        if new_data and new_data.get('success'):
            entry = update_company_data(entry, new_data)
        merged_data.append(entry)

    with open(input_filename, 'w', encoding='utf-8') as file:
        json.dump(merged_data, file, indent=4, ensure_ascii=False)

    return jwt_token, last_auth_time


def get_company_enrichment_data_loose(entry, jwt_token):
    """
    Retrieves company enrichment data without location data from the ZoomInfo API using the provided entry and JWT token.

    Args:
        entry (dict): A dictionary containing the company information to be enriched.
        jwt_token (str): A JWT token used for authentication with the ZoomInfo API.

    Returns:
        dict: A dictionary containing the enriched company data, or None if an error occurred.
    """
    
    url = "https://api.zoominfo.com/enrich/company-master" 
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {jwt_token}' 
    }
    
    match_company_input = { 
        "zi_c_name": entry["companyName"],
        "phone": {
            "zi_c_phone": entry["phone"]
        },
        "address": {
            "zi_c_country": entry["companyCountry"]
        },
        "match_reasons": [{"zi_c_country": "E"}] 
    }   
    if entry["emailAddress"]:
        match_company_input["email"] = entry["emailAddress"]
    else:
        pass
    
    payload = {
        "matchCompanyInput": [match_company_input],
        "outputFields": [
            "zi_c_location_id", "zi_c_name", "zi_c_company_name",
            "zi_c_phone", "zi_c_url", "zi_c_company_url", "zi_c_naics6",
            "zi_c_industry_primary", "zi_c_sub_industry_primary", "zi_c_employees",
            "zi_c_street", "zi_c_city", "zi_c_state", "zi_c_zip", "zi_c_country", "zi_c_company_id"
        ]
    }

    response = requests.post(url, headers=headers, json=payload)
    

    if response.status_code != 200: # 200 is the HTTP status code for 'OK'
        print(f"Error: Received status code {response.status_code}")
        print(response.text)
        entry['enrichmentStatus'] = 'Failed'
        entry['errorMessage'] = response.text
        return None

    return response.json()


def update_company_data_loose(entry, new_data_item):
    """
    Updates the company data in the given entry with the new data item.

    Args:
    entry (dict): A dictionary containing the company data to be updated.
    new_data_item (dict): A dictionary containing the new data item to update the company data with.

    Returns:
    dict: The updated company data.
    """
    
    if 'data' in new_data_item and new_data_item['data'].get('result'):
        company_data = new_data_item['data']['result'][0]['data']

        fields_to_update = [
            'zi_c_location_id', 'zi_c_company_name', 'zi_c_phone', 'zi_c_url',
            'zi_c_naics6', 'zi_c_industry_primary', 'zi_c_sub_industry_primary',
            'zi_c_employees', 'zi_c_street', 'zi_c_city', 'zi_c_state',
            'zi_c_zip', 'zi_c_country', 'zi_c_name', 'zi_c_company_id'
        ]

        for field in fields_to_update:
            if entry[field] == '' and field in company_data:
                entry[field] = company_data[field]

    else:
        print("No 'data' key in the response or 'result' list is empty.")

    return entry


def company_enrich_loose(input_filename, jwt_token, last_auth_time, username, password):
    """
    Enriches the company data in the input file using loose matching.

    Args:
        input_filename (str): The path to the input file.
        jwt_token (str): The JWT token for authentication.
        last_auth_time (float): The timestamp of the last authentication.
        username (str): The username for authentication.
        password (str): The password for authentication.

    Returns:
        tuple: A tuple containing the updated JWT token and last authentication time.
    """
    
    with open(input_filename, 'r', encoding='utf-8') as file:
        old_data = json.load(file)

    merged_data = []
    
    for entry in old_data:

        if time.time() - last_auth_time >= 55*60:
            jwt_token = auth.authenticate(username, password)
            last_auth_time = time.time()


        new_data = get_company_enrichment_data_loose(entry, jwt_token)
        if new_data and new_data.get('success'):
            entry = update_company_data_loose(entry, new_data)
        merged_data.append(entry)

    with open(input_filename, 'w', encoding='utf-8') as file:
        json.dump(merged_data, file, indent=4, ensure_ascii=False)

    return jwt_token, last_auth_time
