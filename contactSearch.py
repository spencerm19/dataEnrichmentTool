import requests
import json
import time
import auth
 
def get_contact_person_id_strict(entry, jwt_token):
    """
    Fetches the contact person ID using the Zoominfo API. This function uses department and managmentLevel as additional
    matching criteria.

    Args:
        entry (dict): A dictionary containing the company ID.
        jwt_token (str): A JWT token for authentication.

    Returns:
        str: The contact person ID, or None if not found.
    """
    
    url = "https://api.zoominfo.com/search/contact"

    if not entry.get("zi_c_company_id"):
        return None

    # Ensure companyId is a string
    company_id_str = str(entry["zi_c_company_id"])

    payload = json.dumps({
        "companyId": company_id_str,
        "requiredFields": 'email, phone',
        "managementLevel": 'C Level Exec, VP Level Exec, Director, Manager',
        "department": 'C-Suite, Operations, Marketing, Engineering & Technical',
        "sortBy": 'hierarchy', # newly added check for error
        "rpp": 1,  # 'Results Per Page', increase or decrease as needed
        "page": 1
    })
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {jwt_token}'
    }

    response = requests.post(url, headers=headers, data=payload)
    
    if response.status_code != 200: # 200 is the HTTP status code for 'OK'
        print(f"Error: Received status code {response.status_code}")
        print(response.text)
        entry['enrichmentStatus'] = 'Failed'
        entry['errorMessage'] = response.text
        return None

    response_data = response.json()
      
    # Check if the response contains data and a contact person ID
    if response_data.get('data') and response_data['data'][0].get('id'):
        return response_data['data'][0]['id']
      
    else:
        return None
    
      
def get_contact_person_id_loose(entry, jwt_token):
    """
    Searches for a contact person associated with a given company ID using the ZoomInfo API. This function does not use
    additional matching criteria.
    
    Args:
    entry (dict): A dictionary containing information about the company.
    jwt_token (str): A JWT token used for authentication with the ZoomInfo API.
    
    Returns:
    str: The ID of the contact person associated with the given company ID, or None if no contact person is found.
    """
    
    url = "https://api.zoominfo.com/search/contact"
    
    if not entry.get("zi_c_company_id"):
       # print("Error: 'zi_c_company_id' is empty.")
        return None
    
    # Ensure companyId is a string
    company_id_str = str(entry["zi_c_company_id"])

    payload = json.dumps({
        "companyId": company_id_str,
        "requiredFields": 'email, phone',
        "sortBy": 'hierarchy',
        "rpp": 1,  # 'Results Per Page', increase or decrease as needed
        "page": 1
    })
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {jwt_token}'
    }

    response = requests.post(url, headers=headers, data=payload)
    
    if response.status_code != 200: # 200 is the HTTP status code for 'OK'
        print(f"Error: Received status code {response.status_code}")
        print(response.text)
        entry['enrichmentStatus'] = 'Failed'
        entry['errorMessage'] = response.text
        return None

    response_data = response.json()
      
    # Check if the response contains data and a contact person ID
    if response_data.get('data') and response_data['data'][0].get('id'):
        return response_data['data'][0]['id']
      
    else:
        return None


def contact_search_strict(input_filename, jwt_token, last_auth_time, username, password):
    """
    Searches for new contacts using strict matching criteria and updates personId.

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
        data = json.load(file)

    for entry in data:
        if time.time() - last_auth_time >= 55*60:
            jwt_token = auth.authenticate(username, password)
            last_auth_time = time.time()

        if entry.get('needsContact') == 'Yes' and 'zi_c_company_id' in entry:
            person_id = get_contact_person_id_strict(entry, jwt_token)
            
            if person_id is not None:
                entry['personId'] = person_id
                entry['newContactFound'] = 'Yes'
                entry['contactMeetsCriteria'] = 'Yes'
            else:
                entry['newContactFound'] = 'No'
    else:
        pass

    with open(input_filename, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

    return jwt_token, last_auth_time
  
        
def contact_search_loose(input_filename, jwt_token, last_auth_time, username, password):
    """
    Searches for new contacts using loose matching criteria and updates personId.

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
        data = json.load(file)

    for entry in data:

        if time.time() - last_auth_time >= 55*60:
            jwt_token = auth.authenticate(username, password)
            last_auth_time = time.time()

        if entry.get('needsContact') == 'Yes' and entry.get('newContactFound') == 'No' and 'zi_c_company_id' in entry:
            person_id = get_contact_person_id_loose(entry, jwt_token)
            
            if person_id is not None:
                entry['personId'] = person_id
                entry['newContactFound'] = 'Yes'
                entry['contactMeetsCriteria'] = 'No'
            else:
                entry['newContactFound'] = 'No'
    else:
        pass
    
    with open(input_filename, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

    return jwt_token, last_auth_time
