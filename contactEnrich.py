import requests
import json
import time
import auth


def get_contact_enrichment_data(entry, jwt_token):
    """
    Enriches contact data using the Zoominfo API.

    Constructs a request to the Zoominfo API using contact entry,
    and attempts to enrich the provided contact information. The function handles API response
    and returns enriched data if successful, or None if there's an error.

    Args:
        entry (dict): A dictionary containing contact information.
        jwt_token (str): A JWT token for authentication with the Zoominfo API.

    Returns:
        dict or None: A dictionary containing enriched contact information, or None if there was an error.
    """
    
    url = "https://api.zoominfo.com/enrich/contact"
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {jwt_token}'
    }

    match_person_input = {
        "companyName": entry["companyName"],
        "firstName": entry["firstName"],
        "lastName": entry["lastName"],
        "emailAddress": entry["emailAddress"],
        "phone": entry["phone"]
    }

    payload = {
        "matchPersonInput": [match_person_input],
        "outputFields": ["firstName", "lastName", "email", "phone"]
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code}")
        print(response.text)
        entry['enrichmentStatus'] = 'Failed'
        entry['errorMessage'] = response.text
        return None

    return response.json()

    
    
def update_contact_data(entry, new_data_item):
    """
    Updates an existing contact entry with new data from the Zoominfo API.

    Takes an original contact entry and new data and updates the contact entry with
    information from the new data only if the corresponding fields in the original entry
    are empty or missing. The function doesn't overwrite any existing information in the entry.

    Args:
        entry (dict): A dictionary representing the original contact entry.
        new_data_item (dict): A dictionary containing new data from the Zoominfo API.

    Returns:
        dict: A dictionary representing the updated contact entry.
    """
    
    person_data = new_data_item['data'][0]

    if not entry['firstName'] and person_data['firstName']:
        entry['firstName'] = person_data['firstName']
    if not entry['lastName'] and person_data['lastName']:
        entry['lastName'] = person_data['lastName']
    if not entry['emailAddress'] and person_data['email']:
        entry['emailAddress'] = person_data['email']
    if not entry['phone'] and person_data['phone']:
        entry['phone'] = person_data['phone']

    return entry



def contact_enrich(input_filename, jwt_token, last_auth_time, username, password):
    """
    Enriches the contact data in the input file.

    Args:
        input_filename (str): The path to the input file containing the contact data.
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


        new_data = get_contact_enrichment_data(entry, jwt_token)
        if new_data.get('success') and new_data['data']['result'][0]['matchStatus'] in ["CONTACT_ONLY_MATCH", "FULL_MATCH"]:
            entry = update_contact_data(entry, new_data['data']['result'][0])
        merged_data.append(entry)

    with open(input_filename, 'w', encoding='utf-8') as file:
        json.dump(merged_data, file, indent=4, ensure_ascii=False)

    return jwt_token, last_auth_time
