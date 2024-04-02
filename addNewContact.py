import requests
import json
import time
import auth


def get_new_contact_data(entry, jwt_token):
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
        "Content-Type": "application/json",
        "Authorization": f"Bearer {jwt_token}",
    }

    match_person_input = {"personId": entry["personId"]}

    payload = {
        "matchPersonInput": [match_person_input],
        "outputFields": ["firstName", "lastName", "email", "phone", "jobTitle"],
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code != 200:  # 200 is the HTTP status code for 'OK'
        print(f"Error: Received status code {response.status_code}")
        print(response.text)
        entry["enrichmentStatus"] = "Failed"
        entry["errorMessage"] = response.text
        return None

    return response.json()


def update_new_contact_data(entry, new_data_item):
    """
    Updates an existing contact entry with new data from the Zoominfo API if the existing data is missing.

    Args:
        entry (dict): The existing contact entry to update.
        new_data_item (dict): The new data item from the Zoominfo API.

    Returns:
        dict: The updated contact entry.
    """

    person_data = new_data_item["data"][0]

    if not entry["firstName"] and person_data["firstName"]:
        entry["firstName"] = person_data["firstName"]
    if not entry["lastName"] and person_data["lastName"]:
        entry["lastName"] = person_data["lastName"]
    if not entry["emailAddress"] and person_data["email"]:
        entry["emailAddress"] = person_data["email"]
    if not entry["phone"] and person_data["phone"]:
        entry["phone"] = person_data["phone"]
    if not entry["jobTitle"] and person_data["jobTitle"]:
        entry["jobTitle"] = person_data["jobTitle"]

    return entry


def add_new_contact(input_filename, jwt_token, last_auth_time, username, password):
    """
    Adds new contact data to the existing data in the input file.

    Args:
        input_filename (str): The path to the input file.
        jwt_token (str): The JWT token for authentication.
        last_auth_time (float): The timestamp of the last authentication.
        username (str): The username for authentication.
        password (str): The password for authentication.

    Returns:
        tuple: A tuple containing the updated JWT token and last authentication time.
    """

    with open(input_filename, "r", encoding="utf-8") as file:
        old_data = json.load(file)

    merged_data = []
    processed_records = 0

    for entry in old_data:

        if time.time() - last_auth_time >= 55 * 60:
            jwt_token = auth.authenticate(username, password)
            last_auth_time = time.time()

        try:
            if entry.get("needsContact") == "Yes" and entry.get("personId"):
                new_data = get_new_contact_data(entry, jwt_token)
                if new_data and new_data.get("success"):
                    entry = update_new_contact_data(
                        entry, new_data["data"]["result"][0]
                    )
                    processed_records += 1
                    print(
                        f"\rContacts updated: {processed_records}", end="", flush=True
                    )

        except IndexError as e:
            print(f"Error processing record: {entry}. Error: {e}")

        merged_data.append(entry)

    with open(input_filename, "w", encoding="utf-8") as file:
        json.dump(merged_data, file, indent=4, ensure_ascii=False)

    return jwt_token, last_auth_time
