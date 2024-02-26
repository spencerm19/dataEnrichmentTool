import requests
import json
import time
import auth


def get_contact_person_id(entry, jwt_token, strict, use_location_id):
    """
    Retrieves the contact person ID based on the provided entry, JWT token, strict flag, and use_location_id flag.

    Parameters:
    - entry (dict): The entry containing the identifiers for the contact search.
    - jwt_token (str): The JWT token for authentication.
    - strict (bool): Flag indicating whether to apply strict search criteria.
    - use_location_id (bool): Flag indicating whether to use location ID for the search.

    Returns:
    - str or None: The contact person ID if found, or None if not found.
    """

    url = "https://api.zoominfo.com/search/contact"

    if not entry.get("zi_c_company_id") and not entry.get("zi_c_location_id"):
        return None

    payload = {
        "requiredFields": "email, phone",
        "sortBy": "hierarchy",
        "rpp": 1,  # 'Results Per Page', adjust as needed
        "page": 1,
    }

    if use_location_id:
        if entry.get("zi_c_location_id"):
            payload["locationCompanyId"] = [(entry["zi_c_location_id"])]

    else:
        if entry.get("zi_c_company_id"):
            payload["companyId"] = str(entry["zi_c_company_id"])

    if strict:
        payload.update(
            {
                "managementLevel": "C Level Exec, VP Level Exec, Director, Manager",
                "department": "C-Suite, Operations, Marketing, Engineering & Technical",
            }
        )

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {jwt_token}",
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code}")
        print(response.text)
        entry["enrichmentStatus"] = "Failed"
        entry["errorMessage"] = response.text
        return None

    response_data = response.json()

    if response_data.get("data") and response_data["data"][0].get("id"):
        return response_data["data"][0]["id"]
    else:
        return None


def contact_search(input_filename, jwt_token, last_auth_time, username, password):
    """
    Search for contacts in the given input file and enrich the data with contact information.

    Parameters:
    input_filename (str): The path to the input file containing the data to be processed.
    jwt_token (str): The JWT token for authentication.
    last_auth_time (float): The timestamp of the last authentication.
    username (str): The username for authentication.
    password (str): The password for authentication.

    Returns:
    tuple: A tuple containing the updated JWT token and last authentication time.
    """

    with open(input_filename, "r", encoding="utf-8") as file:
        data = json.load(file)

    processed_records = 0
    person_id_count = 0

    for entry in data:
        if time.time() - last_auth_time >= 55 * 60:
            jwt_token = auth.authenticate(username, password)
            last_auth_time = time.time()

        if entry.get("needsContact") == "Yes":
            person_id = None

            if entry.get("zi_c_location_id"):
                person_id = get_contact_person_id(
                    entry, jwt_token, strict=True, use_location_id=True
                )
                if person_id:
                    entry["personId"] = person_id
                    entry["contactMatchCriteria"] = "locationId_strict"
                    person_id_count += 1
                else:
                    person_id = get_contact_person_id(
                        entry, jwt_token, strict=False, use_location_id=True
                    )
                    if person_id:
                        entry["personId"] = person_id
                        entry["contactMatchCriteria"] = "locationId_loose"
                        person_id_count += 1

            if person_id is None and entry.get("zi_c_company_id"):
                person_id = get_contact_person_id(
                    entry, jwt_token, strict=True, use_location_id=False
                )
                if person_id:
                    entry["personId"] = person_id
                    entry["contactMatchCriteria"] = "companyId_strict"
                    person_id_count += 1
                else:
                    person_id = get_contact_person_id(
                        entry, jwt_token, strict=False, use_location_id=False
                    )
                    if person_id:
                        entry["personId"] = person_id
                        entry["contactMatchCriteria"] = "companyId_loose"
                        person_id_count += 1

            entry["newContactFound"] = "Yes" if person_id else "No"

            processed_records += 1
            print(f"\rProcessed records: {processed_records}", end="", flush=True)

    print(f"\nTotal contact's found: {person_id_count}")

    with open(input_filename, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

    return jwt_token, last_auth_time
