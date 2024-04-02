import requests
import json
import time
import auth


def get_company_enrichment_data(entry, jwt_token, strict):
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
        "Content-Type": "application/json",
        "Authorization": f"Bearer {jwt_token}",
    }

    def create_payload(include_email):

        payload = {
            "matchCompanyInput": [
                {
                    "zi_c_name": entry["companyName"],
                    "phone": {"zi_c_phone": entry["phone"]},
                    "address": {"zi_c_country": entry["companyCountry"]},
                    "match_reasons": [{"zi_c_country": "E"}],
                }
            ],
            "outputFields": [
                "zi_c_location_id",
                "zi_c_name",
                "zi_c_company_name",
                "zi_c_phone",
                "zi_c_url",
                "zi_c_company_url",
                "zi_c_naics6",
                "zi_c_employees",
                "zi_c_street",
                "zi_c_city",
                "zi_c_state",
                "zi_c_zip",
                "zi_c_country",
                "zi_c_company_id",
                "zi_c_linkedin_url",
            ],
        }
        if include_email and entry.get("emailAddress"):
            payload["matchCompanyInput"][0]["email"] = entry["emailAddress"]

        if strict:
            updated_address = {
                "zi_c_street": entry["companyStreet"],
                "zi_c_city": entry["companyCity"],
                "zi_c_state": entry["companyState"],
                "zi_c_zip": entry["companyZipCode"],
            }
            payload["matchCompanyInput"][0]["address"].update(updated_address)
            payload["matchCompanyInput"][0]["match_reasons"] = [
                {"zi_c_country": "E", "zi_c_name": "F"}
            ]

        return payload

    try:
        payload = create_payload(include_email=True)
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raises an exception for 4XX and 5XX status codes

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            payload = create_payload(include_email=False)
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
        else:
            entry["enrichmentStatus"] = "Failed"
            entry["errorMessage"] = str(e)
            return None

    except requests.exceptions.RequestException as e:
        entry["enrichmentStatus"] = "Failed"
        entry["errorMessage"] = str(e)
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

    if "data" in new_data_item and new_data_item["data"].get("result"):
        company_data = new_data_item["data"]["result"][0]["data"]

        fields_to_update = [
            "zi_c_location_id",
            "zi_c_company_name",
            "zi_c_phone",
            "zi_c_url",
            "zi_c_naics6",
            "zi_c_employees",
            "zi_c_street",
            "zi_c_city",
            "zi_c_state",
            "zi_c_zip",
            "zi_c_country",
            "zi_c_name",
            "zi_c_company_id",
            "zi_c_linkedin_url",
        ]

        for field in fields_to_update:
            if entry[field] == "" and field in company_data:
                entry[field] = company_data[field]

    else:
        print("No 'data' key in the response or 'result' list is empty.")

    return entry


def company_enrich(input_filename, jwt_token, last_auth_time, username, password):
    """
    Enriches company data in the input file using the provided JWT token and authentication credentials.

    Args:
        input_filename (str): The path to the input file containing the company data.
        jwt_token (str): The JWT token for authentication.
        last_auth_time (float): The timestamp of the last authentication.
        username (str): The username for authentication.
        password (str): The password for authentication.

    Returns:
        tuple: A tuple containing the updated JWT token and the timestamp of the last authentication.
    """

    with open(input_filename, "r", encoding="utf-8") as file:
        old_data = json.load(file)

    merged_data = []
    companies_processed = 0

    for entry in old_data:

        if time.time() - last_auth_time >= 55 * 60:
            jwt_token = auth.authenticate(username, password)
            last_auth_time = time.time()

        entry["company_match_criteria"] = "None"

        new_data = get_company_enrichment_data(entry, jwt_token, strict=True)
        if (
            new_data
            and new_data.get("success")
            and new_data["data"].get("result")
            and new_data["data"]["result"][0].get("data")
        ):
            entry["company_match_criteria"] = "Strict"
        else:
            new_data = get_company_enrichment_data(entry, jwt_token, strict=False)
            if new_data and new_data.get("success") and new_data["data"].get("result"):
                entry["company_match_criteria"] = "Non-strict"

        if new_data and new_data.get("success") and new_data["data"].get("result"):
            entry = update_company_data(entry, new_data)

        merged_data.append(entry)

        companies_processed += 1
        print(f"\rCompanies processed: {companies_processed}", end="", flush=True)

    with open(input_filename, "w", encoding="utf-8") as file:
        json.dump(merged_data, file, indent=4, ensure_ascii=False)

    return jwt_token, last_auth_time
