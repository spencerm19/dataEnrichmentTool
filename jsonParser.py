import json


def updateNeedsContact(input_filename):
    """
    Updates the 'needsContact' field in the JSON records based on whether the record has missing contact information.
    If the record has missing contact information, 'needsContact' is set to 'Yes', otherwise it is set to 'No'.

    Args:
    - input_filename (str): The path to the input JSON file.

    Returns:
    - count (int): The number of records with missing contact information.
    """

    with open(input_filename, 'r', encoding='utf-8') as f:
        data = json.load(f)

    count = 0

    for record in data:
        if record['firstName'] == '' and record['lastName'] == '' and record['emailAddress'] == '' and record['phone'] == '':
            record['needsContact'] = 'Yes'
            count += 1
        else:
            record['needsContact'] = 'No'

    with open(input_filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(str(count) + " missing contacts found.")
    return count


def remove_spaces(input_filename):
    """
    Removes leading and trailing spaces from string values in a JSON file.

    Args:
        input_filename (str): The path to the input JSON file.

    Returns:
        None
    """
    with open(input_filename, 'r', encoding='utf-8') as f:
        data = json.load(f)

    for record in data:
        for key, value in record.items():
            # If the value is a string, remove leading and trailing spaces and replace multiple spaces with a single space
            if isinstance(value, str):
                record[key] = ' '.join(value.strip().split())

    with open(input_filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    

def update_address(input_filename):
    """
    Update the address fields of each entry in the JSON file located at input_filename.
    If the companyStreet, companyCity, companyState, and companyZipCode fields are all missing,
    they will be updated with the values of zi_c_street, zi_c_city, zi_c_state, and zi_c_zip respectively.
    """
    
    with open(input_filename, 'r', encoding='utf-8') as file:
        data = json.load(file)

    for entry in data:
        if (not entry.get('companyStreet') and not entry.get('companyCity') 
                and not entry.get('companyState') and not entry.get('companyZipCode')):
            entry['companyStreet'] = entry.get('zi_c_street', '')
            entry['companyCity'] = entry.get('zi_c_city', '')
            entry['companyState'] = entry.get('zi_c_state', '')
            entry['companyZipCode'] = entry.get('zi_c_zip', '')

    with open(input_filename, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
