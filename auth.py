import requests
import json
import getpass


def get_login_credentials():
    """
    Gets the login credentials from the user.

    :return: tuple, a tuple containing the username and password
    """

    while True:
        username = input("Enter your Zoominfo username: ")
        password = getpass.getpass("Enter your Zoominfo password: ")

        if authenticate(username, password):
            print("User authenticated.\n")
            return (username, password)
        else:
            print("Invalid login. Please try again.")


def authenticate(username, password):
    """
    Authenticates the user and generates an JWT Token to grant access to the Zoominfo API.

    :param username: str, the username of the user
    :param password: str, the password of the user
    :return: str, the JWT token if authentication is successful, None otherwise
    """

    url = "https://api.zoominfo.com/authenticate"

    payload = json.dumps({"username": username, "password": password})

    headers = {"Content-Type": "application/json"}

    response = requests.request("POST", url, headers=headers, data=payload)

    # if the response status code is 200, returns the JWT token
    if response.status_code == 200:  # 200 is the HTTP status code for 'OK'
        response_data = response.json()
        return response_data["jwt"]
    else:
        return None
