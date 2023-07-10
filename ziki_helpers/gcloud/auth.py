import os
import json

from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

import gspread
import pydrive
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

from ziki_helpers.aws.s3 import read_from_s3, write_to_s3


CONFIG_S3_BUCKET = 'ziki-analytics-config'


def get_authenticated_pydrive_client() -> pydrive.drive.GoogleDrive:
    """
    Authenticates pydrive access to Google Drive.
    Requires 'pydrive-credentials.json' to be in the S3 bucket: 'ziki-analytics-config'.

    :return: Authenticated GoogleDrive
    """
    # https://stackoverflow.com/questions/24419188/automating-pydrive-verification-process
    cred_file_name = 'pydrive-credentials.json'

    credentials_data = read_from_s3(CONFIG_S3_BUCKET, cred_file_name)

    # Save credentials to a temporary file
    temp_credentials_path = '/tmp/credentials.json'  # Path to a temporary file
    with open(temp_credentials_path, 'wb') as temp_file:
        temp_file.write(credentials_data.encode())

    gauth = GoogleAuth()
    # Try to load saved client credentials
    gauth.LoadCredentialsFile(temp_credentials_path)

    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
        #gauth.LocalWebserverAuth()  # If refresh fails, then do the webserver auth
    else:
        # Initialize the saved creds
        gauth.Authorize()

    # Save the current credentials to the temporary file
    gauth.SaveCredentialsFile(temp_credentials_path)

    # Write the credentials to S3
    with open(temp_credentials_path, 'rb') as temp_file:
        temp_file_content = temp_file.read().decode('utf-8')
    write_to_s3(CONFIG_S3_BUCKET, cred_file_name, temp_file_content)

    # Remove the temporary file
    os.remove(temp_credentials_path)

    drive = GoogleDrive(gauth)

    return drive


def get_authenticated_gspread_client() -> gspread.Client:
    """
    Authenticates gspread access using service account credentials.
    Accepts the credentials as a dictionary.

    :param credentials_data: Service account credentials as a dictionary
    :return: Authenticated gspread Client
    """
    credentials_data = json.loads(
        read_from_s3(CONFIG_S3_BUCKET, 'gspread-creds.json')
    )

    gc = gspread.service_account_from_dict(credentials_data)
    return gc


def get_authenticated_google_credentials():

    # Authorize drive, sheets, & slides
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/presentations",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/gmail.send"
    ]

    auth_creds_file_name = 'google-auth-credential-sample.json'
    google_client_secrets_file_name = 'google-client-secrets.json'

    auth_creds = json.loads(
        read_from_s3(CONFIG_S3_BUCKET, auth_creds_file_name)
    )
    google_client_secrets = json.loads(
        read_from_s3(CONFIG_S3_BUCKET, google_client_secrets_file_name)
    )

    # Store auth_creds locally
    temp_auth_creds_path = '/tmp/google-auth-credential-sample.json'
    with open(temp_auth_creds_path, 'w') as temp_file:
        json.dump(auth_creds, temp_file)

    store = Storage(temp_auth_creds_path)
    credentials = store.get()

    # Delete temporary auth_creds file
    os.remove(temp_auth_creds_path)

    if not credentials or credentials.invalid or credentials.access_token_expired:
        # Store secrets locally
        temp_secrets_path = '/tmp/google-client-secrets.json'
        with open(temp_secrets_path, 'w') as temp_file:
            json.dump(google_client_secrets, temp_file)
        flow = client.flow_from_clientsecrets(temp_secrets_path, scopes)
        credentials = tools.run_flow(flow, store)
        # Delete temporary auth_creds file
        os.remove(temp_auth_creds_path)
        write_to_s3(CONFIG_S3_BUCKET, auth_creds_file_name, credentials.to_json())

    return credentials


if __name__ == '__main__':
    print(get_authenticated_google_credentials())