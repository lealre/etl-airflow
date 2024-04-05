from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd

def authenticate_and_connect(service_account_file):
    """
    Authenticate and connect to the Google Drive API.

    Parameters:
        service_account_file (str): Path to the service account JSON file containing
            Google API credentials.

    Returns:
        Resource: An instance of the Google Drive API service for interacting with
            Google Drive.
    
    Raises:
        FileNotFoundError: If the provided service account file is not found.
        google.auth.exceptions.DefaultCredentialsError: If the credentials cannot
            be loaded from the provided service account file.
        google.auth.exceptions.RefreshError: If the provided credentials are invalid
            or expired.
    """
    SCOPES = ['https://www.googleapis.com/auth/drive']
    creds = service_account.Credentials.from_service_account_file(
        service_account_file,
        scopes = SCOPES
    )
    service = build('drive', 'v3', credentials= creds)

    return service


def get_main_folder_id(service, folder_name):
    """
    Retrieve the ID of the main folder with the specified name in Google Drive.

    Parameters:
        service (Resource): An instance of the Google Drive API service.
        folder_name (str): The name of the main folder to retrieve the ID for.

    Returns:
        str: The ID of the main folder with the specified name.

    Raises:
        IndexError: If no folder with the specified name is found in Google Drive.
        googleapiclient.errors.HttpError: If an error occurs while executing the API request.
    """

    items = service.files().list(pageSize=1000, 
                            fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)",
                            q = f"mimeType = 'application/vnd.google-apps.folder' and name = '{folder_name}'").execute()
    
    main_folder_id = items['files'][0]['id']

    return main_folder_id


def list_folders_and_files(service, folder_id):
    """
    List all folders and files within the specified folder in Google Drive.

    Parameters:
        service (Resource): An instance of the Google Drive API service.
        folder_id (str): The ID of the folder to list files and folders from.

    Returns:
        list: A list containing dictionaries representing files and folders within the specified folder.
            Each dictionary contains metadata such as file ID, name, MIME type, size, and modification time.
    
    Raises:
        googleapiclient.errors.HttpError: If an error occurs while executing the API request.
    """

    items = service.files().list(pageSize=1000, 
                                fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)",
                                q= f"'{folder_id}' in parents").execute()
    list_files_and_folders = items.get('files', [])

    return list_files_and_folders

def transform_files_list_in_dataframe(files_dict):
    """
    Transform a list of file metadata dictionaries into a Pandas DataFrame.

    Parameters:
        files_dict (list): A list containing dictionaries representing file metadata.
            Each dictionary should contain metadata such as file ID, name, MIME type,
            size, and modification time.

    Returns:
        DataFrame: A Pandas DataFrame containing the transformed file metadata.
            The DataFrame has columns for file size (in MB), file ID, name, last modification
            time, and type of file.

    Notes:
        The file size is converted from bytes to megabytes (MB) and rounded to two decimal places.
        If the size metadata is missing for a file, it is treated as 0 MB.
    """

    data = []

    for row in files_dict:
        if row["mimeType"] != "application/vnd.google-apps.folder": 
            row_data = []
            try:
                row_data.append(round(int(row["size"])/1000000, 2))
            except KeyError:
                row_data.append(0.00)
            row_data.append(row["id"])
            row_data.append(row["name"])
            row_data.append(row["modifiedTime"])
            row_data.append(row["mimeType"])
            data.append(row_data)

    cleared_df = pd.DataFrame(data, columns = ['size_in_MB', 'id', 'name', 'last_modification', 'type_of_file'])

    return cleared_df
     


if __name__ == '__main__':

    folder_name = 'test driver'

    service_account_file = 'service_account.json'

    service = authenticate_and_connect(service_account_file)
    main_folder_id = get_main_folder_id(service, folder_name)
    list_dir_and_files = list_folders_and_files(service, main_folder_id)
    df_files = transform_files_list_in_dataframe(list_dir_and_files)

    print(df_files)
