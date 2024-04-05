from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import pandas as pd

class GoogleDrive():

    def __init__(self, service_account_file: str, folder_name: str) -> None:

        ''' connect '''
        SCOPES = ['https://www.googleapis.com/auth/drive']
        CREDS = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes = SCOPES
        )
        self.service = build('drive', 'v3', credentials= CREDS)

        ''' takes main folder id '''
        items = self.service.files().list(pageSize=1000, 
                                fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)",
                                q = f"mimeType = 'application/vnd.google-apps.folder' and name = '{folder_name}'").execute()
        
        self._main_folder_id = items['files'][0]['id']

    @property
    def main_folder_id(self):
        return self._main_folder_id

    def list_folders_and_files(self, as_df= False) -> dict | pd.DataFrame:

        ''' list files and foldes in maind folder '''
        items = self.service.files().list(pageSize=1000, 
                                    fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)",
                                    q= f"'{self._main_folder_id}' in parents").execute()
        files_and_folders = items.get('files', [])

        if as_df:
            data = []

            for row in files_and_folders:
                # if row["mimeType"] != "application/vnd.google-apps.folder": 
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
        
        return files_and_folders
    
    def create_folder(self, folder_name = 'New Folder') -> str:
        ''' createa a new folder in main folder '''
        folder_metadata = {
            'name': folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            'parents': [self.main_folder_id]
        }

        new_folder_id = self.service.files().create(
            body=folder_metadata,
            fields='id'
        ).execute()

        print(f'Created Folder ID: {new_folder_id["id"]}')

        return new_folder_id
         
    def upload_file(self, file_path: str, file_name = 'New File', folder_id: str = None )-> str:
        
        if folder_id is None:
            folder_id = self._main_folder_id

        file_metadata = {'name': file_name,
                         'parents': [folder_id]}
        media = MediaFileUpload(file_path)
        file = self.service.files().create(body=file_metadata, 
                                           media_body=media, 
                                           fields='id').execute()
        
        print(f'File "{file_name}" uploaded successfully with ID: {file["id"]}')

        return file['id']

