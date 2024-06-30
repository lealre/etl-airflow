import io

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload


class GoogleDrive():

    def __init__(self, service_account_file: str, folder_name: str) -> None:

        """ connect """
        SCOPES = ['https://www.googleapis.com/auth/drive']
        CREDS = service_account.Credentials.from_service_account_file(
            service_account_file,
            scopes=SCOPES
        )
        self.service = build('drive', 'v3', credentials=CREDS)

        """ takes main folder id """
        items = self.service.files().list(
            pageSize=1000,
            fields=(
                "nextPageToken, "
                "files(id, name, mimeType, size, modifiedTime)"
            ),
            q=(
                "mimeType = "
                "'application/vnd.google-apps.folder' and "
                f"name = '{folder_name}'"
            )
        ).execute()

        self._main_folder_id = items['files'][0]['id']

    @property
    def main_folder_id(self):
        return self._main_folder_id

    def list_folders_and_files(
        self, as_df=False, folder_id: str = None
    ) -> dict | pd.DataFrame:
        """ list files and foldes in a folder """

        if folder_id is None:
            folder_id = self._main_folder_id

        items = self.service.files().list(
            pageSize=1000,
            fields=(
                "nextPageToken, files(id, name, mimeType, size, modifiedTime)"
            ),
            q=f"'{folder_id}' in parents"
        ).execute()

        files_and_folders = items.get('files', [])

        if as_df:
            data = []

            for row in files_and_folders:
                # if row["mimeType"] != "application/vnd.google-apps.folder":
                row_data = []
                try:
                    row_data.append(round(int(row["size"]) / 1000000, 2))
                except KeyError:
                    row_data.append(0.00)
                row_data.append(row["id"])
                row_data.append(row["name"])
                row_data.append(row["modifiedTime"])
                row_data.append(row["mimeType"])
                data.append(row_data)

            cleared_df = pd.DataFrame(
                data,
                columns=[
                    'size_in_MB',
                    'id',
                    'name',
                    'last_modification',
                    'type_of_file'
                ]
            )

            return cleared_df

        return files_and_folders

    def id_folders(self) -> dict[str, str]:
        all_directories = self.list_folders_and_files(as_df=True)

        folder_type = 'application/vnd.google-apps.folder'
        folders_id = (
            all_directories
            .loc[all_directories['type_of_file'] == folder_type,
                 ['id', 'name']
            ]
        )
        folders_id = folders_id.set_index('name')['id'].to_dict()

        return folders_id

    def create_folder(self, folder_name='New Folder') -> str:
        """ createa a new folder in main folder """

        folder_metadata = {
            'name': folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            'parents': [self.main_folder_id]
        }

        if folder_name not in list(self.id_folders().keys()):
            new_folder = self.service.files().create(
                body=folder_metadata,
                fields='id'
                ).execute()
            print(f'Created Folder ID: {new_folder["id"]}')
            return new_folder['id']

        print('Folder already exists.')
        folder_id = self.id_folders[folder_name]
        return folder_id

    def upload_file(
            self, file_path: str, file_name='New File', folder_id: str = None
        ) -> str:

        if folder_id is None:
            folder_id = self._main_folder_id

        file_metadata = {'name': file_name,
                         'parents': [folder_id]}
        media = MediaFileUpload(file_path)
        file = self.service.files().create(body=file_metadata,
                                           media_body=media,
                                           fields='id').execute()

        print(
            f"File '{file_name}'"
            f"uploaded successfully with ID: {file['id']}"
        )

        return file['id']

    def get_csv_files(self, folder_id: str) -> pd.DataFrame:

        df_csv = (
            self.list_folders_and_files(
                as_df=True,
                folder_id=folder_id)
                .loc[:, ['name', 'type_of_file', 'id']]
        )

        return df_csv.loc[df_csv['name'].str.endswith('.csv'), :]

    def read_csv_from_drive(self, file_id: str) -> pd.DataFrame:
        """ reads a csv from google drive """
        try:
            request_file = self.service.files().get_media(fileId=file_id)
            file = io.BytesIO()
            downloader = MediaIoBaseDownload(file, request_file)
            done = False
            while done is False:
                _, done = downloader.next_chunk()
                # print(F'Download {int(status.progress() * 100)}.')
        except HttpError as error:
            print(F'An error occurred: {error}')

        file.seek(0)  # Reset the file pointer to the beginning
        df = pd.read_csv(file)

        # # to download it
        # file_retrieved: str = file.getvalue()
        # with open(f"downloaded_file.csv", 'wb') as f:
        #     f.write(file_retrieved)

        return df
