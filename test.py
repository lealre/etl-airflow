from drive_connections import GoogleDrive
from googleapiclient.http import MediaFileUpload

from google.oauth2 import service_account
from googleapiclient.discovery import build

service_account_file = 'service_account.json'
# folder_name = 'python_to_drive'

# drive_conn = GoogleDrive(service_account_file= service_account_file,
#                          folder_name= folder_name)

# file_name = 'novo.csv'
# file_path = 'test.txt'


# print(drive_conn.list_folders_and_files(as_df = True))


''' connect '''
SCOPES = ['https://www.googleapis.com/auth/drive']
CREDS = service_account.Credentials.from_service_account_file(
    service_account_file,
    scopes = SCOPES
)
service = build('drive', 'v3', credentials= CREDS)

