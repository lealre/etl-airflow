from drive_connections import GoogleDrive
import os

service_account_file = 'service_account.json'
main_folder = 'python_to_drive'


drive_conn = GoogleDrive(service_account_file= service_account_file,
                         folder_name= main_folder)

def upload_files(folder_id: str):
    data_folder_path = 'data'
    folder_content = os.listdir(data_folder_path)

    for file in folder_content:
        file_path = os.path.join(data_folder_path, file)
        drive_conn.upload_file(file_name = file, folder_id = folder_id, file_path= file_path)


# print(drive_conn.list_folders_and_files(as_df=True))


new_folder_name = 'Companies Invoicing'
folder_id = drive_conn.create_folder(new_folder_name)
upload_files(folder_id)
