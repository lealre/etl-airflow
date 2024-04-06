from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
from google_drive import GoogleDrive

load_dotenv(".env")

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')

POSTGRES_DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_engine(POSTGRES_DATABASE_URL)

# connect with googel drive
service_account_file = 'service_account.json'
main_folder = 'python_to_drive'
drive_conn = GoogleDrive(service_account_file= service_account_file,
                         folder_name= main_folder)

# get folder id
invoices_folder_name = 'Companies Invoicing'
folders_id = drive_conn.id_folders()
invoices_folder_id = folders_id[invoices_folder_name]

# get id from directories
files_info = drive_conn.list_folders_and_files(as_df= True, folder_id = invoices_folder_id)
files_info = files_info[['name', 'type_of_file', 'id']]

for id in files_info['id']:
    # read csv file into dataframe
    df = drive_conn.read_csv_from_drive(file_id=id)
    print(df)

    # Save to database
    df.to_sql('invoices_table', con=engine, if_exists='append', index=False)


