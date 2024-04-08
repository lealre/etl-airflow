import pandas as pd
import pandera as pa
from sqlalchemy import create_engine
import json
from dotenv import load_dotenv
import os
from .google_drive import GoogleDrive
from .schema import CompanyRevenue 



def extract_files(service_account_path: str, parent_folder_name: str, folder_to_extract: str) -> list[pd.DataFrame]:
    
    # connect with google drive
    drive_conn = GoogleDrive(service_account_file= service_account_path,
                            folder_name= parent_folder_name)
    
    # get folder id
    folders_id = drive_conn.id_folders()
    invoices_folder_id = folders_id[folder_to_extract]

    # get id from directories
    files_info = drive_conn.list_folders_and_files(as_df= True, folder_id = invoices_folder_id)
    files_info = files_info[['name', 'type_of_file', 'id']]

    list_df = []
    
    for index, row in files_info.iterrows():

        df_raw = drive_conn.read_csv_from_drive(file_id=row['id'])
        file_name = row['name']
        print(f'File {file_name} extracted.')   

        try:
            df_validated = validate_schema_files(df_raw)
            list_df.append(df_validated)
            print(f'File {file_name} validated!')
        except pa.errors.SchemaError as e:
            print(f"Error in {file_name}:")
            # print(json.dumps(e.message, indent=4))
            print(e)
        
    return list_df

@pa.check_input(CompanyRevenue, lazy = False)
def validate_schema_files(df: pd.DataFrame) -> pd.DataFrame:
    return df


def load_files(list_df: list[pd.DataFrame]) -> None:
    
    load_dotenv(".env")

    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT')
    POSTGRES_DB = os.getenv('POSTGRES_DB')

    POSTGRES_DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    engine = create_engine(POSTGRES_DATABASE_URL)

    print('Loading files...')
    for df in list_df:
        # Save to database
        df.to_sql('invoices_table', con=engine, if_exists='append', index=False)
        print('Loaded in:', df['Currency'][0])
    
    print('Files loaded!')

def pipeline(service_account_path: str, parent_folder_name: str, folder_to_extract: str) -> None:

    list_df = extract_files(service_account_path, parent_folder_name, folder_to_extract)

    # load_files(list_df)
    
    print(list_df[0].info()) 





