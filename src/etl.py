import pandas as pd
import pandera as pa
from typing import Union
from .google_drive import GoogleDrive
from .database import engine, get_files_ids_from_db
from .schema import CompanyRevenue 
from controller import transform_data

def extract_files(service_account_path: str, parent_folder_name: str, folder_to_extract: str) -> Union[list[pd.DataFrame], list[None]]:
    
    list_df: list[pd.DataFrame] = []

    drive_conn = GoogleDrive(service_account_file= service_account_path,
                            folder_name= parent_folder_name)
    
    try:
        revenue_folder_id = drive_conn.id_folders()[folder_to_extract]
    except:
        print('There is no folder with this name.')

    df_files_csv = drive_conn.get_csv_files(folder_id= revenue_folder_id)
    
    database_files_ids = get_files_ids_from_db()

    for _ , row in df_files_csv.iterrows():

        file_id = row['id']
        file_name = row['name']
        if file_id not in database_files_ids:
            df_raw = drive_conn.read_csv_from_drive(file_id=file_id)
            print(f'File {file_name} extracted.')   
            try:
                df_validated = CompanyRevenue.validate(df_raw, lazy = True)
                df_validated['file_id'] = file_id
                list_df.append(df_validated)
            except pa.errors.SchemaErrors as err:
                print("Schema errors and failure cases:")
                print(err.failure_cases)
                print("\nDataFrame object that failed validation:")
                print(err.data)
        else:
            print(f'file {file_name} already loaded in database.')

    return list_df


def load_files(list_df: Union[list[pd.DataFrame], list[None]]) -> None:

    if not list_df:
        print('There are no files to load into the database')
        return None

    print('Loading files...')
    for df in list_df:
        file_id = df['file_id'][0]
        df.to_sql('revenues', con=engine, if_exists='append', index=False)
        print(f'File with id {file_id} loaded')
    
    print('Files loaded!')

def pipeline(service_account_path: str, parent_folder_name: str, folder_to_extract: str) -> None:

    list_df = extract_files(service_account_path, parent_folder_name, folder_to_extract)

    df_list = []
    for df in list_df:
        df_transformed = transform_data(df)
        df_list.append(df_transformed)
        
    load_files(df_list)




