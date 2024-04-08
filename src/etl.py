import pandas as pd
import pandera as pa
import json
from .google_drive import GoogleDrive
from .database import engine
from .controller import get_gd_ids_from_db
from .schema import CompanyRevenue 



def extract_files(service_account_path: str, parent_folder_name: str, folder_to_extract: str) -> list[pd.DataFrame]:
    
    list_df: list[pd.DataFrame] = []

    # connect with google drive
    drive_conn = GoogleDrive(service_account_file= service_account_path,
                            folder_name= parent_folder_name)
    
    # get folder id
    try:
        revenue_folder_id = drive_conn.id_folders()[folder_to_extract]
    except:
        print('There is no folder with this name.')

    # filter to csv
    df_files_csv = drive_conn.get_csv_files(folder_id= revenue_folder_id)
    
    # get ids from db
    database_gd_ids = get_gd_ids_from_db()

    print(list(database_gd_ids))

    # read CSV
    for index, row in df_files_csv.iterrows():
        # print(database_gd_ids)
    
        if row['id'] not in list(database_gd_ids):
            print(row['id'])
            print('Not in db', row['name'])

        df_raw = drive_conn.read_csv_from_drive(file_id=row['id'])
        file_name = row['name']
        print(f'File {file_name} extracted.')   

        try:
            df_validated = validate_schema_files(df_raw)
            df_validated['gd_id'] = row['id']
            list_df.append(df_validated)
            print(f'File {file_name} validated!')
        except pa.errors.SchemaError as e:
            print(f"Error in {file_name}:")
            # print(json.dumps(e.message, indent=4))
            print(e)
        
    return list_df

@pa.check_output(CompanyRevenue, lazy = False)
def validate_schema_files(df: pd.DataFrame) -> pd.DataFrame:
    return df


def load_files(list_df: list[pd.DataFrame]) -> None:

    print('Loading files...')
    for df in list_df:
        # Save to database
        df.to_sql('revenues', con=engine, if_exists='append', index=False)
        print('Loaded in:', df['currency'][0])
    
    print('Files loaded!')

def pipeline(service_account_path: str, parent_folder_name: str, folder_to_extract: str) -> None:

    list_df = extract_files(service_account_path, parent_folder_name, folder_to_extract)

    # load_files(list_df)
    
    # print(list_df[0].info()) 





