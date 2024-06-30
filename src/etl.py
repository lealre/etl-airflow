import pandas as pd
import pandera as pa

from .database import engine, get_files_ids_from_db
from .google_drive import GoogleDrive
from .schema import CompanyRevenueBase
from .transform_utils import get_currencies_rates, transform_and_validate_data


def connect_drive_and_extract_files(
    service_account_path: str,
    parent_folder_name: str,
    folder_to_extract: str
) -> list[pd.DataFrame] | list[None]:

    try:
        drive_conn = GoogleDrive(
            service_account_file=service_account_path,
            folder_name=parent_folder_name
        )
    except Exception as e:
        print(f"Error connecting with Google Drive: {e}")

    try:
        revenue_folder_id = (
            drive_conn.id_folders()[folder_to_extract]
        )
    except Exception as e:
        print("There is no folder with this name.")
        print(e)

    try:
        df_files_csv = (
            drive_conn.get_csv_files(folder_id=revenue_folder_id)
        )
    except Exception as e:
        print(e)

    database_files_ids = get_files_ids_from_db()

    list_df_csv: list[pd.DataFrame] = []

    for _, row in df_files_csv.iterrows():
        file_id = row["id"]
        file_name = row["name"]
        if file_id not in database_files_ids:
            df_extracted = (
                drive_conn
                .read_csv_from_drive(file_id=file_id)
            )
            df_extracted["file_id"] = file_id
            list_df_csv.append(df_extracted)
            print(f"File {file_name} extracted.")
        else:
            print(
                f"File {file_name} "
                "was previously loaded into the database"
            )

    return list_df_csv


def validate_data(
    list_df_csv: list[pd.DataFrame]
) -> list[pd.DataFrame] | list[None]:

    list_df_validated: list[pd.DataFrame] = []

    for df in list_df_csv:
        try:
            df_validated = CompanyRevenueBase.validate(df, lazy=True)
            list_df_validated.append(df_validated)
        except pa.errors.SchemaErrors as err:
            print("Schema errors and failure cases:")
            print(err.message)

    return list_df_validated


def transform_data(
        list_df_validated: list[pd.DataFrame]) -> list[pd.DataFrame]:

    list_df_transformed: list[pd.DataFrame] = []

    df_convertion_rates = get_currencies_rates()

    for df in list_df_validated:
        try:
            df_transformed = transform_and_validate_data(df_to_transform=df,
                                                         df_convertion_rates=df_convertion_rates)
            list_df_transformed.append(df_transformed)
        except pa.errors.SchemaErrors as err:
            print("Validation error in transformation (out)")
            print("Schema errors and failure cases:")
            print(err.message)

    return list_df_transformed


def load_files(list_df: list[pd.DataFrame] | list[None]) -> None:

    if not list_df:
        print("There are no files to load into the database")
        return None

    print("Loading files...")
    for df in list_df:
        file_id = df["file_id"][0]
        df.to_sql('revenues', con=engine, if_exists="append", index=False)
        print(f"File with id {file_id} loaded")

    print("Files loaded!")


def pipeline(
    service_account_path: str,
    parent_folder_name: str,
    folder_to_extract: str
) -> None:

    list_df_files_info = connect_drive_and_extract_files(
        service_account_path,
        parent_folder_name,
        folder_to_extract
    )

    list_df_extracted = validate_data(list_df_files_info)

    list_df_transformed = transform_data(list_df_extracted)

    load_files(list_df_transformed)
