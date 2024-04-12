from src.etl import connect_drive_and_extract_files, validate_files, transform_data, load_files
from airflow.decorators import dag, task
from datetime import datetime

@dag(
        dag_id = 'ETL-GDrive-to-PostgreSQL',
        description='Dag to extract data from Google Drive, transform and store in a POstgreSQL',
        schedule= '*/2 * * * *',
        start_date= datetime(2024,4,7),
        catchup= False,
        params= {
            'service_account_path' : 'service_account.json',
            'parent_folder_name' : 'python_to_drive',
            'folder_to_extract' : 'Companies Invoicing'

        }
)
def pipeline():

    @task(task_id = 'connect-with-drive-and-read-files-info')
    def task_connect_drive_and_extract_files(**context):
        return connect_drive_and_extract_files(context['params']['service_account_path'], context['params']['parent_folder_name'], context['params']['folder_to_extract'])
    
    @task(task_id = 'validate-data')
    def task_validate_files(list_df):
        return validate_files(list_df)
    
    @task(task_id = 'transform-data')
    def task_transform_data(list_df):
        return transform_data(list_df)

    @task(task_id = 'load-data')
    def task_load_files(list_df):
        return load_files(list_df)
    
    task_connect_and_extract = task_connect_drive_and_extract_files()
    task_validate_data = task_validate_files(task_connect_and_extract)
    task_transform = task_transform_data(task_validate_data)
    task_load = task_load_files(task_transform) 

    task_connect_and_extract >> task_validate_data >> task_transform >> task_load

pipeline()