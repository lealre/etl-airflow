from src.etl import extract_files, load_files
from airflow.decorators import dag, task
from datetime import datetime

service_account_path = 'service_account.json'
parent_folder_name = 'python_to_drive'
folder_to_extract = 'Companies Invoicing'

@dag(
        dag_id = 'example_dag',
        description='Dag with 3 simple steps',
        schedule= '* * * * *',
        start_date= datetime(2024,4,7),
        catchup= False
)
def pipeline(service_account_path: str, parent_folder_name: str, folder_to_extract: str):

    @task(task_id = 'Extract data from Google Drive')
    def task_extract_files():
        return extract_files(service_account_path = service_account_path,
                             parent_folder_name = parent_folder_name,
                             folder_to_extract = folder_to_extract)
    @task(task_id = 'Load files to PostgreSQL')
    def task_load_files(list_df):
        return load_files(list_df)
    
    task_extract = task_extract_files()
    task_load = task_load_files(task_extract) 

    task_extract >> task_load


pipeline()