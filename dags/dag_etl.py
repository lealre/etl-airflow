from src.etl import extract_files, load_files
from airflow.decorators import dag, task
from datetime import datetime

@dag(
        dag_id = 'example_dag',
        description='Dag with 3 simple steps',
        schedule= '* * * * *',
        start_date= datetime(2024,4,7),
        catchup= False,
        params= {
            'service_account_path' : 'service_account.json',
            'parent_folder_name' : 'python_to_drive',
            'folder_to_extract' : 'Companies Invoicing'

        }
)
def pipeline():

    @task(task_id = 'extract-data')
    def task_extract_files(**context):
        return extract_files(context['params']['service_account_path'], context['params']['parent_folder_name'], context['params']['folder_to_extract'])
    
    @task(task_id = 'load-data')
    def task_load_files(list_df):
        return load_files(list_df)
    
    task_extract = task_extract_files()
    task_load = task_load_files(task_extract) 

    task_extract >> task_load


pipeline()