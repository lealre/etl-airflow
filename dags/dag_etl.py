from src.etl import extract_files, load_files
from airflow.decorators import dag, task
from datetime import datetime

@dag(
        dag_id = 'example_dag',
        description='Dag with 3 simple steps',
        schedule= '* * * * *',
        start_date= datetime(2024,4,7),
        catchup= False
)
def pipeline():

    @task(task_id = 'extract-data')
    def task_extract_files():
        return extract_files()
    
    @task(task_id = 'load-data')
    def task_load_files(list_df):
        return load_files(list_df)
    
    task_extract = task_extract_files()
    task_load = task_load_files(task_extract) 

    task_extract >> task_load


pipeline()