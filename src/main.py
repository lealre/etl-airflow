## sET PATH AS AIRFLOW
import sys
import os
from load_dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.getenv('PYTHON_PATH'))
######################################################
######################################################
from src.etl import pipeline


service_account_path = 'service_account.json'
parent_folder_name = 'python_to_drive'
folder_to_extract = 'Companies Invoicing'

pipeline(service_account_path = service_account_path,
         parent_folder_name = parent_folder_name,
         folder_to_extract = folder_to_extract)