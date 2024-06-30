import os
import sys

# Add the root directory of your project to sys.path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
)
######################################################
from src.etl import pipeline

service_account_path = 'service_account.json'
parent_folder_name = 'python_to_drive'
folder_to_extract_from = 'Operational Revenue'

pipeline(service_account_path=service_account_path,
         parent_folder_name=parent_folder_name,
         folder_to_extract=folder_to_extract_from)
