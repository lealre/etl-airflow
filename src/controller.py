import pandas as pd
from sqlalchemy import MetaData
from sqlalchemy.orm import Session
from .database import engine

def get_gd_ids_from_db():

    table_name = 'revenues'    
    metadata = MetaData(bind=engine)
    metadata.reflect()

    if table_name in metadata.tables: 
        try:
            query = f"SELECT DISTINCT gd_id FROM {table_name}"
            database_gd_ids = pd.read_sql_query(query, engine)
            return database_gd_ids['gd_id']
        except Exception as e:
            print(e)
    
