import sys
import os
# Add the root directory of your project to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest
import pandera as pa
import pandas as pd
import numpy as np
from src.schema import CompanyRevenue

def test_valid_schema():

    df = pd.DataFrame({
        'company': ['ABC Inc', 'company'],
        'currency': ['EUR', 'USD'],
        'operational_revenue': [1000, 2000],
        'date': ['2020-01', '2020-02']
    })

    CompanyRevenue.validate(df)

def test_missing_value():

    df = pd.DataFrame({
        'company': ['ABC Inc', np.nan],
        'currency': ['EUR', 'USD'],
        'operational_revenue': [1000, 2000],
        'date': ['2020-01', '2020-02']
    })

    with pytest.raises(pa.errors.SchemaErrors):  
        CompanyRevenue.validate(df, lazy= True)

def test_wrong_date():

    df = pd.DataFrame({
        'company': ['ABC Inc', 'company'],
        'currency': ['EUR', 'USD'],
        'operational_revenue': [1000, 2000],
        'date': ['2020-01', 'date']
    })

    with pytest.raises(pa.errors.SchemaErrors):  
        CompanyRevenue.validate(df, lazy= True)
