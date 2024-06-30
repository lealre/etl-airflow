import pandas as pd
import pandera as pa
import pytest

from src.schema import CompanyRevenueTransformed


def test_valid_schema():
    df = pd.DataFrame({
        'company': ['ABC Inc', 'company', 'Company 02'],
        'currency': ['EUR', 'EUR', 'EUR'],
        'operational_revenue': [1000, 2000, 3],
        'date': ['March 2022', '2022-03', '2022/03'],
        'file_id': ['str1', 'str2', 'str3'],
        'convertion_rate': [1, 2, 3],
        'usd_converted': [30, 40, 50]
    })

    CompanyRevenueTransformed.validate(df)


def test_missing_file_id():
    df = pd.DataFrame({
        'company': ['ABC Inc', 'company', 'Company 02'],
        'currency': ['EUR', 'EUR', 'EUR'],
        'operational_revenue': [1000, 2000, 3],
        'date': ['March 2022', '2022-03', '2022/03'],
        'convertion_rate': [1, 2, 3],
        'usd_converted': [30, 40, 50]
    })

    with pytest.raises(pa.errors.SchemaErrors):
        CompanyRevenueTransformed.validate(df, lazy=True)


def test_rate_negative():
    df = pd.DataFrame({
        'company': ['ABC Inc', 'company', 'Company 02'],
        'currency': ['EUR', 'EUR', 'EUR'],
        'operational_revenue': [1000, 2000, 3],
        'date': ['March 2022', '2022-03', '2022/03'],
        'convertion_rate': [-1, 2, 3],
        'usd_converted': [30, 40, 50]
    })

    with pytest.raises(pa.errors.SchemaErrors):
        CompanyRevenueTransformed.validate(df, lazy=True)


def test_usd_converted_negative():
    df = pd.DataFrame({
        'company': ['ABC Inc', 'company', 'Company 02'],
        'currency': ['EUR', 'EUR', 'EUR'],
        'operational_revenue': [1000, 2000, 3],
        'date': ['March 2022', '2022-03', '2022/03'],
        'convertion_rate': [1, 2, 3],
        'usd_converted': [30, -40, 50]
    })

    with pytest.raises(pa.errors.SchemaErrors):
        CompanyRevenueTransformed.validate(df, lazy=True)
