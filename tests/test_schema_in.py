import numpy as np
import pandas as pd
import pandera as pa
import pytest

from src.schema import CompanyRevenueBase


def test_valid_schema():
    df = pd.DataFrame({
        'company': ['ABC Inc', 'company', 'Company 02'],
        'currency': ['EUR', 'EUR', 'EUR'],
        'operational_revenue': [1000, 2000, 3],
        'date': ['March 2022', '2022-03', '2022/03']
    })

    CompanyRevenueBase.validate(df)


def test_aditional_column():
    df = pd.DataFrame({
        'company': ['ABC Inc', 'company'],
        'currency': ['EUR', 'EUR'],
        'operational_revenue': [1000, 2000],
        'date': ['2020-02', '2020-02'],
        'aditional': [0, 0]
    })

    with pytest.raises(pa.errors.SchemaErrors):
        CompanyRevenueBase.validate(df, lazy=True)


def test_missing_value():
    df = pd.DataFrame({
        'company': ['ABC Inc', np.nan],
        'currency': ['EUR', 'EUR'],
        'operational_revenue': [1000, 2000],
        'date': ['2020-02', '2020-02']
    })

    with pytest.raises(pa.errors.SchemaErrors):
        CompanyRevenueBase.validate(df, lazy=True)


def test_parse_to_datetime():
    df = pd.DataFrame({
        'company': ['ABC Inc', 'company'],
        'currency': ['USD', 'USD'],
        'operational_revenue': [1000, 2000],
        'date': ['2020-02', 'date']
    })

    with pytest.raises(pa.errors.SchemaErrors):
        CompanyRevenueBase.validate(df, lazy=True)


def test_dates_differents():
    df = pd.DataFrame({
        'company': ['ABC Inc', 'company'],
        'currency': ['USD', 'USD'],
        'operational_revenue': [1000, 2000],
        'date': ['2022-02', 'April 2022']
    })

    with pytest.raises(pa.errors.SchemaErrors):
        CompanyRevenueBase.validate(df, lazy=True)


def test_negative_revenue():
    df = pd.DataFrame({
        'company': ['ABC Inc', 'company'],
        'currency': ['USD', 'USD'],
        'operational_revenue': [-1000, 2000],
        'date': ['2020-02', '2020-02']
    })

    with pytest.raises(pa.errors.SchemaErrors):
        CompanyRevenueBase.validate(df, lazy=True)


def test_currency_not_allowed():
    df = pd.DataFrame({
        'company': ['ABC Inc', 'company'],
        'currency': ['BRL', 'BRL'],
        'operational_revenue': [1000, 2000],
        'date': ['2020-02', '2020-02']
    })

    with pytest.raises(pa.errors.SchemaErrors):
        CompanyRevenueBase.validate(df, lazy=True)


def test_more_than_one_currency():
    df = pd.DataFrame({
        'company': ['ABC Inc', 'company'],
        'currency': ['EUR', 'USD'],
        'operational_revenue': [1000, 2000],
        'date': ['2020-02', '2020-02']
    })

    with pytest.raises(pa.errors.SchemaErrors):
        CompanyRevenueBase.validate(df, lazy=True)
