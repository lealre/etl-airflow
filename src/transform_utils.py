import datetime

import pandas as pd
import pandas_datareader.data as web
import pandera as pa

from .schema import CompanyRevenueTransformed


def get_currencies_rates() -> pd.DataFrame:

    start_date = datetime.datetime(2010, 1, 1)

    eur_usd = web.DataReader('DEXUSEU', 'fred', start=start_date)
    eur_usd = eur_usd.resample('M').last()
    eur_usd.index = eur_usd.index.strftime('%Y-%m')

    usd_yen = web.DataReader('DEXJPUS', 'fred', start=start_date)
    usd_yen = usd_yen.resample('M').last()
    usd_yen.index = usd_yen.index.strftime('%Y-%m')
    yen_usd = (1 / usd_yen)  # Convertion rate from YEN to USD

    df_convertion_rates = pd.concat([yen_usd, eur_usd], axis=1)

    df_convertion_rates.columns = ['YEN/USD', 'EUR/USD']

    return df_convertion_rates


@pa.check_output(CompanyRevenueTransformed, lazy=True)
def transform_and_validate_data(
    df_to_transform: pd.DataFrame, df_convertion_rates: pd.DataFrame
) -> pd.DataFrame:

    currency = df_to_transform['currency'][0]

    if currency == 'USD':
        convertion_rate = 1
        df_to_transform['convertion_rate'] = convertion_rate
        df_to_transform['usd_converted'] = (
             df_to_transform['operational_revenue']
        )
        return df_to_transform

    currency_pair = f'{currency}/USD'
    convertion_rate_date = df_to_transform['date'].dt.strftime('%Y-%m')[0]

    convertion_rate = (
         df_convertion_rates.loc[convertion_rate_date][currency_pair]
    )

    df_to_transform['convertion_rate'] = convertion_rate
    df_to_transform['usd_converted'] = (
         df_to_transform['operational_revenue'] * convertion_rate
    )

    return df_to_transform
