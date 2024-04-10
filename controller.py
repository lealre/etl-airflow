import pandas as pd
import pandas_datareader.data as web
import datetime

def transform_data(df:pd.DataFrame): 

    df_transformed = df.copy()

    start_date = datetime.datetime(2010, 1, 1)
    # get date from df
    date = pd.to_datetime(df['date']).dt.strftime('%Y-%m')[0]

    # Make conversion
    if df_transformed['currency'][0] == 'EUR':

        eur_usd = web.DataReader('DEXUSEU', 'fred', start = start_date)
        eur_usd = eur_usd.resample('M').last()
        eur_usd.index = eur_usd.index.strftime('%Y-%m')

        rate = eur_usd.loc[date][0]

        df_transformed['conversion_rate'] = rate
        df_transformed['usd_converted']  = df_transformed['operational_revenue'] * rate

        return df_transformed 
    
    if df_transformed['currency'][0] == 'YEN':

        usd_yen = web.DataReader('DEXJPUS', 'fred', start = start_date)
        usd_yen = usd_yen.resample('M').last()
        usd_yen.index = usd_yen.index.strftime('%Y-%m')
        yen_usd = (1/ usd_yen) #normalize


        rate = yen_usd.loc[date][0]

        df_transformed['conversion_rate'] = rate
        df_transformed['usd_converted']  = df_transformed['operational_revenue'] * rate

        return df_transformed 

    df_transformed['conversion_rate'] = 1
    df_transformed['usd_converted']  = df_transformed['operational_revenue'] 

    return df_transformed



if __name__ == '__main__':
    df = pd.DataFrame({
    'company': ['ABC Inc', 'company'],
    'currency': ['USD', 'USD'],
    'operational_revenue': [1000, 2000],
    'date': ['2020-01-05', '2020-01-05']
    })

    print(transform_data(df))
