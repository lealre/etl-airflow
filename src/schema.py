import pandera as pa
import pandas as pd
from pandera.typing import Series

class CompanyRevenue(pa.SchemaModel):

    company: Series[str]
    currency: Series[str] = pa.Field(isin= ['EUR', 'USD', 'YEN'])
    operational_revenue: Series[float] = pa.Field(ge = 0)
    date: Series[pa.DateTime] 
    
    class Config:
        coerce = True # coerce types of all schema components
        strict = True # make sure all specified columns are in the validated dataframe 