import pandera as pa
from pandera.typing import Series

class CompanyRevenue(pa.SchemaModel):

    company: Series[str]
    currency: Series[str]
    operational_revenue: Series[float]
    date: Series[pa.DateTime] 
    
    class Config:
        coerce = True # coerce types of all schema components
        strict = True # make sure all specified columns are in the validated dataframe 
        # from_format = # data format before validation.
        # to_format = # data format to serialize into after validation.