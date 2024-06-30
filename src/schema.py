from typing import Optional

import pandera as pa
from pandera.typing import Series


class CompanyRevenueBase(pa.SchemaModel):

    company: Series[str]
    currency: Series[str] = pa.Field(isin=['EUR', 'USD', 'YEN'])
    operational_revenue: Series[float] = pa.Field(ge=0)
    date: Series[pa.DateTime]
    file_id: Optional[str]

    class Config:
        coerce = True
        strict = True

    @pa.check(
        "currency",
        name="Currency types",
        error="There is more than one type of currency;"
    )
    def check_equal_currency(cls, currency: Series[str]) -> Series[bool]:
        return currency.nunique() == 1

    @pa.check("date", name="Date format", error="There is more than one date;")
    def check_date_format(cls, date: Series[pa.DateTime]) -> Series[bool]:
        return date.nunique() == 1


class CompanyRevenueTransformed(CompanyRevenueBase):

    file_id: Series[str]
    convertion_rate: Series[float] = pa.Field(ge=0)
    usd_converted: Series[float] = pa.Field(ge=0)
