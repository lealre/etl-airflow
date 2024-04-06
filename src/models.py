from .database import Base
from sqlalchemy import Column, Integer, String, Float, DateTime

class CompanyInvoicing(Base):
    __tablename__ = 'companies_invoicing'

    id = Column(Integer, primary_key= True)
    name = Column(String)
    currency = Column(String)
    value = Column(Float)
    date = Column(DateTime(timezone= True))