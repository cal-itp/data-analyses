from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DimCountyGeography(Base):
    __tablename__ = "dim_county_geography"

    key = Column(String, primary_key=True)
    source_record_id = Column(String)
    name = Column(String)
    fips = Column(Integer)
    msa = Column(String)
    caltrans_district = Column(Integer)
    caltrans_district_name = Column(String)
    place_geography = Column(String)
    organization_key = Column(String)
    service_key = Column(String)
    _is_current = Column(Boolean)
    _valid_from = Column(DateTime)
    _valid_to = Column(DateTime)
