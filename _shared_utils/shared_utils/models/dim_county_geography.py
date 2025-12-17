from shared_utils.models.base import get_table_name
from sqlalchemy import TIMESTAMP, Boolean, Column, Integer, String
from sqlalchemy.orm import declarative_base, declared_attr

Base = declarative_base()


class DimCountyGeography(Base):
    dataset = "mart_transit_database"
    table = "dim_county_geography"

    @declared_attr
    def __tablename__(cls):
        return get_table_name(cls.dataset, cls.table)

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
    _valid_from = Column(TIMESTAMP)
    _valid_to = Column(TIMESTAMP)
