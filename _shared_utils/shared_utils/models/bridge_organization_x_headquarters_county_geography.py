from shared_utils.models.base import get_table_name
from sqlalchemy import Boolean, Column, DateTime, String
from sqlalchemy.orm import declarative_base, declared_attr

Base = declarative_base()


class BridgeOrganizationXHeadquartersCountyGeography(Base):
    dataset = "mart_transit_database"
    table = "bridge_organizations_x_headquarters_county_geography"

    @declared_attr
    def __tablename__(cls):
        return get_table_name(cls.dataset, cls.table)

    organization_key = Column(String, primary_key=True)
    county_geography_key = Column(String)
    organization_name = Column(String)
    county_geography_name = Column(String)
    _valid_from = Column(DateTime)
    _valid_to = Column(DateTime)
    _is_current = Column(Boolean)
