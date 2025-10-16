from sqlalchemy import Boolean, Column, DateTime, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class BridgeOrganizationsXHeadquartersCountyGeography(Base):
    __tablename__ = "bridge_organizations_x_headquarters_county_geography"

    organization_key = Column(String, primary_key=True)
    county_geography_key = Column(String)
    organization_name = Column(String)
    county_geography_name = Column(String)
    _valid_from = Column(DateTime)
    _valid_to = Column(DateTime)
    _is_current = Column(Boolean)
