from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DimOrganization(Base):
    __tablename__ = "dim_organizations"

    key = Column(String, primary_key=True)
    source_record_id = Column(String)
    name = Column(String)
    organization_type = Column(String)
    roles = Column(String)
    itp_id = Column(Integer)
    details = Column(String)
    website = Column(String)
    reporting_category = Column(String)
    hubspot_company_record_id = Column(String)
    gtfs_static_status = Column(String)
    gtfs_realtime_status = Column(String)
    _deprecated__assessment_status = Column(Boolean)
    manual_check__contact_on_website = Column(String)
    alias = Column(String)
    is_public_entity = Column(Boolean)
    ntd_id = Column(String)
    ntd_agency_info_key = Column(String)
    ntd_id_2022 = Column(String)
    rtpa_key = Column(String)
    rtpa_name = Column(String)
    mpo_key = Column(String)
    mpo_name = Column(String)
    public_currently_operating = Column(Boolean)
    public_currently_operating_fixed_route = Column(Boolean)
    _is_current = Column(Boolean)
    _valid_from = Column(DateTime)
    _valid_to = Column(DateTime)
