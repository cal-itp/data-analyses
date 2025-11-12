from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DimProviderGtfsData(Base):
    __tablename__ = "dim_provider_gtfs_data"

    key = Column(String, primary_key=True)
    public_customer_facing_fixed_route = Column(Boolean)
    public_customer_facing_or_regional_subfeed_fixed_route = Column(Boolean)
    organization_key = Column(String)
    organization_name = Column(String)
    organization_itp_id = Column(Integer)
    organization_hubspot_company_record_id = Column(String)
    organization_ntd_id = Column(String)
    organization_source_record_id = Column(String)
    service_key = Column(String)
    service_name = Column(String)
    service_source_record_id = Column(String)
    gtfs_service_data_customer_facing = Column(Boolean)
    regional_feed_type = Column(String)
    associated_schedule_gtfs_dataset_key = Column(String)
    schedule_gtfs_dataset_name = Column(String)
    schedule_source_record_id = Column(String)
    service_alerts_gtfs_dataset_name = Column(String)
    service_alerts_source_record_id = Column(String)
    vehicle_positions_gtfs_dataset_name = Column(String)
    vehicle_positions_source_record_id = Column(String)
    trip_updates_gtfs_dataset_name = Column(String)
    trip_updates_source_record_id = Column(String)
    schedule_gtfs_dataset_key = Column(String)
    service_alerts_gtfs_dataset_key = Column(String)
    vehicle_positions_gtfs_dataset_key = Column(String)
    trip_updates_gtfs_dataset_key = Column(String)
    _valid_from = Column(DateTime)
    _valid_to = Column(DateTime)
    _is_current = Column(Boolean)
