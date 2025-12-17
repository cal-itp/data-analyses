from shared_utils.models.base import get_table_name
from sqlalchemy import TIMESTAMP, Column, Date, Float, Integer, String, Time
from sqlalchemy.orm import declarative_base, declared_attr

Base = declarative_base()


class FctVehicleLocation(Base):
    dataset = "mart_gtfs"
    table = "fct_vehicle_locations"

    @declared_attr
    def __tablename__(cls):
        return get_table_name(cls.dataset, cls.table)

    key = Column(String, primary_key=True)
    gtfs_dataset_key = Column(String)
    dt = Column(Date)
    service_date = Column(Date)
    hour = Column(TIMESTAMP)
    base64_url = Column(String)
    _extract_ts = Column(TIMESTAMP)
    _config_extract_ts = Column(TIMESTAMP)
    gtfs_dataset_name = Column(String)
    schedule_gtfs_dataset_key = Column(String)
    schedule_base64_url = Column(String)
    schedule_name = Column(String)
    schedule_feed_key = Column(String)
    schedule_feed_timezone = Column(String)
    _header_message_age = Column(Integer)
    _vehicle_message_age = Column(Integer)
    header_timestamp = Column(TIMESTAMP)
    header_version = Column(String)
    header_incrementality = Column(String)
    id = Column(String)
    current_stop_sequence = Column(Integer)
    stop_id = Column(String)
    current_status = Column(String)
    vehicle_timestamp = Column(TIMESTAMP)
    congestion_level = Column(String)
    occupancy_status = Column(String)
    occupancy_percentage = Column(Integer)
    vehicle_id = Column(String)
    vehicle_label = Column(String)
    vehicle_license_plate = Column(String)
    vehicle_wheelchair_accessible = Column(String)
    trip_id = Column(String)
    trip_route_id = Column(String)
    trip_direction_id = Column(Integer)
    trip_start_time = Column(String)
    # trip_start_time_interval should be Interval type, however this is not currently supported with sqlalchemy-bigquery
    # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/453
    # https://github.com/googleapis/python-bigquery/issues/826. Falling back to Time satisfies our current usage.
    trip_start_time_interval = Column(Time)  # Interval
    trip_start_date = Column(Date)
    trip_schedule_relationship = Column(String)
    position_latitude = Column(Float)
    position_longitude = Column(Float)
    position_bearing = Column(Float)
    position_odometer = Column(Float)
    position_speed = Column(Float)
    location_timestamp = Column(TIMESTAMP)
    vehicle_trip_key = Column(String)
    next_location_key = Column(String)
    # location should be GEOMETRY type to match BigQuery, however the current usage of this data expects a string representation.
    location = Column(String)  # GEOMETRY
    trip_instance_key = Column(String)
