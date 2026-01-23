from shared_utils.models.base import get_table_name
from sqlalchemy import (
    TIMESTAMP,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    String,
)
from sqlalchemy.orm import declarative_base, declared_attr

Base = declarative_base()


class FctDailyScheduledStop(Base):
    dataset = "mart_gtfs"
    table = "fct_daily_scheduled_stops"

    @declared_attr
    def __tablename__(cls):
        return get_table_name(cls.dataset, cls.table)

    key = Column(String, primary_key=True)
    service_date = Column(Date)
    feed_key = Column(String)
    stop_id = Column(String)
    feed_timezone = Column(String)
    daily_arrivals = Column(Integer)
    first_stop_arrival_datetime_pacific = Column(DateTime)
    last_stop_departure_datetime_pacific = Column(DateTime)
    _feed_valid_from = Column(TIMESTAMP)
    n_hours_in_service = Column(Integer)
    n_route_types = Column(Integer)
    arrivals_per_hour_owl = Column(Float)
    arrivals_per_hour_early_am = Column(Float)
    arrivals_per_hour_am_peak = Column(Float)
    arrivals_per_hour_midday = Column(Float)
    arrivals_per_hour_pm_peak = Column(Float)
    arrivals_per_hour_evening = Column(Float)
    arrivals_owl = Column(Integer)
    arrivals_early_am = Column(Integer)
    arrivals_am_peak = Column(Integer)
    arrivals_midday = Column(Integer)
    arrivals_pm_peak = Column(Integer)
    arrivals_evening = Column(Integer)
    route_type_0 = Column(Integer)
    route_type_1 = Column(Integer)
    route_type_2 = Column(Integer)
    route_type_3 = Column(Integer)
    route_type_4 = Column(Integer)
    route_type_5 = Column(Integer)
    route_type_6 = Column(Integer)
    route_type_7 = Column(Integer)
    route_type_11 = Column(Integer)
    route_type_12 = Column(Integer)
    missing_route_type = Column(Integer)
    contains_warning_duplicate_stop_times_primary_key = Column(Boolean)
    contains_warning_duplicate_trip_primary_key = Column(Boolean)
    route_id_array = Column(Integer)
    route_type_array = Column(Integer)
    contains_warning_duplicate_stop_primary_key = Column(Boolean)
    stop_key = Column(String)
    tts_stop_name = Column(String)
    # pt_geom should be GEOMETRY type to match BigQuery, however the current code expects a text representation
    pt_geom = Column(String)  # GEOMETRY
    parent_station = Column(String)
    stop_code = Column(String)
    stop_name = Column(String)
    stop_desc = Column(String)
    location_type = Column(Integer)
    stop_timezone_coalesced = Column(String)
    wheelchair_boarding = Column(Integer)
