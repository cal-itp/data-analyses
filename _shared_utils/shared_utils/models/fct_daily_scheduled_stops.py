from shared_utils.models.base import get_table_name
from sqlalchemy import Boolean, Column, Date, DateTime, Integer, String
from sqlalchemy.orm import declarative_base, declared_attr

Base = declarative_base()


class FctDailyScheduledStops(Base):
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
    stop_event_count = Column(Integer)
    first_stop_arrival_datetime_pacific = Column(DateTime)
    last_stop_departure_datetime_pacific = Column(DateTime)
    _feed_valid_from = Column(DateTime)
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
    route_type_array = Column(Integer)
    transit_mode_array = Column(String)
    n_transit_modes = Column(Integer)
    contains_warning_duplicate_stop_primary_key = Column(Boolean)
    stop_key = Column(String)
    tts_stop_name = Column(String)
    pt_geom = Column(String)
    parent_station = Column(String)
    stop_code = Column(String)
    stop_name = Column(String)
    stop_desc = Column(String)
    location_type = Column(Integer)
    stop_timezone_coalesced = Column(String)
    wheelchair_boarding = Column(Integer)
