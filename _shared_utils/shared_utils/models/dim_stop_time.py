from shared_utils.models.base import get_table_name
from sqlalchemy import TIMESTAMP, Boolean, Column, Date, Integer, Numeric, String, Time
from sqlalchemy.orm import declarative_base, declared_attr

Base = declarative_base()


class DimStopTime(Base):
    dataset = "mart_gtfs"
    table = "dim_stop_times"

    @declared_attr
    def __tablename__(cls):
        return get_table_name(cls.dataset, cls.table)

    key = Column(String, primary_key=True)
    _gtfs_key = Column(String)
    base64_url = Column(String)
    feed_key = Column(String)
    trip_id = Column(String)
    stop_id = Column(String)
    stop_sequence = Column(Integer)
    arrival_time = Column(String)
    departure_time = Column(String)
    # arrival_time_interval, departure_time_interval, start_pickup_drop_off_window_interval, and end_pickup_drop_off_window_interval should
    # be Interval type, however this is not currently supported with sqlalchemy-bigquery https://github.com/googleapis/python-bigquery-sqlalchemy/issues/453
    # https://github.com/googleapis/python-bigquery/issues/826. Falling back to Time satisfies our current usage.
    arrival_time_interval = Column(Time)  # Interval
    departure_time_interval = Column(Time)  # Interval
    stop_headsign = Column(String)
    pickup_type = Column(Integer)
    drop_off_type = Column(Integer)
    continuous_pickup = Column(Integer)
    continuous_drop_off = Column(Integer)
    shape_dist_traveled = Column(Numeric)
    timepoint = Column(Integer)
    warning_duplicate_gtfs_key = Column(Boolean)
    warning_missing_foreign_key_stop_id = Column(Boolean)
    _dt = Column(Date)
    _feed_valid_from = Column(TIMESTAMP)
    _line_number = Column(Integer)
    feed_timezone = Column(String)
    arrival_sec = Column(Integer)
    departure_sec = Column(Integer)
    start_pickup_drop_off_window = Column(String)
    end_pickup_drop_off_window = Column(String)
    start_pickup_drop_off_window_interval = Column(Time)  # Interval
    end_pickup_drop_off_window_interval = Column(Time)  # Interval
    start_pickup_drop_off_window_sec = Column(Integer)
    end_pickup_drop_off_window_sec = Column(Integer)
    mean_duration_factor = Column(Numeric)
    mean_duration_offset = Column(Numeric)
    safe_duration_factor = Column(Numeric)
    safe_duration_offset = Column(Numeric)
    pickup_booking_rule_id = Column(String)
    drop_off_booking_rule_id = Column(String)
