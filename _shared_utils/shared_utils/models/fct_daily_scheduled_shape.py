from shared_utils.models.base import get_table_name
from sqlalchemy import Boolean, Column, Date, DateTime, Integer, String
from sqlalchemy.orm import declarative_base, declared_attr

Base = declarative_base()


class FctDailyScheduledShape(Base):
    dataset = "mart_gtfs"
    table = "fct_daily_scheduled_shapes"

    @declared_attr
    def __tablename__(cls):
        return get_table_name(cls.dataset, cls.table)

    key = Column(String, primary_key=True)
    feed_key = Column(String)
    service_date = Column(Date)
    shape_id = Column(String)
    shape_array_key = Column(String)
    feed_timezone = Column(String)
    n_trips = Column(Integer)
    shape_first_departure_datetime_pacific = Column(DateTime)
    shape_last_arrival_datetime_pacific = Column(DateTime)
    contains_warning_duplicate_trip_primary_key = Column(Boolean)
    # pt_array should be GEOMETRY type to match BigQuery, however the current code expects a text representation
    pt_array = Column(String)  # GEOMETRY
