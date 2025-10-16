from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class FctDailyFeedScheduledServiceSummary(Base):
    __tablename__ = "fct_daily_feed_scheduled_service_summary"

    service_date = Column(DateTime, primary_key=True)
    feed_key = Column(String, primary_key=True)
    gtfs_dataset_key = Column(String, primary_key=True)
    ttl_service_hours = Column(Float)
    n_trips = Column(Integer)
    first_departure_sec = Column(Integer)
    last_arrival_sec = Column(Integer)
    num_stop_times = Column(Integer)
    n_routes = Column(Integer)
    contains_warning_duplicate_stop_times_primary_key = Column(Boolean)
    contains_warning_duplicate_trip_primary_key = Column(Boolean)
    contains_warning_missing_foreign_key_stop_id = Column(Boolean)
