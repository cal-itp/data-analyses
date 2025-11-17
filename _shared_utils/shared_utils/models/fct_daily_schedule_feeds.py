from sqlalchemy import Column, DateTime, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class FctDailyScheduleFeeds(Base):
    __tablename__ = "fct_daily_schedule_feeds"

    key = Column(String, primary_key=True)
    date = Column(DateTime)
    feed_key = Column(String)
    feed_timezone = Column(String)
    base64_url = Column(String)
    gtfs_dataset_key = Column(String)
    gtfs_dataset_name = Column(String)
