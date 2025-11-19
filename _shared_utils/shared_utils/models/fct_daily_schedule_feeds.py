from shared_utils.models.base import get_table_name
from sqlalchemy import Column, DateTime, String
from sqlalchemy.orm import declarative_base, declared_attr

Base = declarative_base()


class FctDailyScheduleFeeds(Base):
    dataset = "mart_gtfs"
    table = "fct_daily_schedule_feeds"

    @declared_attr
    def __tablename__(cls):
        return get_table_name(cls.dataset, cls.table)

    key = Column(String, primary_key=True)
    date = Column(DateTime)
    feed_key = Column(String)
    feed_timezone = Column(String)
    base64_url = Column(String)
    gtfs_dataset_key = Column(String)
    gtfs_dataset_name = Column(String)
