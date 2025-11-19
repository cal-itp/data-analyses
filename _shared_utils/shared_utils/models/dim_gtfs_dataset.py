from shared_utils.models.base import get_table_name
from sqlalchemy import Boolean, Column, Date, DateTime, String
from sqlalchemy.orm import declarative_base, declared_attr

Base = declarative_base()


class DimGtfsDataset(Base):
    dataset = "mart_transit_database"
    table = "dim_gtfs_datasets"

    @declared_attr
    def __tablename__(cls):
        return get_table_name(cls.dataset, cls.table)

    key = Column(String, primary_key=True)
    source_record_id = Column(String)
    name = Column(String)
    type = Column(String)
    regional_feed_type = Column(String)
    backdated_regional_feed_type = Column(String)
    uri = Column(String)
    future_uri = Column(String)
    deprecated_date = Column(Date)
    data_quality_pipeline = Column(Boolean)
    manual_check__link_to_dataset_on_website = Column(String)
    manual_check__accurate_shapes = Column(String)
    manual_check__data_license = Column(String)
    manual_check__authentication_acceptable = Column(String)
    manual_check__stable_url = Column(String)
    manual_check__localized_stop_tts = Column(String)
    manual_check__grading_scheme_v1 = Column(String)
    base64_url = Column(String)
    private_dataset = Column(Boolean)
    analysis_name = Column(String)
    _is_current = Column(Boolean)
    _valid_from = Column(DateTime)
    _valid_to = Column(DateTime)
