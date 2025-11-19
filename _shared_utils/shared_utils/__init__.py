import sys

from calitp_data_analysis.sql import get_engine
from sqlalchemy.orm import sessionmaker

if hasattr(sys, "_called_from_test"):
    db_engine = get_engine(project="cal-itp-data-infra-staging")
    DBSession = sessionmaker(db_engine)
else:
    from . import (
        arcgis_query,
        catalog_utils,
        dask_utils,
        geo_utils,
        gtfs_utils_v2,
        portfolio_utils,
        publish_utils,
        rt_dates,
        rt_utils,
        schedule_rt_utils,
        time_helpers,
    )

    __all__ = [
        "arcgis_query",
        "catalog_utils",
        "dask_utils",
        "geo_utils",
        "gtfs_utils_v2",
        "portfolio_utils",
        "publish_utils",
        "rt_dates",
        "rt_utils",
        "schedule_rt_utils",
        "time_helpers",
    ]

    db_engine = get_engine()
    DBSession = sessionmaker(db_engine)
