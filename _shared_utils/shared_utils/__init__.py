import sys

from calitp_data_analysis.sql import get_engine

from sqlalchemy.orm import sessionmaker

if hasattr(sys, "_called_from_test"):
    db_engine = get_engine(project="cal-itp-data-infra-staging")
    DBSession = sessionmaker(db_engine)
else:
    db_engine = get_engine()
    DBSession = sessionmaker(db_engine)
