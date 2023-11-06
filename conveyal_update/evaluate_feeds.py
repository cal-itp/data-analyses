import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000)
from shared_utils import gtfs_utils_v2

from calitp_data_analysis.tables import tbls
from siuba import *
import pandas as pd
import datetime as dt

import conveyal_vars

def check_defined_elsewhere(row, df):
    '''
    for feeds without service defined, check if the same service is captured in another feed that does include service
    '''
    is_defined = ((df >> filter(-_.n.isna())).service_key == row.service_key).any()
    row['service_any_feed'] = is_defined
    return row

target_date = conveyal_vars.target_date

def get_feeds_check_service():
    feeds_on_target = gtfs_utils_v2.schedule_daily_feed_to_gtfs_dataset_name(selected_date=target_date)
    # default will use mtc subfeeds (prev Conveyal behavior), can spec customer facing if we wanna switch

    operator_feeds = feeds_on_target.feed_key
    trips = (
        tbls.mart_gtfs.fct_scheduled_trips()
        >> filter(_.feed_key.isin(operator_feeds), _.service_date == target_date)
        >> group_by(_.feed_key)
        >> count(_.feed_key)
        # >> collect()
        # >> mutate(any_trip = True)
    )
    service_defined = trips >> collect()
    feeds_on_target = feeds_on_target >> left_join(_, service_defined, on = 'feed_key') >> select(-_.name)
    return feeds_on_target
    
def attach_transit_services(feeds_on_target: pd.DataFrame):

    target_dt = dt.datetime.combine(target_date, dt.time(0))

    services = (tbls.mart_transit_database.dim_gtfs_service_data()
        >> filter(_._valid_from <= target_dt, _._valid_to > target_dt)
        # >> filter(_.gtfs_dataset_key == 'da7e9e09d3eec6c7686adc21c8b28b63') # test with BCT
        # >> filter(_.service_key == '5bc7371dca26d74a99be945b18b3174e')
        >> select(_.service_key, _.gtfs_dataset_key)
        >> collect()
    )

    feeds_on_target = feeds_on_target >> left_join(_, services, on='gtfs_dataset_key')
    return feeds_on_target           
        
def report_undefined(feeds_on_target: pd.DataFrame):
    fname = 'no_apparent_service.csv'
    undefined = feeds_on_target.apply(check_defined_elsewhere, axis=1, args=[feeds_on_target]) >> filter(-_.service_any_feed)
    print('these feeds have no service defined on target date, nor are their services captured in other feeds:')
    print(undefined >> select(_.gtfs_dataset_name, _.service_any_feed))
    print(f'saving detailed csv to {fname}')
    undefined.to_csv(fname)
    return

if __name__ == '__main__':
    
    feeds_on_target = get_feeds_check_service()
    feeds_on_target = attach_transit_services(feeds_on_target)
    report_undefined(feeds_on_target)
    feeds_on_target.to_parquet(f'{conveyal_vars.gcs_path}feeds_{target_date.isoformat()}.parquet')
    