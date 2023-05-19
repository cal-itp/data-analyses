import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(1_000_000_000_000) ## 1TB?
os.environ['USE_PYGEOS'] = '0'

from siuba import *
import pandas as pd
import datetime as dt

from calitp_data_analysis.tables import tbls
import shared_utils

ANALYSIS_DATE = dt.date.fromisoformat(shared_utils.rt_dates.DATES['may2023'])
PROGRESS_PATH = f'./_rt_progress_{ANALYSIS_DATE}.parquet'

def build_speedmaps_index(analysis_date: dt.date) -> pd.DataFrame:
    '''
    An index table for tracking down a given org's schedule/rt feeds.
    Note that in limited cases, multiple orgs may share the same datasets
    (VCTC combined feeds, SD Airport and SDMTS...)
    '''
    analysis_dt = dt.datetime.combine(analysis_date, dt.time(0, 0))
    
    dim_orgs = (tbls.mart_transit_database.dim_organizations()
                >> filter(_._valid_from <= analysis_dt, _._valid_to > analysis_dt)
                >> select(_.source_record_id, _.caltrans_district)
               )
    
    orgs_with_vp = (tbls.mart_transit_database.dim_provider_gtfs_data()
    >> filter(_._valid_from <= analysis_dt, _._valid_to > analysis_dt,
              _.public_customer_facing_or_regional_subfeed_fixed_route,
              _.vehicle_positions_gtfs_dataset_key != None)
    >> inner_join(_, dim_orgs, on = {'organization_source_record_id': 'source_record_id'})
    >> select(_.organization_itp_id, _.organization_name, _.organization_source_record_id,
             _.caltrans_district, _._is_current, _.vehicle_positions_gtfs_dataset_key)
    >> collect()
    )
    assert not orgs_with_vp.isnull().values.any()
    orgs_with_vp['analysis_date'] = analysis_date
    orgs_with_vp = orgs_with_vp >> distinct(_.organization_name,
                    _.organization_itp_id, _.organization_source_record_id,
                    _.caltrans_district, _._is_current, _.analysis_date
                                           )
    return orgs_with_vp

if __name__ == "__main__":
    
    speedmaps_index = build_speedmaps_index(ANALYSIS_DATE)
    speedmaps_index_joined = shared_utils.rt_utils.check_intermediate_data(speedmaps_index)
    speedmaps_index_joined.to_parquet(PROGRESS_PATH)