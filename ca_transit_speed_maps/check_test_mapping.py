import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(1_000_000_000_000) ## 1TB?
os.environ['USE_PYGEOS'] = '0'

from siuba import *
import pandas as pd
import datetime as dt

from shared_utils import rt_utils
from rt_analysis import rt_filter_map_plot
import tqdm
import warnings
from build_speedmaps_index import ANALYSIS_DATE


def check_map_gen(row, pbar):
    '''
    Call using pd.apply for convienient iteration.
    Save progress to parquet after attempting each agency's map in case script is interrupted
    That progress parquet (when complete) is used in next script, actual output is ignored
    '''
    
    global speedmaps_index_joined
    analysis_date = row.analysis_date
    progress_path = f'./_rt_progress_{analysis_date}.parquet'
        
    if row.status not in ('parser_failed', 'map_confirmed'):
        try:
            rt_day = rt_filter_map_plot.from_gcs(row.organization_itp_id,
                                                       analysis_date, pbar)
            rt_day.set_filter(start_time='06:00', end_time='09:00')
            _m = rt_day.segment_speed_map()
            row.status = 'map_confirmed'
        except Exception as e:
            print(f'{row.organization_itp_id} map test failed: {e}')
            row.status = 'map_failed'
        speedmaps_index_joined.loc[row.name] = row
        speedmaps_index_joined.to_parquet(progress_path)
    
    return

if __name__ == "__main__":
    
    speedmaps_index_joined = rt_utils.check_intermediate_data(
        analysis_date = ANALYSIS_DATE)
    # check if this stage needed
    if speedmaps_index_joined.status.isin(['map_confirmed', 'map_failed', 'parser_failed']).all():
        print('already attempted to test all maps:')
    else:
        pbar = tqdm.tqdm()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            _ = speedmaps_index_joined.apply(check_map_gen, axis = 1, args=[pbar])
            print()
            print('map testing complete:')
            print(speedmaps_index_joined.status.value_counts())