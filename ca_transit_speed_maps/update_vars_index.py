import pandas as pd
import geopandas as gpd
import datetime as dt

from shared_utils import rt_dates, catalog_utils, schedule_rt_utils

from segment_speed_utils.project_vars import (
    COMPILED_CACHED_VIEWS,
    PROJECT_CRS,
    SEGMENT_GCS,
)

catalog = catalog_utils.get_catalog('gtfs_analytics_data')

ANALYSIS_DATE = dt.date.fromisoformat(rt_dates.DATES['mar2025'])
PROGRESS_PATH = f'./_rt_progress_{ANALYSIS_DATE}.parquet'
GEOJSON_SUBFOLDER = f'segment_speeds_{ANALYSIS_DATE}/'

def build_speedmaps_index(analysis_date: dt.date) -> pd.DataFrame:
    '''
    An index table for tracking down a given org's schedule/rt feeds.
    Note that in limited cases, multiple orgs may share the same datasets
    (VCTC combined feeds, SD Airport and SDMTS...)
    '''
    path = f'{catalog.speedmap_segments.dir}{catalog.speedmap_segments.segment_timeofday}_{analysis_date}.parquet'
    org_cols = ['organization_name', 'organization_source_record_id', 'name', 'base64_url']
    speedmap_segs = gpd.read_parquet(path)[org_cols].drop_duplicates().reset_index(drop=True)
    districts = schedule_rt_utils.filter_dim_county_geography(analysis_date)
    new_ix = speedmap_segs.merge(districts, on = 'organization_name')
    new_ix['status'] = 'speedmap_segs_available'
    new_ix['analysis_date'] = analysis_date
    return new_ix

if __name__ == "__main__":
    
    print(f'analysis date from shared_utils/rt_dates: {ANALYSIS_DATE}')
    speedmaps_index = build_speedmaps_index(ANALYSIS_DATE)
    speedmaps_index.to_parquet(PROGRESS_PATH)