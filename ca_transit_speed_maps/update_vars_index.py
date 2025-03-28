import pandas as pd
import geopandas as gpd
import datetime as dt
import yaml

from shared_utils import rt_dates, catalog_utils, schedule_rt_utils

from segment_speed_utils.project_vars import (
    COMPILED_CACHED_VIEWS,
    PROJECT_CRS,
    SEGMENT_GCS,
)

catalog = catalog_utils.get_catalog('gtfs_analytics_data')

def datetime_to_rt_date_key(datetime: dt.datetime, day_offset: int = 0) -> str:
    '''
    using a datetime object and optional day offset,
    compose string key to rt_dates.DATES
    '''
    datetime = datetime + dt.timedelta(days = day_offset)
    return datetime.strftime('%b%Y').lower()

#  default is current month then 2 months of lookback
ANALYSIS_DATE_LIST = [datetime_to_rt_date_key(dt.datetime.now(), x) for x in range(0, -61, -30)]
ANALYSIS_DATE_LIST = [rt_dates.DATES[key] for key in ANALYSIS_DATE_LIST]
PROGRESS_PATH = f'./_rt_progress_{ANALYSIS_DATE_LIST[0]}.parquet'
SPEED_SEGS_PATH = f'{catalog.speedmap_segments.dir}{catalog.speedmap_segments.segment_timeofday}'
GEOJSON_SUBFOLDER = f'segment_speeds_{ANALYSIS_DATE_LIST[0]}/'

def read_segs(analysis_date: str) -> gpd.GeoDataFrame:
    '''
    read speedmap segments from gcs and keep one row per organization x feed
    '''
    path = f'{SPEED_SEGS_PATH}_{analysis_date}.parquet'
    org_cols = ['organization_name', 'organization_source_record_id', 'name', 'base64_url']
    speedmap_segs = gpd.read_parquet(path)[org_cols].drop_duplicates().reset_index(drop=True)
    return speedmap_segs

def append_previous(speedmap_segs: pd.DataFrame, date: str, operators: dict) -> pd.DataFrame():
    '''
    operators: dict of the most recent rt_date an operator's feed was seen,
    currently via '../gtfs_funnel/published_operators.yml'
    '''
    previous_segs = read_segs(date).query('name.isin(@operators[@date])')
    previous_segs['analysis_date'] = date
    speedmap_segs = pd.concat([speedmap_segs, previous_segs])
    return speedmap_segs

def build_speedmaps_index(analysis_date_list: dt.date, operators: dict) -> pd.DataFrame:
    '''
    An index table for tracking down a given org's schedule/rt feeds.
    Note that in limited cases, multiple orgs may share the same datasets
    (VCTC combined feeds, SD Airport and SDMTS...)
    '''
    speedmap_segs = pd.DataFrame()
    for i in range(len(analysis_date_list)):
        speedmap_segs = append_previous(speedmap_segs, analysis_date_list[i], operators)
    districts = schedule_rt_utils.filter_dim_county_geography(analysis_date_list[0])
    new_ix = speedmap_segs.merge(districts, on = 'organization_name')
    new_ix['status'] = 'speedmap_segs_available'
    return new_ix

if __name__ == "__main__":
    
    print(f'analysis date from shared_utils/rt_dates: {ANALYSIS_DATE_LIST[0]}')
    print(f'will append from these previous dates if missing: {ANALYSIS_DATE_LIST[1:]}')
    with open ('../gtfs_funnel/published_operators.yml', 'r') as f:
        operators = yaml.safe_load(f)
        operators = {key.isoformat(): operators[key] for key in operators.keys()}
    speedmaps_index = build_speedmaps_index(ANALYSIS_DATE_LIST, operators)
    speedmaps_index.to_parquet(PROGRESS_PATH)