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

from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
gcsgp = GCSGeoPandas()

catalog = catalog_utils.get_catalog('gtfs_analytics_data')

def datetime_to_rt_date_key(datetime: dt.datetime, day_offset: int = 0) -> str:
    '''
    using a datetime object and optional day offset,
    compose string key to rt_dates.DATES
    '''
    datetime = datetime + dt.timedelta(days = day_offset)
    return datetime.strftime('%b%Y').lower()

#  default is current month if available in rt_dates then 3 months of lookback
# ANALYSIS_DATE_LIST = [datetime_to_rt_date_key(dt.datetime.now(), x) for x in range(0, -91, -30)]
# ANALYSIS_DATE_LIST = [rt_dates.DATES[key] for key in ANALYSIS_DATE_LIST if key in rt_dates.DATES.keys()]
ANALYSIS_DATE_LIST = [rt_dates.DATES['sep2025']]
PROGRESS_PATH = f'./_rt_progress_{ANALYSIS_DATE_LIST[0]}.parquet'
SPEED_SEGS_PATH = f'{catalog.speedmap_segments.dir}{catalog.speedmap_segments.segment_timeofday}'
GEOJSON_SUBFOLDER = f'segment_speeds_{ANALYSIS_DATE_LIST[0]}/'

def read_segs(analysis_date: str) -> gpd.GeoDataFrame:
    '''
    read speedmap segments from gcs and keep one row per organization x feed
    '''
    path = f'{SPEED_SEGS_PATH}_{analysis_date}.parquet'
    # path = './speedmaps_analysis_name_test.parquet'
    # print(f'testing with {path}')
    org_cols = ['analysis_name', 'name', 'base64_url', 'caltrans_district']
    # speedmap_segs = gpd.read_parquet(path)
    speedmap_segs = gcsgp.read_parquet(path)
    speedmap_segs = speedmap_segs[org_cols].drop_duplicates().reset_index(drop=True)
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
    speedmap_segs['status'] = 'speedmap_segs_available'
    return speedmap_segs

if __name__ == "__main__":
    
    print(f'analysis date from shared_utils/rt_dates: {ANALYSIS_DATE_LIST[0]}')
    print(f'will append from these previous dates if missing: {ANALYSIS_DATE_LIST[1:]}')
    with open ('../gtfs_funnel/published_operators.yml', 'r') as f:
        operators = yaml.safe_load(f)
        operators = {key.isoformat(): operators[key] for key in operators.keys()}
    speedmaps_index = build_speedmaps_index(ANALYSIS_DATE_LIST, operators)
    speedmaps_index.to_parquet(PROGRESS_PATH)