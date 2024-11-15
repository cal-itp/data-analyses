import pandas as pd
from siuba import *
import numpy as np
import geopandas as gpd
import update_vars_index
from shared_utils import rt_utils
from calitp_data_analysis.geography_utils import CA_NAD83Albers

catalog = shared_utils.catalog_utils.get_catalog('gtfs_analytics_data')

#def read_detail_segments(itp_id: int) -> gpd.GeoDataFrame:
#    '''
#    Read detailed speedmap segments (all times of day including interpolated segs)
#    for a given itp_id (legacy compatability, may switch to an alternate identifer...)
#    '''
#    speedmap_index = pd.read_parquet(f'_rt_progress_{analysis_date}.parquet') >> filter(_.organization_itp_id == itp_id)
#    path = f'{catalog.speedmap_segments.dir}{catalog.speedmap_segments.shape_stop_single_segment_detail}_{analysis_date}.parquet'
#    speedmap_segs = gpd.read_parquet(path) #  aggregated
#    speedmap_segs = speedmap_segs >> filter(_.schedule_gtfs_dataset_key == speedmap_index.schedule_gtfs_dataset_key.iloc[0])
#    
#    return speedmap_segs

def prepare_segment_gdf(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    '''
    Project segment speeds gdf and add column for rich speedmap display
    '''
    gdf = gdf.to_crs(CA_NAD83Albers)
    #  TODO move upstream and investigate
    gdf['fast_slow_ratio'] = gdf.p80_mph / gdf.p20_mph
    gdf.fast_slow_ratio = gdf.fast_slow_ratio.replace(np.inf, 3)
    gdf = gdf.round(1)

    ## shift to right side of road to display direction
    gdf.geometry = gdf.geometry.apply(rt_utils.try_parallel)
    gdf = gdf.apply(rt_utils.arrowize_by_frequency, axis=1, frequency_col='trips_hr_sch')

    gdf = gdf >> arrange(_.trips_hr_sch)

    return gdf

def render_spa_link(spa_map_url: str, text='Full Map') -> None:
    
    display(Markdown(f'<a href="{spa_map_url}" target="_blank">Open {text} in New Tab</a>'))
    return

def display_spa_map(spa_map_url: str, width: int=1000, height: int=650) -> None:
    '''
    Display map from external simple web app in the notebook/JupyterBook context via an IFrame.
    Will show most recent map set using self.map_gz_export
    Width/height defaults are current best option for JupyterBook, don't change for portfolio use
    width, height: int (pixels)
    '''
    i = IFrame(spa_map_url, width=width, height=height)
    display(i)
    return

def map_shn(district_gdf: gpd.GeoDataFrame):
    dist = district_gdf.District.iloc[0]
    filename = f'{dist}_SHN'
    title = f"D{dist} State Highway Network"
    
    export_result = set_state_export(district_gdf, subfolder = update_vars_index.GEOJSON_SUBFOLDER, filename = filename,
                        map_type = 'state_highway_network', map_title = title)
    spa_map_state = export_result['state_dict']
    return spa_map_state

#from shared_utils.rt_utils import ACCESS_ZERO_THIRTY_COLORSCALE, , , 
def map_time_period(district_gdf: gpd.GeoDataFrame, speedmap_segs: gpd.GeoDataFrame, time_of_day: str,
                    map_type: str):
    '''
    Always add State Highway Network first.
    '''
    time_of_day_lower = time_of_day.lower().replace(' ', '_')
    gdf = gdf >> filter(_.time_of_day == time_of_day)
    color_col = {'new_speedmap': 'p20_mph', 'new_speed_variation': 'fast_slow_ratio'}[map_type]
    shn_state = map_shn(district_gdf)
    filename = f"{speedmap_segs.organization_source_record_id.iloc[0]}_{map_type}"
    
    if map_type == 'new_speedmap':
        cmap = rt_utils.ACCESS_ZERO_THIRTY_COLORSCALE
        legend_url = rt_utils.ACCESS_SPEEDMAP_LEGEND_URL
    elif map_type == 'new_speed_variation'
        cmap = rt_utils.VARIANCE_FIXED_COLORSCALE
        legend_url = rt_utils.VARIANCE_LEGEND_URL
        
    speedmap_state = rt_utils.set_state_export(
        period_test, subfolder = update_vars_index.GEOJSON_SUBFOLDER, filename=filename,
        map_type=map_type,
        color_col=color_col, cmap=cmap, legend_url=legend_url,
        cache_seconds=0, map_title=f'Speedmap Segs {time_of_day} {analysis_date}',
        existing_state = shn_state)