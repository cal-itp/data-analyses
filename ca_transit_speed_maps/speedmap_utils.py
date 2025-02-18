import pandas as pd
from siuba import *
import numpy as np
import geopandas as gpd
import update_vars_index
from shared_utils import rt_utils, catalog_utils
from segment_speed_utils import helpers
from calitp_data_analysis.geography_utils import CA_NAD83Albers_m
import datetime as dt
import altair as alt
from IPython.display import display, Markdown, IFrame
catalog = catalog_utils.get_catalog('gtfs_analytics_data')

def read_segments_shn(organization_source_record_id: str) -> (gpd.GeoDataFrame, gpd.GeoDataFrame):
    '''
    Get filtered detailed speedmap segments for an organization, and relevant district SHN.
    '''
    path = f'{catalog.speedmap_segments.dir}{catalog.speedmap_segments.shape_stop_single_segment_detail}_{update_vars_index.ANALYSIS_DATE}.parquet'
    # path = f'{catalog.stop_segments.dir}{catalog.stop_segments.route_dir_single_segment_detail}_{update_vars_index.ANALYSIS_DATE}.parquet'
    speedmap_segs = gpd.read_parquet(path, filters=[['organization_source_record_id', '==', organization_source_record_id]]) #  aggregated
    assert (speedmap_segs
    >> select(-_.route_short_name, -_.direction_id)).isna().any().any() == False, 'no cols besides route_short_name, direction_id should be nan'
    speedmap_segs = prepare_segment_gdf(speedmap_segs)
    shn = gpd.read_parquet(rt_utils.SHN_PATH)
    this_shn = shn >> filter(_.District.isin([int(x[:2]) for x in speedmap_segs.caltrans_district.unique()]))
    
    return (speedmap_segs, this_shn)

def read_shapes(speedmap_segs: gpd.GeoDataFrame):

    shapes = helpers.import_scheduled_shapes(update_vars_index.ANALYSIS_DATE, columns=['shape_array_key', 'geometry'])
    trips = helpers.import_scheduled_trips(update_vars_index.ANALYSIS_DATE, columns=['shape_array_key', 'shape_id', 'route_id',
                                                                   'route_short_name', 'gtfs_dataset_key']).drop_duplicates()
    shapes = shapes.merge(trips, on='shape_array_key')
    org_shapes = shapes.merge(speedmap_segs[['schedule_gtfs_dataset_key']].drop_duplicates(),
                              on='schedule_gtfs_dataset_key')
    return org_shapes

def prepare_segment_gdf(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    '''
    Project segment speeds gdf and add column for rich speedmap display
    '''
    gdf = gdf.to_crs(CA_NAD83Albers_m)
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
    
    export_result = rt_utils.set_state_export(district_gdf, subfolder = update_vars_index.GEOJSON_SUBFOLDER, filename = filename,
                        map_type = 'state_highway_network', map_title = title)
    spa_map_state = export_result['state_dict']
    return spa_map_state

def map_excluded_shapes(existing_state: dict, speedmap_segs: gpd.GeoDataFrame, shapes_gdf: gpd.GeoDataFrame,
                       time_of_day: str, analysis_date: dt.date):
    '''
    
    '''
    display_date = analysis_date.strftime('%B %d %Y (%A)')
    filename = f"{analysis_date}_{speedmap_segs.organization_source_record_id.iloc[0]}_excluded_shapes_{time_of_day}"
    title = f"{speedmap_segs.organization_name.iloc[0]} {display_date} Excluded Shapes {time_of_day}"

    shapes_gdf = shapes_gdf[['shape_id', 'route_id', 'route_short_name', 'geometry']]
    speedmap_segs = speedmap_segs.dissolve()
    speedmap_segs.geometry = speedmap_segs.buffer(35) #  slightly bigger than parallel_offset in rt_utils
    excluded_shapes = shapes_gdf.overlay(speedmap_segs, how='difference')
    excluded_shapes['color'] = [(50,50,50) for _ in excluded_shapes.iterrows()] #  make it dark gray!
    excluded_shapes['info'] = "No data in time period"
    excluded_shapes.geometry = excluded_shapes.buffer(8) #  for display
    
    if excluded_shapes.empty:
        return {}
    export_result = rt_utils.set_state_export(excluded_shapes, subfolder = update_vars_index.GEOJSON_SUBFOLDER, filename = filename,
                        map_title = title, existing_state = existing_state)
    
    return export_result['state_dict']


def map_time_period(district_gdf: gpd.GeoDataFrame, speedmap_segs: gpd.GeoDataFrame, org_shapes: gpd.GeoDataFrame,
                    analysis_date: dt.date, time_of_day: str, map_type: str):
    '''
    Always add State Highway Network first.
    '''
    time_of_day_lower = time_of_day.lower().replace(' ', '_')
    speedmap_segs = speedmap_segs >> filter(_.time_of_day == time_of_day)
    if speedmap_segs.empty:
        return None
    color_col = {'new_speedmap': 'p20_mph', 'new_speed_variation': 'fast_slow_ratio'}[map_type]
    shn_state = map_shn(district_gdf)
    excluded_shapes_state = map_excluded_shapes(shn_state, speedmap_segs, org_shapes,
                                                time_of_day, analysis_date)
    
    display_date = analysis_date.strftime('%B %d %Y (%A)')
    filename = f"{analysis_date}_{speedmap_segs.organization_source_record_id.iloc[0]}_{map_type}_{time_of_day}"
    title = f"{speedmap_segs.organization_name.iloc[0]} {display_date} {time_of_day}"
    
    if map_type == 'new_speedmap':
        cmap = rt_utils.ACCESS_ZERO_THIRTY_COLORSCALE
        legend_url = rt_utils.ACCESS_SPEEDMAP_LEGEND_URL
    elif map_type == 'new_speed_variation':
        cmap = rt_utils.VARIANCE_FIXED_COLORSCALE
        legend_url = rt_utils.VARIANCE_LEGEND_URL
        
    export_result = rt_utils.set_state_export(
        speedmap_segs, subfolder = update_vars_index.GEOJSON_SUBFOLDER, filename=filename,
        map_type=map_type,
        color_col=color_col, cmap=cmap, legend_url=legend_url,
        map_title=title,
        existing_state = excluded_shapes_state)
    
    spa_link = export_result['spa_link'] 
    return spa_link

def chart_speeds_by_time_period(speedmap_segs: gpd.GeoDataFrame) -> None:
    '''
    Use Altair to chart p20,p50,p80 speeds by time of day.
    Match speedmap colorscale.
    '''
    cmap = rt_utils.ACCESS_ZERO_THIRTY_COLORSCALE
    domain = cmap.index
    range_ = [cmap.rgb_hex_str(i) for i in cmap.index]
    df = speedmap_segs[['time_of_day', 'p50_mph', 'p20_mph', 'p80_mph']]
    df = df >> group_by(_.time_of_day) >> summarize(p50_mph = _.p50_mph.quantile(.5),
                                                   p20_mph = _.p20_mph.quantile(.5),
                                                   p80_mph = _.p80_mph.quantile(.5),)
    df['p50 - p20'] = -(df['p50_mph'] - df['p20_mph'])
    df['p80 - p50'] = df['p80_mph'] - df['p50_mph']
    error_bars = alt.Chart(df).mark_errorbar(thickness=5, color='gray', opacity=.6).encode(
        y = alt.Y("p50_mph:Q", title='Segment Speed (mph): 20, 50, 80%ile'),
        yError=("p50 - p20:Q"),
        yError2=("p80 - p50:Q"),
        x = alt.X("time_of_day:N", sort=['Early AM', 'AM Peak', 'Midday', 'PM Peak', 'Evening', 'Owl']),
        tooltip=[alt.Tooltip('p20_mph:Q', title="p20 mph"), alt.Tooltip('p50_mph:Q', title="p50 mph"),
                alt.Tooltip('p80_mph:Q', title="p80 mph")]
    ).properties(width=400)
    points = alt.Chart(df).mark_point(filled=True, size = 300, opacity = 1).encode(
        alt.Y("p50_mph:Q"),
        alt.X("time_of_day:N", sort=['Early AM', 'AM Peak', 'Midday', 'PM Peak', 'Evening', 'Owl'],
             title='Time of Day'),
        color=alt.Color('p50_mph', title='Median Segment Speed (mph)').scale(domain=domain, range = range_),
        tooltip=[alt.Tooltip('p50_mph:Q', title="p50 mph")],
    )
    chart = error_bars + points
    chart = chart.configure(axis = alt.AxisConfig(labelFontSize=14, titleFontSize=18),
                           legend = alt.LegendConfig(titleFontSize=14, labelFontSize=14, titleLimit=250,
                                                     titleOrient='left', labelOffset=100))
    display(chart)
    return