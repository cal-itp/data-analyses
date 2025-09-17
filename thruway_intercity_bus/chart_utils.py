import pandas as pd
import numpy as np
import shapely
import branca
from itertools import cycle
import altair as alt
alt.data_transformers.enable("vegafusion")

from shared_utils import rt_utils

blu = branca.colormap.linear.Blues_09
grn = branca.colormap.linear.Greens_09
orn = branca.colormap.linear.Oranges_09
red = branca.colormap.linear.Reds_09
gray = branca.colormap.linear.Greys_09
all_cmaps = [blu, grn, orn, red, gray]
#  start with a more saturated color
all_cmaps = [branca.colormap.LinearColormap(colors=cmap.colors[2:]) for cmap in all_cmaps]

def transform_gtfs_info(one_direction_st_df: pd.DataFrame, shape_geom: shapely.Geometry) -> pd.DataFrame:
    '''
    Using stop_times and a shape geometry for a representative trip, linear project stop locations to shape.
    
    Create mi_range column for all stops except the final stop.
    '''
    df = one_direction_st_df.assign(stop_meters = one_direction_st_df.geometry.map(lambda x: shape_geom.project(x)))
    df = df.assign(stop_miles = np.round((df.stop_meters / rt_utils.METERS_PER_MILE), 0).astype(int))
    df = df.assign(next_miles = df.stop_miles.shift(-1))
    
    mi_range = lambda row: np.arange(row.stop_miles, row.next_miles) if not np.isnan(row.stop_miles) and not np.isnan(row.next_miles) else None
    df = df.assign(mile_range = df.apply(mi_range, axis=1))

    return df

def explode_gtfs_info(gtfs_info_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Use mi_range column from transform_gtfs_info to explode df to have one row for each mile travelled
    alongside stop_times info.
    '''
    gtfs_info_df = gtfs_info_df[['amtrak_stop', 'stop_sequence','route_id',
                     'route_short_name', 'shape_id', 'direction_id',
                        'mile_range']].explode(column='mile_range').rename(columns={'mile_range': 'route_mileage'})
    return gtfs_info_df

def stop_sequence_dict_and_direction_id(gtfs_info_df: pd.DataFrame) -> tuple:
    '''
    Get useful data structures for manipulating o/d ridership data from Amtrak.
    
    Returns a dictionary of amtrak ridership data stops to stop sequences and the direction id from GTFS
    '''
    df = gtfs_info_df[['amtrak_stop', 'stop_sequence', 'direction_id']].drop_duplicates()
    assert len(df.direction_id.unique()) == 1
    direction_id = df.direction_id.iloc[0]
    sequence_dict = df[['amtrak_stop', 'stop_sequence']].set_index('amtrak_stop').stop_sequence.to_dict()
    
    return sequence_dict, direction_id

def determine_direction(row: pd.Series, gtfs_info_df: pd.DataFrame) -> str:
    '''
    Use dictionary of amtrak ridership data stops to GTFS stop sequences to match
    each origin/destination pair to a direction_id.
    '''
    sequence_dict, direction_id = stop_sequence_dict_and_direction_id(gtfs_info_df)
    if sequence_dict[row.orig] < sequence_dict[row.dest]:
        return direction_id
    elif sequence_dict[row.orig] > sequence_dict[row.dest]:
        return 1 - direction_id
    else:
        return ''
    
def filter_ridership(source_ridership_df: pd.DataFrame, rider_bus_route: str, gtfs_info_df: pd.DataFrame):
    '''
    Determine direction of each Amtrak ridership data o/d pair,
    filter to a single direction (and route)
    '''
    sequence_dict, direction_id = stop_sequence_dict_and_direction_id(gtfs_info_df)
    df = source_ridership_df.query('orig.isin(@gtfs_info_df.amtrak_stop) & dest.isin(@gtfs_info_df.amtrak_stop)')
    if rider_bus_route:
        df = df.query('ca_bus_route == @rider_bus_route')
    df = df.assign(direction_id = df.apply(determine_direction, axis=1, gtfs_info_df = gtfs_info_df),
                orig_seq = df.orig.apply(lambda x: sequence_dict[x]),
                dest_seq = df.dest.apply(lambda x: sequence_dict[x])
                                  )
    df = df.query('direction_id == @direction_id')
    return df

def running_ridership(filtered_ridership_df: pd.DataFrame, gtfs_info_df: pd.DataFrame):
    '''
    Explode ridership dataframe to keep a running count of riders onboard at any location
    (even if they did not board there)
    '''
    sequence_dict, direction_id = stop_sequence_dict_and_direction_id(gtfs_info_df)
    running_df = []
    for stn in sequence_dict.keys():
        stn_seq = sequence_dict[stn]
        if stn_seq < max(sequence_dict.values()):
            df_at = filtered_ridership_df.query('orig_seq <= @stn_seq & dest_seq > @stn_seq').assign(departing_station = stn)
            running_df += [df_at]
    return pd.concat(running_df).sort_values('departing_station')

def create_cmap_mapping(cmap_list: list, unique_values_to_map: list) -> dict:
    '''
    Map a colormap to each unique origin value. Wrap around colormaps and duplicate
    if there are more origins than colormaps.
    '''
    #  https://stackoverflow.com/questions/54864170/repeat-items-in-list-to-required-length
    x = cycle(cmap_list)
    cmap_per_origin = [next(x) for origin in unique_values_to_map]

    origin_cmaps = dict(zip(unique_values_to_map, cmap_per_origin))
    return origin_cmaps

def scale_transform_cmaps(cmap_list: list, cmap_origin_dict: dict, with_distance_df: pd.DataFrame) -> list:
    '''
    Scale each colormap to the max and minimum o/d distances of each origin sharing the cmap,
    since there may be multiple.
    '''
    #  this works but could refactor...
    scaled_cmaps = []
    for cmap in cmap_list:
        min_dist = 10000
        max_dist = 0
        #  only consider origins sharing this cmap
        for origin in [key for key in cmap_origin_dict.keys() if cmap_origin_dict[key] == cmap]:
            origin_min = with_distance_df.query('orig == @origin').od_miles.min()
            origin_max = with_distance_df.query('orig == @origin').od_miles.max()
            min_dist = min(origin_min, min_dist)
            max_dist = max(origin_max, max_dist)
        if min_dist == max_dist:
            min_dist = min_dist - 1
            max_dist = max_dist + 1 #  adjust if equal to use midtone color
        # print(min_dist, end = '->')
        # print(max_dist)
        # display(cmap)
        scaled_cmaps += [cmap.scale(vmin = min_dist, vmax = max_dist)]
        
    return scaled_cmaps

def color_from_od_row(row: pd.Series, scaled_cmaps_dict: dict, index_df: pd.DataFrame):
    '''
    Use a df row with origin and od_miles (o/d distance) to get a rgb hex value
    from the relevant colormap.
    '''
    cmap = scaled_cmaps_dict[row.orig]
    color = cmap.rgb_hex_str(row.od_miles)
    return color

def color_by_origin_and_distance(with_distance_df: pd.DataFrame, cmaps: list) -> alt.Scale:
    '''
    Create an Altair Scale (colorscale) with 
    '''
    unique_origins = with_distance_df.orig.unique()
    origin_cmaps = create_cmap_mapping(cmaps, unique_origins)
    scaled_cmaps = scale_transform_cmaps(cmap_list=cmaps, cmap_origin_dict=origin_cmaps, with_distance_df=with_distance_df)
    origin_cmaps_scaled = create_cmap_mapping(cmap_list=scaled_cmaps, unique_values_to_map=unique_origins)
    
    df = with_distance_df[['od', 'orig', 'od_miles']].drop_duplicates()
    color_index_df = df.assign(color = df.apply(color_from_od_row, axis = 1, scaled_cmaps_dict = origin_cmaps_scaled, index_df = df))
    color_dict = dict(zip(color_index_df.od, color_index_df.color))
    colorscale = alt.Scale(domain = color_dict.keys(), range = color_dict.values())
    
    return colorscale

def chart_rider_flow(with_distance_df, color_args={}):

    flow = alt.Chart(with_distance_df).mark_area().encode(
    alt.X('route_mileage:Q').title('Route Mileage'),
    alt.Y('sum(ridership):Q').axis().title('Passenger Load (total monthly)'),
    # color=alt.Color('od', scale=scale),
    color=alt.Color('od', legend=alt.Legend(symbolLimit=0, labelFontSize=12, titleFontSize=14), **color_args),
    tooltip = ['departing_station', 'od', 'ridership'],
        ).properties(width = with_distance_df.route_mileage.max() * 3, height = 600)
    flow = flow.configure_axis(labelFontSize=14, titleFontSize=16)
    return flow

def flow_chart_from_shape_trip_row(row: pd.Series, stop_times: pd.DataFrame, ridership: pd.DataFrame,
                                  ridership_data_route: str = None):
    trip_st = stop_times.query('trip_id == @row.trip_id')
    # print(trip_st.columns)
    # print(trip_st.route_id.iloc[0])
    if trip_st.route_id.iloc[0] == '1c':
        print('correcting LAX to SMN for Route 1c') #  is this a GTFS error?
        trip_st = trip_st.assign(amtrak_stop = trip_st.amtrak_stop.str.replace('LAX', 'SMN'))
    gtfs_info_df = transform_gtfs_info(trip_st, row.geometry)
    gtfs_info_df = explode_gtfs_info(gtfs_info_df)
    rider_one_rt_dir = filter_ridership(source_ridership_df = ridership, rider_bus_route=ridership_data_route, gtfs_info_df=gtfs_info_df)
    rider_one_rt_dir = running_ridership(filtered_ridership_df = rider_one_rt_dir, gtfs_info_df=gtfs_info_df)
    with_distance = rider_one_rt_dir.merge(
        gtfs_info_df[['amtrak_stop', 'route_mileage']], left_on = 'departing_station', right_on = 'amtrak_stop')
    with_distance = with_distance.assign(od_miles = with_distance.od.map(with_distance.groupby('od')[['route_mileage']].size()))
    try:
        colorscale = color_by_origin_and_distance(with_distance_df=with_distance, cmaps=all_cmaps)
        chart = chart_rider_flow(with_distance, color_args={'scale': colorscale})
    except Exception as e:
        print('custom coloring failed, trying default colors')
        print(e)
        chart = chart_rider_flow(with_distance)
    return chart