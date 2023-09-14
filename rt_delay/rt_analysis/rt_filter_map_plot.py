from shared_utils.geography_utils import WGS84, CA_NAD83Albers
from shared_utils.rt_utils import *
from shared_utils.utils import geoparquet_gcs_export
from shared_utils.calitp_color_palette import CALITP_CATEGORY_BOLD_COLORS, CALITP_CATEGORY_BRIGHT_COLORS
import branca
import mapclassify

from siuba import *

import pandas as pd
import geopandas as gpd
import shapely
import folium

import datetime as dt
from tqdm import tqdm

import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

from IPython.display import display, Markdown, IFrame

import gzip
import base64
import json
from calitp_data_analysis import get_fs
import warnings

class RtFilterMapper:
    '''
    Collects filtering and mapping functions, init with stop segment speed view and rt_trips
    '''
    def __init__(self, rt_trips, stop_delay_view,
                 shapes, pbar = None):
        self.debug_dict = {}
        self.pbar = pbar
        self.rt_trips = rt_trips
        self.calitp_itp_id = self.rt_trips.calitp_itp_id.iloc[0]
        self.stop_delay_view = stop_delay_view
        # # below line fixes interpolated segment mapping for 10/12 data, fixed upstream for future dates
        # self.stop_delay_view = self.stop_delay_view >> group_by(_.shape_id) >> mutate(direction_id = _.direction_id.ffill()) >> ungroup()
        self.shapes = shapes
        self.using_v2 = 'organization_name' in rt_trips.columns
        if self.using_v2:
            if 'activity_date' in rt_trips.columns:
                # raise Exception('rerun intermediate data')
                # temporarily accomodate 3/15/22, 4/12/22 intermediates generated with activity_date
                self.analysis_date = rt_trips.activity_date.iloc[0] #v2 warehouse
            else:
                self.analysis_date = rt_trips.service_date.iloc[0]
            self.organization_name = rt_trips.organization_name.iloc[0]
            self.caltrans_district = self.rt_trips.caltrans_district.iloc[0]
            self.ct_dist_numeric = int(self.caltrans_district.split(' ')[0])
            self.shn = (gpd.read_parquet(SHN_PATH)
                        >> select(_.Route, _.County, _.District,
                                  _.RouteType, _.geometry)
                        >> filter(_.District == self.ct_dist_numeric)
                       )
        else: # v1 compatibility
            self.analysis_date = rt_trips.service_date.iloc[0]
            self.organization_name = rt_trips.calitp_agency_name.iloc[0]
        self.display_date = self.analysis_date.strftime('%b %d, %Y (%a)')
        self.transit_priority_target_mph = 16
        self.endpoint_delay_view = (self.stop_delay_view
                      >> group_by(_.trip_id)
                      >> filter(_.stop_sequence == _.stop_sequence.max())
                      >> ungroup()
                      >> mutate(arrival_hour = _.arrival_time.apply(lambda x: x.hour))
                      >> inner_join(_, self.rt_trips >> select(_.trip_id, _.mean_speed_mph), on = 'trip_id')
                     )
        self.endpoint_delay_summary = (self.endpoint_delay_view
                      >> group_by(_.direction_id, _.route_id, _.arrival_hour)
                      >> summarize(n_trips = _.route_id.size, mean_end_delay_seconds = _.delay_seconds.mean())
                     )
        self.reset_filter()
        self.pbar_desc = f'itp_id: {self.calitp_itp_id} org: {self.organization_name[:15]}'

    def set_filter(self, start_time = None, end_time = None, route_names = None,
                   shape_ids = None, direction_id = None, direction = None, trip_ids = None,
                   route_types = None
                  ):
        '''
        start_time, end_time: string %H:%M, for example '11:00' and '14:00'
        route_names: list or pd.Series of route_names (GTFS route_short_name)
        direction_id: '0' or '1'
        direction: string 'Northbound', 'Eastbound', 'Southbound', 'Westbound' (experimental)
        trip_ids: list or pd.Series of trip_ids (GTFS trip_ids)
        route_types: list or pd.Series of route_type
        '''
        route_names = list(route_names) if isinstance(route_names, pd.Series) else route_names
        route_types = list(route_types) if isinstance(route_types, pd.Series) else route_types
        trip_ids = list(trip_ids) if isinstance(trip_ids, pd.Series) else trip_ids
        args = [start_time, end_time, route_names, direction_id, direction, shape_ids, trip_ids, route_types]
        assert any(args), 'must supply at least 1 argument to filter'
        assert not start_time or type(dt.datetime.strptime(start_time, '%H:%M') == type(dt.datetime)), 'invalid time string'
        assert not end_time or type(dt.datetime.strptime(end_time, '%H:%M') == type(dt.datetime)), 'invalid time string'
        if route_types:
            assert pd.Series(route_types).isin(self.rt_trips.route_type).all(), 'at least 1 route type not found in self.rt_trips'
        if route_names:
            assert pd.Series(route_names).isin(self.rt_trips.route_short_name).all(), 'at least 1 route not found in self.rt_trips'
        assert not direction_id or direction_id in ['0', '1', 0, 1, 0.0, 1.0]
        if direction_id:
            direction_id = float(direction_id)
        self.filter = {}
        if start_time:
            self.filter['start_time'] = dt.datetime.strptime(start_time, '%H:%M').time()
        else:
            self.filter['start_time'] = None
        if end_time:
            self.filter['end_time'] = dt.datetime.strptime(end_time, '%H:%M').time()
        else:
            self.filter['end_time'] = None
        self.filter['route_names'] = route_names
        self.filter['route_types'] = route_types
        self.filter['shape_ids'] = shape_ids
        self.filter['trip_ids'] = trip_ids
        if shape_ids:
            shape_trips = (self.rt_trips >> filter(_.shape_id.isin(shape_ids))
                           >> distinct(_.route_short_name)
                          )
            if not shape_trips.route_short_name.isnull().all():
                self.filter['route_names'] = list(shape_trips.route_short_name)
            if len(shape_ids) == 1:
                direction = (self.rt_trips >> filter(_.shape_id == shape_ids[0])).direction.iloc[0]
        if trip_ids:
            trips = (self.rt_trips >> filter(_.trip_id.isin(trip_ids))
                           >> distinct(_.route_short_name)
                          )
            if not trips.route_short_name.isnull().all():
                self.filter['route_names'] = list(trips.route_short_name)
            if len(trip_ids) == 1:
                direction = (self.rt_trips >> filter(_.trip_id == trip_ids[0])).direction.iloc[0]
        self.filter['direction_id'] = direction_id
        self.filter['direction'] = direction
        if start_time and end_time:
            self.hr_duration_in_filter = (dt.datetime.combine(self.analysis_date, self.filter['end_time'])
                                 - dt.datetime.combine(self.analysis_date, self.filter['start_time'])
                                ).seconds / 60**2
        else:
            self.hr_duration_in_filter = (self.stop_delay_view.actual_time.max() - 
                                         self.stop_delay_view.actual_time.min()).seconds / 60**2
        
        if self.filter['route_names'] and len(self.filter['route_names']) <= 5:
            rts = 'Route(s) ' + ', '.join(self.filter['route_names'])
        elif self.filter['route_names'] and len(self.filter['route_names']) > 5:
            rts = 'Multiple Routes'
        elif not self.filter['route_names']:
            rts = 'All Routes'
                
        if self.filter['route_types'] and len(self.filter['route_types']) <= 3:
            typename = [route_type_names[rttyp] for rttyp in self.filter['route_types']]
            typename_print = ' & '.join(typename)
        elif self.filter['route_types'] and len(self.filter['route_types']) > 3:
            typename_print = 'Multiple Route Types'
        elif not self.filter['route_types']:
            typename_print = 'All Route Types'
            
        if start_time and end_time and start_time == '15:00' and end_time == '19:00':
            self.filter_period = 'PM_Peak' # underscores enable use in filenames
        elif start_time and end_time and start_time == '06:00' and end_time == '09:00':
            self.filter_period = 'AM_Peak'
        elif start_time and end_time and start_time == '10:00' and end_time == '14:00':
            self.filter_period = 'Midday'
        elif not start_time and not end_time:
            self.filter_period = 'All_Day'
        else:
            self.filter_period = f'{start_time}–{end_time}'
        elements_ordered = [typename_print, rts, direction, self.filter_period, self.display_date]
        self.filter_formatted = ', ' + ', '.join([str(x).replace('_', ' ') for x in elements_ordered if x])
        
        self._time_only_filter = not route_types and not route_names and not shape_ids and not direction_id and not direction and not trip_ids
            
    def reset_filter(self):
        '''
        Clear filter.
        '''
        self.filter = None
        self._time_only_filter = True
        self.hr_duration_in_filter = (self.stop_delay_view.actual_time.max() - 
                                         self.stop_delay_view.actual_time.min()).seconds / 60**2
        self.filter_period = 'All_Day'
        rts = 'All Routes'
        elements_ordered = [rts, self.filter_period, self.display_date]
        self.filter_formatted = ', ' + ', '.join([str(x).replace('_', ' ') for x in elements_ordered if x])
    
    def _filter(self, df):
        '''Filters a df (containing trip_id) on trip_id based on any set filter.
        '''
        if not self.filter:
            return df
        else:
            trips = self.rt_trips.copy()
            # print(f'view filter: {self.filter}')
        if self.filter['start_time']:
            trips = trips >> filter(_.median_time > self.filter['start_time'])
        if self.filter['end_time']:
            trips = trips >> filter(_.median_time < self.filter['end_time'])
        if self.filter['route_names']:
            trips = trips >> filter(_.route_short_name.isin(self.filter['route_names']))
        if self.filter['route_types']:
            trips = trips >> filter(_.route_type.isin(self.filter['route_types']))
        if self.filter['shape_ids']:
            trips = trips >> filter(_.shape_id.isin(self.filter['shape_ids']))
        if self.filter['trip_ids']:
            trips = trips >> filter(_.trip_id.isin(self.filter['trip_ids']))
        if self.filter['direction_id']:
            trips = trips >> filter(_.direction_id == self.filter['direction_id'])
        if self.filter['direction']:
            trips = trips >> filter(_.direction == self.filter['direction'])
        return df.copy() >> inner_join(_, trips >> select(_.trip_id), on = 'trip_id')
    
    def add_corridor(self, corridor_gdf, manual_exclude = {}):
        '''
        Add ability to filter stop_delay_view and subsequently generated stop_segment_speed_views
        to trips running within a corridor, as specified by a polygon bounding box (corridor gdf).
        Enables calculation of metrics for SCCP, LPP.
        
        manual_exclude: dict in {'shape': {'min': int, 'max': int}, ...} format (values are stop sequence)
        '''
        corridor_gdf = corridor_gdf.to_crs(CA_NAD83Albers)
        if 'distance_meters' in corridor_gdf.columns:
            self.corridor = corridor_gdf >> select(_.geometry, _.distance_meters)
        else:
            self.corridor = corridor_gdf >> select(_.geometry)
        to_clip = self.stop_delay_view.drop_duplicates(subset=['shape_id', 'stop_sequence'])
        clipped = to_clip.clip(corridor_gdf)
        shape_sequences = (self.stop_delay_view
                          >> distinct(_.shape_id, _.stop_sequence)
                          >> arrange(_.shape_id, _.stop_sequence)
                         )
        stops_per_shape = shape_sequences >> group_by(_.shape_id) >> summarize(n_stops = _.shape_id.size)
        # can use this as # of stops in corr...
        stops_in_corr = clipped >> group_by(_.shape_id) >> summarize(in_corr = _.shape_id.size)
        stop_comparison = stops_per_shape >> inner_join(_, stops_in_corr, on='shape_id')
        # # only keep shapes with at least 10% of stops in corridor
        # stop_comparison = stop_comparison >> mutate(corr_shape = _.in_corr > _.n_stops / 10)
        # corr_shapes = stop_comparison >> filter(_.corr_shape)
        corr_shapes = stop_comparison # actually keep all

        self._shape_sequence_filter = {}
        for shape_id in corr_shapes.shape_id.unique():
            self._shape_sequence_filter[shape_id] = {}
            clipped_min = (clipped >> filter(_.shape_id == shape_id)).stop_sequence.min()
            clipped_max = (clipped >> filter(_.shape_id == shape_id)).stop_sequence.max()
            this_shape = shape_sequences >> filter(_.shape_id == shape_id)
            filter_min = (this_shape >> filter(_.stop_sequence < clipped_min)).stop_sequence.max()
            filter_max = (this_shape >> filter(_.stop_sequence > clipped_max)).stop_sequence.min()
            self._shape_sequence_filter[shape_id]['min'] = max(0, filter_min)
            self._shape_sequence_filter[shape_id]['max'] = filter_max if not np.isnan(filter_max) else clipped_max
            if manual_exclude and shape_id in manual_exclude.keys():
                if 'min' in manual_exclude[shape_id].keys():
                    self._shape_sequence_filter[shape_id]['min'] = manual_exclude[shape_id]['min']
                if 'max' in manual_exclude[shape_id].keys():
                    self._shape_sequence_filter[shape_id]['max'] = manual_exclude[shape_id]['max']
            
        fn = (lambda x: x.shape_id in (self._shape_sequence_filter.keys())
              and x.stop_sequence >= self._shape_sequence_filter[x.shape_id]['min']
              and x.stop_sequence <= self._shape_sequence_filter[x.shape_id]['max']
             )
        self.stop_delay_view['corridor'] = self.stop_delay_view.apply(fn, axis=1)
        corridor_filtered = self.stop_delay_view >> filter(_.corridor)
        assert not corridor_filtered.empty, 'no stops in corridor!'
        first_stops = corridor_filtered >> group_by(_.trip_id) >> summarize(stop_sequence = _.stop_sequence.min())
        entry_delays = (first_stops
                >> inner_join(_, corridor_filtered, on = ['trip_id', 'stop_sequence'])
                >> select(_.trip_id, _.delay_seconds)
                >> rename(entry_delay_seconds = _.delay_seconds)
               )
        with_entry_delay = corridor_filtered >> inner_join(_, entry_delays, on='trip_id')
        with_entry_delay = with_entry_delay >> mutate(corridor_delay_seconds = _.delay_seconds - _.entry_delay_seconds)
        self.corridor_stop_delays = with_entry_delay
    
    def autocorridor(self, shape_id: str, stop_seq_range: list, manual_exclude = {}):
        '''
        Defines a corridor for delay analysis using existing GTFS Shapes data,
        without the need for externally defining the corridor bounding box.
        Designed to be used alongside segment_speed_map in a notebook, i.e.
        by viewing speeds to decide on bounds and inputting the corresponding
        shape_id and stop sequence range. Attaches corridor for corridor maps/metrics.
        
        shape_id: string, GTFS shape_id
        stop_seq_range: list, containing 2 ints/floats, any order
        exclude_stops: stop_id to exclude manually (optional)
        
        example:
        shape_id = '27_3_87'
        stop_range = [16, 31]
        
        NOTE: last generated speedmap must include shape_id of interest for this to work
        
        manual_exclude: dict in {'shape': {'min': int, 'max': int}, ...} format (values are stop sequence)
        '''
        corridor = (self.stop_segment_speed_view.copy() # will returning a copy here help?
         >> distinct(_.shape_id, _.stop_sequence, _keep_all=True)
         >> filter(_.shape_id == shape_id,
                   _.stop_sequence >= min(stop_seq_range),
                   _.stop_sequence <= max(stop_seq_range))
         >> mutate(distance_meters = _.shape_meters.max() - _.shape_meters.min())
        )
        corridor.geometry = corridor.buffer(100)
        corridor = corridor.dissolve()
        self.add_corridor(corridor, manual_exclude)
        return
    
    def segment_speed_map(self, segments: str='stops', how: str='low_speeds',
                          colorscale = ZERO_THIRTY_COLORSCALE, size: list=[900, 550],
                         no_title = False, corridor = False, shn = False,
                         no_render = False):
        ''' Generate a map of segment speeds aggregated across all trips for each shape, either as medians
        or 20th percentile speeds.
        
        segments: 'stops' or 'detailed' (detailed not yet implemented)
        how: 'median', 'low_speeds' (20%ile)
        colorscale: branca.colormap
        size: [x, y]
        no_title: don't show title in folium map
        corridor: if corridor set, only map segments relevant to that corridor
        shn: show state highway network
        no_render: don't remder map using folium, only store as self.detailed_map_view
        '''
        assert segments in ['stops', 'detailed']
        assert how in ['median', 'low_speeds']
        
        gcs_filename = f'{self.calitp_itp_id}_{self.analysis_date.isoformat()}_{self.filter_period}'
        subfolder = 'v2_segment_speed_views/' if self.using_v2 else 'segment_speed_views/'
        cached_periods = ['PM_Peak', 'AM_Peak', 'Midday', 'All_Day']
        if (check_cached(filename = f'{gcs_filename}.parquet', subfolder = subfolder) and self.filter_period in cached_periods
            and self._time_only_filter and not corridor):
            self.stop_segment_speed_view = gpd.read_parquet(f'{GCS_FILE_PATH}{subfolder}{gcs_filename}.parquet')
        else:
            gdf = self._filter(self.stop_delay_view)
            if corridor:
                assert hasattr(self, 'corridor'), 'must add corridor before generating corridor map'
                gdf = gdf >> filter(_.corridor)
            all_stop_speeds = gpd.GeoDataFrame()
            # for shape_id in tqdm(gdf.shape_id.unique()): trying self.pbar.update...
            if type(self.pbar) != type(None):
                self.pbar.reset(total=len(gdf.shape_id.unique()))
                self.pbar.desc = f'Generating segment speeds {self.pbar_desc}'
            for shape_id in gdf.shape_id.unique():
                try:
                    this_shape = (gdf >> filter((_.shape_id == shape_id))).copy()
                    # self.debug_dict[f'{shape_id}_segments'] = this_shape
                    # Siuba errors unless you do this twice? TODO make upstream issue...
                    this_shape = this_shape >> group_by(_.trip_id) >> arrange(_.stop_sequence) >> ungroup()
                    stop_speeds = (this_shape
                                 >> group_by(_.trip_id)
                                 >> arrange(_.stop_sequence)
                                 >> mutate(seconds_from_last = (_.actual_time - _.actual_time.shift(1)).apply(lambda x: x.seconds))
                                 >> mutate(last_loc = _.shape_meters.shift(1))
                                 >> mutate(meters_from_last = (_.shape_meters - _.last_loc))
                                 >> mutate(speed_from_last = _.meters_from_last / _.seconds_from_last)
                                 >> mutate(delay_chg_sec = (_.delay_seconds - _.delay_seconds.shift(1)))
                                 >> ungroup()
                                )
                    # self.debug_dict[f'{shape_id}_{direction_id}_st_spd'] = stop_speeds
                    stop_speeds = stop_speeds.dropna(subset=['last_loc']).set_crs(CA_NAD83Albers)
                    stop_speeds.geometry = stop_speeds.apply(
                        lambda x: shapely.ops.substring(
                                    (self.shapes >> filter(_.shape_id == x.shape_id)).geometry.iloc[0],
                                    x.last_loc,
                                    x.shape_meters),
                                                    axis = 1)
                    # self.debug_dict[f'{shape_id}_st_spd1'] = stop_speeds
                    stop_speeds = (stop_speeds
                         >> mutate(speed_mph = _.speed_from_last * MPH_PER_MPS)
                         >> filter(_.speed_mph < 80, _.speed_mph > 0) ## drop impossible speeds, TODO logging?
                         >> group_by(_.stop_sequence)
                         >> mutate(n_trips_shp = _.stop_sequence.size, # filtered to shape
                                    p50_mph = _.speed_mph.median(),
                                    p20_mph = _.speed_mph.quantile(.2),
                                    p80_mph = _.speed_mph.quantile(.8),
                                    fast_slow_ratio = _.p80_mph / _.p20_mph # new intuitive variation measure
                                    # var_mph = _.speed_mph.var() # old statistical variance
                                  )
                         >> ungroup()
                         >> select(-_.arrival_time, -_.actual_time, -_.delay, -_.last_delay)
                        )
                    # self.debug_dict[f'{shape_id}_st_spd2'] = stop_speeds
                    assert not stop_speeds.empty, 'stop speeds gdf is empty!'
                except Exception as e:
                    print(f'stop_speeds shape: {stop_speeds.shape}, shape_id: {shape_id}')
                    print(e)
                    continue
                
                all_stop_speeds = pd.concat((all_stop_speeds, stop_speeds))

                if type(self.pbar) != type(None):
                    self.pbar.update()
                if type(self.pbar) != type(None):
                    self.pbar.refresh
            # count all trips in direction with RT data for a better gauge of frequency
            direction_counts = (self._filter(self.rt_trips)
                 >> count(_.route_id, _.direction_id)
                 >> mutate(trips_per_hour = _.n / self.hr_duration_in_filter)
                 >> select(-_.n)
                )
            all_stop_speeds = all_stop_speeds >> inner_join(_, direction_counts, on = ['route_id', 'direction_id'])
            self.stop_segment_speed_view = all_stop_speeds
            export_path = f'{GCS_FILE_PATH}{subfolder}'
            if self._time_only_filter and self.filter_period in cached_periods:
                geoparquet_gcs_export(all_stop_speeds, export_path, gcs_filename)
        self.speed_map_params = (how, colorscale, size, no_title, corridor, shn, no_render)
        return self._show_speed_map()
    
    def _show_speed_map(self):
        '''
        Final formatting for speed map, saves to self.detailed_map_view and
        renders via folium unless self.segment_speed_map called with no_render = True
        
        Don't call directly; use self.segment_speed_map
        '''
        how, colorscale, size, no_title, corridor, shn, no_render = self.speed_map_params
        gdf = self.stop_segment_speed_view.copy()
        # essential here for reasonable map size!
        gdf = gdf >> distinct(_.shape_id, _.stop_sequence, _keep_all=True)
        gdf['miles_from_last'] = gdf.meters_from_last / 1609
        # Further reduce map size
        gdf = gdf >> select(-_.speed_mph, -_.speed_from_last, -_.trip_id,
                            -_.trip_key, -_.delay_seconds, -_.seconds_from_last,
                           -_.delay_chg_sec, -_.activity_date, -_.service_date,
                            -_.last_loc, -_.shape_meters,
                           -_.meters_from_last, -_.n_trips_shp)
        orig_rows = gdf.shape[0]
        gdf = gdf.round({'p50_mph': 1, 'p20_mph': 1, 'p80_mph': 1,
                         'miles_from_last': 1, 'trips_per_hour': 1, 'avg_sec': 0,
                         '_20p_sec': 0, 'fast_slow_ratio': 1}) ##round for display
        
        how_speed_col = {'median': 'p50_mph', 'low_speeds': 'p20_mph'}
        how_formatted = {'median': 'Median', 'low_speeds': '20th Percentile'}
        
        # filter out 0's in distance, speed-- issue arises from rounding error
        gdf = (gdf >> filter(_.miles_from_last > 0, _.p20_mph > 0, _.p50_mph > 0))
        
        gdf['time_formatted'] = (gdf.miles_from_last / gdf[how_speed_col[how]]) * 60**2 #seconds
        gdf['time_formatted'] = gdf['time_formatted'].apply(lambda x: f'{int(x)//60}' + f':{int(x)%60:02}')

        gdf = gdf >> arrange(_.trips_per_hour)
        gdf = gdf.set_crs(CA_NAD83Albers)
        
        ## shift to right side of road to display direction
        gdf.geometry = gdf.geometry.apply(try_parallel)
        gdf = gdf.apply(arrowize_by_frequency, axis=1)
        gdf.geometry = gdf.geometry.simplify(tolerance=5) # 5 is max for good-looking arrows?
        gdf = gdf >> filter(gdf.geometry.is_valid)
        gdf = gdf >> filter(-gdf.geometry.is_empty)
        
        assert gdf.shape[0] >= orig_rows*.975, \
            f'over 2.5% of geometries invalid after buffer+simplify ({gdf.shape[0]} / {orig_rows})'
        gdf = gdf.to_crs(WGS84)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.current_centroid = (gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean())
        self.detailed_map_view = gdf.copy()
        if no_render:
            return  # ready but don't show map here, export later           
        display_cols = [how_speed_col[how], 'time_formatted', 'miles_from_last',
                       'route_short_name', 'trips_per_hour', 'shape_id',
                       'stop_sequence', 'fast_slow_ratio']
        display_aliases = ['Speed (miles per hour)', 'Travel time', 'Segment distance (miles)',
                          'Route', 'Frequency (trips per hour)', 'Shape ID',
                          'Stop Sequence', '80th %ile to 20th %ile speed ratio (variation in speeds)']
        tooltip_dict = {'aliases': display_aliases}
        if no_title:
            title = ''
        else:
            title = f"{self.organization_name} {how_formatted[how]} Vehicle Speeds Between Stops{self.filter_formatted}"
        colorscale.caption = "Speed (miles per hour)"
        style_dict = {'opacity': 0, 'fillOpacity': 0.8}
        if shn:
            m = self.shn.explore(tiles = 'CartoDB positron', color = 'gray',
                                 width = size[0], height = size[1], zoom_start = 13,
                                 location = self.current_centroid)
        else:
            m = None
        g = gdf.explore(column=how_speed_col[how],
                        cmap = colorscale,
                        tiles = 'CartoDB positron',
                        style_kwds = style_dict,
                        tooltip = display_cols, popup = display_cols,
                        tooltip_kwds = tooltip_dict, popup_kwds = tooltip_dict,
                        highlight_kwds = {'fillColor': '#DD1C77',"fillOpacity": 0.6},
                        width = size[0], height = size[1], zoom_start = 13,
                        location = self.current_centroid, m = m)
        
        title_html = f"""
         <h3 align="center" style="font-size:20px"><b>{title}</b></h3>
         """
        g.get_root().html.add_child(folium.Element(title_html)) # might still want a util for this...
        return g   
    
    def map_variance(self, no_title = False, no_render = False):
        '''
        A quick map of relative speed variance across stop segments, measured as
        the ratio between the 80th percentile and 20th percentile speeds
        '''
        assert hasattr(self, 'detailed_map_view'), 'must generate a speedmap first with self.segment_speed_map'
        gdf = self.detailed_map_view.copy().dropna(subset=['fast_slow_ratio']) >> arrange(_.trips_per_hour)
        display_cols = ['fast_slow_ratio', 'p20_mph', 'p80_mph', 'miles_from_last',
                       'route_short_name', 'trips_per_hour', 'shape_id',
                       'stop_sequence', 'stop_id', 'route_id', 'stop_name']
        display_aliases = ['80th %ile to 20th %ile speed ratio (variation in speeds)',
                           '20th %ile Speed (miles per hour)', ' 80th %ile Speed (miles per hour)', 'Segment distance (miles)',
                          'Route', 'Frequency (trips per hour)', 'Shape ID',
                          'Stop Sequence', 'Stop ID', 'Route ID', 'Stop Name']
        tooltip_dict = {'aliases': display_aliases}
        cmap = VARIANCE_FIXED_COLORSCALE
        self.variance_cmap = cmap
        gdf = gdf[display_cols + ['geometry']]
        assert isinstance(gdf, gpd.GeoDataFrame)
        self._variance_map_view = gdf
        if no_render:
            return
        if no_title:
            title = ''
        else:
            title = f"{self.organization_name} Variation in Vehicle Speeds Between Stops{self.filter_formatted}"

        style_dict = {'opacity': 0.8, 'fillOpacity': 0.8}
        g = gdf.explore(column='fast_slow_ratio', cmap = self.variance_cmap,
                        tooltip = display_cols, popup = display_cols,
                        tooltip_kwds = tooltip_dict, popup_kwds = tooltip_dict,
                        tiles = 'CartoDB positron',
                   style_kwds = style_dict)
        title_html = f"""
         <h3 align="center" style="font-size:20px"><b>{title}</b></h3>
         """
        g.get_root().html.add_child(folium.Element(title_html)) # might still want a util for this...
        return g
    
    def map_gz_export(self, map_type: str='_20p_speeds'):
        '''
        Test exporting speed data to gcs bucket for iframe render
        Will always put state highway network in state['layers'][0] 
        map_type: '_20p_speeds', 'variance', or 'shn'
        '''
        if not hasattr(self, 'spa_map_state'):
            self.spa_map_state = {"name": "null", "layers": [], "lat_lon": (),
                             "zoom": 13}
        subfolder = f'speeds_{self.analysis_date.isoformat()}/'
        
        if map_type == '_20p_speeds':
            assert hasattr(self, 'detailed_map_view'), 'must generate a speedmap first with self.segment_speed_map'
            if len(self.spa_map_state["layers"]) != 1:  # re-initialize to SHN only
                self.map_gz_export(map_type = 'shn')
                
            gdf = self.detailed_map_view.copy()
            gdf['organization_name'] = self.organization_name
            cmap = self.speed_map_params[1]
            
            filename = f'{self.calitp_itp_id}_{self.filter_period}_speeds'
            title = f"{self.organization_name} {self.display_date} {self.filter_period.replace('_', ' ')}"
            
            export_result = set_state_export(gdf, subfolder = subfolder, filename = filename,
                                map_type = 'speedmap', map_title = title, cmap = cmap,
                                color_col = 'p20_mph', legend_url = SPEEDMAP_LEGEND_URL,
                                existing_state = self.spa_map_state
                                            )
            self.spa_map_state = export_result['state_dict']
            self.spa_map_url = export_result['spa_link']
            return
        
        elif map_type == 'variance':
            assert hasattr(self, 'detailed_map_view'), 'must generate a variance map first with self.map_variance'
            if len(self.spa_map_state["layers"]) != 1:  # re-initialize to SHN only
                self.map_gz_export(map_type = 'shn')
            
            filename = f'{self.calitp_itp_id}_{self.filter_period}_variance'
            title = f"{self.organization_name} {self.display_date} {self.filter_period.replace('_', ' ')}"
            
            export_result = set_state_export(self._variance_map_view, subfolder = subfolder, filename = filename,
                                map_type = 'speed_variation', map_title = title, cmap = self.variance_cmap,
                                color_col = 'fast_slow_ratio', 
                                legend_url = 'https://storage.googleapis.com/calitp-map-tiles/variance_legend.svg',
                                existing_state = self.spa_map_state
                                            )
            self.spa_map_state = export_result['state_dict']
            self.spa_map_url = export_result['spa_link']
            return
        
        elif map_type == 'shn':
            dist = self.caltrans_district[:2]
            filename = f'{dist}_SHN'
            title = f"D{dist} State Highway Network"
            
            export_result = set_state_export(self.shn, subfolder = subfolder, filename = filename,
                                map_type = 'state_highway_network', map_title = title)
            self.spa_map_state = export_result['state_dict']
            self.spa_map_url = export_result['spa_link']
            return
        
    def render_spa_link(self):
        
        display(Markdown(f'<a href="{self.spa_map_url}" target="_blank">Open Full Map in New Tab</a>'))
        return
    
    def display_spa_map(self, width: int=1000, height: int=650):
        '''
        Display map from external simple web app in the notebook/JupyterBook context via an IFrame.
        Will show most recent map set using self.map_gz_export
        Width/height defaults are current best option for JupyterBook, don't change for portfolio use
        width, height: int (pixels)
        '''
        assert hasattr(self, 'spa_map_url'), 'must export map and set state first using self.map_gz_export'
        i = IFrame(self.spa_map_url, width=width, height=height)
        display(i)
        return

    
    def chart_speeds(self, no_title = False):
        '''
        A bar chart showing delays grouped by arrival hour for current filtered selection.
        Currently hardcoded to 0600-2200.
        '''
        filtered_trips = (self._filter(self.rt_trips)
                             >> mutate(median_hour = _.median_time.apply(lambda x: x.hour))
                             >> filter(_.median_hour > 5)
                             >> filter(_.median_hour < 23)
                            )
        grouped = (filtered_trips >> group_by(_.median_hour)
                   >> summarize(median_trip_mph = _.mean_speed_mph.median())
                  )
        grouped['Median Trip Speed (mph)'] = grouped.median_trip_mph.apply(lambda x: round(x, 1))
        grouped['Hour'] = grouped.median_hour
        if no_title:
            title = ''
        else:
            title = f"{self.organization_name} Median Trip Speeds by Arrival Hour{self.filter_formatted}"
            
        sns_plot = (sns.barplot(x=grouped['Hour'], y=grouped['Median Trip Speed (mph)'], ci=None, 
                       palette=[CALITP_CATEGORY_BOLD_COLORS[1]])
            .set_title(title)
           )
        chart = sns_plot.get_figure()
        chart.tight_layout()
        return chart
    
    def chart_variability(self, min_stop_seq = None, max_stop_seq = None, num_segments = None,
                         no_title = False):
        '''
        Chart trip speed variability, as speed between each stop segments.
        stop_sequence_range: (min_stop, max_stop)
        '''
        sns.set(rc = {'figure.figsize':(13,6)})
        assert (self.filter['shape_ids']
                and len(self.filter['shape_ids']) == 1), 'must filter to a single shape_id'
        _map = self.segment_speed_map()
        to_chart = self.stop_segment_speed_view.copy()
        to_chart = to_chart.dropna(subset=['stop_id'])
        if num_segments:
            unique_stops = list(to_chart.stop_sequence.unique())[:num_segments]
            min_stop_seq = min(unique_stops)
            max_stop_seq = max(unique_stops)
        if min_stop_seq:
            to_chart = to_chart >> filter(_.stop_sequence >= min_stop_seq)
        if max_stop_seq:
            to_chart = to_chart >> filter(_.stop_sequence <= max_stop_seq)
        to_chart.stop_name = to_chart.stop_name.str.split('&').map(lambda x: x[-1])
        to_chart = to_chart.rename(columns={'speed_mph': 'Segement Speed (mph)',
                                      'delay_chg_sec': 'Increase in Delay (seconds)',
                                      'stop_sequence': 'Stop Segment ID',
                                      'stop_name': 'Segment Cross Street'})
        plt.xticks(rotation=65)
        title = f"{self.organization_name} Speed Variability by Stop Segment{self.filter_formatted}"
        if no_title:
            title = None
        variability_plt = sns.swarmplot(x = to_chart['Segment Cross Street'], y=to_chart['Segement Speed (mph)'],
              palette=CALITP_CATEGORY_BRIGHT_COLORS,
             ).set_title(title)
        return variability_plt
    
    def describe_slow_routes(self):
        try:
            slowest = (self._filter(self.rt_trips)
                >> group_by(_.shape_id)
                >> summarize(num_trips = _.shape_id.count(), median_trip_mph = _.mean_speed_mph.median())
                >> arrange(_.median_trip_mph)
                >> inner_join(_, self._filter(self.rt_trips) >> distinct(_.shape_id, _.route_id, _.route_short_name,
                            _.route_long_name, _.route_desc, _.direction), on = 'shape_id')
                >> head(7)
            )
            slowest = slowest.apply(describe_slowest, axis=1)
            display_list = slowest.full_description.to_list()
            with_newlines = '\n * ' + '\n * '.join(display_list)
            period_formatted = self.filter_period.replace('_', ' ')
            # display(slowest)
            display(Markdown(f'{period_formatted} slowest routes: {with_newlines}'))
        except:
            # print('describe slow routes failed!')
            pass
        return
    
    def quick_map_corridor(self):
        '''
        Maps currently attached corridor, with stops just before/
        in/just after corridor for all routes in current filter, if any
        '''
        
        assert hasattr(self, 'corridor'), 'Must attach a corridor first'
        filtered_stops = self._filter(self.stop_delay_view)
        mappable_stops = (filtered_stops.dropna(subset=['stop_id'])
                  >> distinct(_.shape_id, _.stop_sequence, _keep_all=True)
                  >> filter(_.corridor)
                  >> select(_.stop_id, _.geometry, _.stop_sequence, _.shape_id)
                 )
        if 'total_schedule_delay' in self.corridor.columns:
            selector = select(_.geometry, _.total_speed_delay, _.total_schedule_delay)
        else:
            selector = select(_.geometry, _.total_speed_delay)
        gdf = (self.corridor >> selector).iloc[:1,:]
        m = pd.concat([mappable_stops, gdf]).explore(tiles = "CartoDB positron")
        return m
    
    def runtime_metrics(self):
        '''
        Measure observed runtimes for routes within filter. Based on full data extent of each trip,
        currently bidirectional. Returns 20th, 50th, 80th %ile.
        Assess all-day frequency as frequency within the narrower of 0600-2100 or actual span.
        Note that frequency is average frequency in each direction.        
        '''
        df = (self._filter(self.stop_delay_view)
             >> filter(_.actual_time > dt.datetime.combine(self.analysis_date, dt.time(6)),
                       _.actual_time < dt.datetime.combine(self.analysis_date, dt.time(21))
                      )
             >> group_by(_.shape_id, _.route_id, _.route_short_name, _.trip_id)
             >> summarize(runtime = _.actual_time.max() - _.actual_time.min(),
                         start = _.actual_time.min())
             >> group_by(_.route_id, _.route_short_name)
             >> summarize(p50_runtime_minutes =_.runtime.quantile(.5).seconds / 60,
                          p20_runtime_minutes =_.runtime.quantile(.2).seconds / 60,
                          p80_runtime_minutes =_.runtime.quantile(.8).seconds / 60,
                         first_start = _.start.min(), last_start = _.start.max(),
                         n_trips = _.shape[0])
             >> mutate(span = _.last_start - _.first_start, span_hours = _.span.map(lambda x: x.seconds) / 60**2,
                      daily_avg_trips_hr = (_.n_trips / _.span_hours) / 2) # create avg per direction by div 2
        ).round(1)
        return df
    def corridor_metrics(self, sccp = False):
        '''
        Set sccp = True to generate schedule and speed based metrics for SCCP/LPP programs.
        
        Default assumption is that speeds will improve to 16mph for the speed based metric.
        Set a different speed via the self.transit_priority_target_mph attribute.
        
        Note that trips_added assumes that service hour savings based on corridor speed improvements
        are reinvested into additonal runs of the *entire* route, at the existing median runtime.
        '''
        assert hasattr(self, 'corridor'), 'must add corridor before generating corridor metrics'
        assert not self._filter(self.corridor_stop_delays).empty, 'filter does not include any corridor trips'
        if sccp:
            assert not self.filter, 'warning: filter set -- for SCCP/LPP reset filter first'
            assert self.transit_priority_target_mph == 16, 'for SCCP/LPP use 16mph standard'
            
        # median delay within segment for each trip, then aggregate to route
        schedule_metric_df = (self._filter(self.corridor_stop_delays)
             >> group_by(_.trip_id, _.route_id, _.route_short_name)
             >> summarize(median_corridor_delay_seconds = _.corridor_delay_seconds.median())
             >> group_by(_.route_id, _.route_short_name)
             >> summarize(schedule_delay_minutes = _.median_corridor_delay_seconds.sum() / 60)
            )
        
        self.stop_delay_view = self.stop_delay_view >> group_by(_.trip_id) >> arrange(_.stop_sequence) >> ungroup()
        self.corridor_trip_speeds = (self._filter(self.stop_delay_view)
             >> filter(_.corridor)
             >> group_by(_.trip_id)
             >> mutate(entry_time = _.actual_time.min())
             >> mutate(exit_time = _.actual_time.max())
             >> mutate(entry_loc = _.shape_meters.min())
             >> mutate(exit_loc = _.shape_meters.max())
             >> mutate(meters_from_entry = (_.exit_loc - _.entry_loc))
             >> mutate(seconds_from_entry = (_.exit_time - _.entry_time).apply(lambda x: x.seconds))
             >> mutate(speed_from_entry = _.meters_from_entry / _.seconds_from_entry)
             >> ungroup()
             >> distinct(_.trip_id, _keep_all=True)
            )
        target_speed_mps = self.transit_priority_target_mph / MPH_PER_MPS
        
        speed_int_df = (self.corridor_trip_speeds
              >> mutate(corridor_speed_mph = _.speed_from_entry * MPH_PER_MPS)
              >> mutate(target_seconds = _.meters_from_entry / target_speed_mps)
              >> mutate(target_delay_seconds = _.seconds_from_entry - _.target_seconds)
             )
        speed_metric_df = (speed_int_df
              >> mutate(organization = self.organization_name)
              >> group_by(_.route_id, _.route_short_name, _.organization)
              >> summarize(p20_corr_mph = _.corridor_speed_mph.quantile(.2),
                           p50_corr_mph = _.corridor_speed_mph.quantile(.5),
                           avg_corr_mph = _.corridor_speed_mph.mean(),
                           speed_delay_minutes = _.target_delay_seconds.sum() / 60)
             )
        both_metrics_df = (speed_metric_df >> inner_join(_, schedule_metric_df, on = ['route_id', 'route_short_name'])
                              >> mutate(total_speed_delay = _.speed_delay_minutes.sum(),
                                       total_schedule_delay = _.schedule_delay_minutes.sum())
                          )
        if not sccp:
            both_metrics_df = both_metrics_df >> select(-_.total_schedule_delay, -_.schedule_delay_minutes)
        runtimes = self.runtime_metrics() >> select(_.route_id, _.p50_runtime_minutes, _.n_trips,
                                                   _.span_hours, _.daily_avg_trips_hr)
        both_metrics_df = (both_metrics_df >> inner_join(_, runtimes, on = 'route_id')
                              >> mutate(trips_added = _.speed_delay_minutes / _.p50_runtime_minutes)
                              >> mutate(new_avg_trips_hr = ((_.n_trips + _.trips_added) / _.span_hours) / 2)
                              # create avg per direction by div 2
                          )
        both_metrics_df['length_miles'] = (self.corridor.distance_meters / METERS_PER_MILE).iloc[0]
        both_metrics_df['target_mph'] = self.transit_priority_target_mph
        # both_metrics_df.length_miles = both_metrics_df.length_miles.fillna(value=gdf.length_miles.iloc[0])
        gdf = gpd.GeoDataFrame(both_metrics_df, geometry=self.corridor.geometry, crs=self.corridor.crs)
        # ffill kinda broken in geopandas?
        gdf.geometry = gdf.geometry.fillna(value=gdf.geometry.iloc[0])
        self.corridor = gdf.round(1)
        print('metrics attached to self.corridor: ')
        return self.corridor
    
def from_gcs(itp_id, analysis_date, pbar = None):
    ''' Generates RtFilterMapper from cached artifacts in GCS. Generate using rt_parser.OperatorDayAnalysis.export_views_gcs()
    '''
    date_iso = analysis_date.isoformat()
    
    # if analysis_date <= warehouse_cutoff_date:
    #     shapes = get_routelines(itp_id, analysis_date)
    #     trips = (pd.read_parquet(f'{GCS_FILE_PATH}rt_trips/{itp_id}_{date_iso}.parquet')
    #         .reset_index(drop=True))
    #     stop_delay = (gpd.read_parquet(f'{GCS_FILE_PATH}stop_delay_views/{itp_id}_{date_iso}.parquet')
    #              .reset_index(drop=True))
    # else:
    
    # always use v2 warehouse, v1 warehouse deprecated/gone
    index_df = get_speedmaps_ix_df(analysis_date = analysis_date, itp_id = itp_id)
    trips = (pd.read_parquet(f'{GCS_FILE_PATH}v2_rt_trips/{itp_id}_{date_iso}.parquet')
        .reset_index(drop=True))
    stop_delay = (gpd.read_parquet(f'{GCS_FILE_PATH}v2_stop_delay_views/{itp_id}_{date_iso}.parquet')
             .reset_index(drop=True))
    shapes = get_shapes(index_df)
    
    stop_delay['arrival_time'] = stop_delay.arrival_time.map(lambda x: np.datetime64(x))
    stop_delay['actual_time'] = stop_delay.actual_time.map(lambda x: np.datetime64(x))
    
    rt_day = RtFilterMapper(trips, stop_delay, shapes, pbar)
    return rt_day