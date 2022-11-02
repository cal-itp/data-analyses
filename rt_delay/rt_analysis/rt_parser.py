import shared_utils
from shared_utils.geography_utils import WGS84, CA_NAD83Albers
from shared_utils.map_utils import make_folium_choropleth_map

import branca

from siuba import *

import pandas as pd
import numpy as np
import geopandas as gpd
import shapely
from shapely.geometry import Point

import datetime as dt
from tqdm import tqdm

# import numpy as np
from calitp.tables import tbls
import seaborn as sns
import matplotlib.pyplot as plt

import logging
logging.basicConfig(filename = 'rt.log')

import gcsfs
fs = gcsfs.GCSFileSystem()

class VehiclePositionsInterpolator:
    ''' Interpolates the location of a specific trip using GTFS-RT Vehicle Positions data
    '''
    def __init__(self, vp_trip_gdf, shape_gdf):
        ''' vp_trip_gdf: a gdf with rt vehicle positions data, joined with trip/route info from GTFS Schedule
        shape_gdf: a gdf with line geometries for each shape
        '''
        # self.debug_dict = {}
        self.logassert(type(vp_trip_gdf) == type(gpd.GeoDataFrame()) and not vp_trip_gdf.empty, "vp_trip_gdf must not be empty")
        self.logassert(type(shape_gdf) == type(gpd.GeoDataFrame()) and not shape_gdf.empty, "shape gdf must not be empty")
        self.logassert(vp_trip_gdf.crs == CA_NAD83Albers and shape_gdf.crs == CA_NAD83Albers, f"position and shape CRS must be {CA_NAD83Albers}")
        self.logassert(vp_trip_gdf.trip_id.nunique() == 1, "non-unique trip id in position gdf")
        self.debug_dict = {}
        self.position_type = 'rt'
        self.time_col = 'vehicle_timestamp'
        trip_info_cols = ['service_date', 'trip_key', 'trip_id', 'route_id', 'route_short_name',
                          'shape_id', 'direction_id', 'calitp_itp_id', 'entity_id', 'vehicle_id']
        self.logassert(set(trip_info_cols).issubset(vp_trip_gdf.columns), f"vp_trip_gdf must contain columns: {trip_info_cols}")
        for col in trip_info_cols:
            setattr(self, col, vp_trip_gdf[col].iloc[0]) ## allow access to trip_id, etc. using self.trip_id
            
        self.logassert((shape_gdf.calitp_itp_id == self.calitp_itp_id).all(), "vp_trip_gdf and shape_gdf itp_id should match")
        self.vp_trip_gdf = vp_trip_gdf >> distinct(_.vehicle_timestamp, _keep_all=True)
        self.vp_trip_gdf = self.vp_trip_gdf.drop(columns = trip_info_cols)
        self._attach_shape(shape_gdf)
        self.median_time = self.vp_trip_gdf[self.time_col].median()
        self.time_of_day = shared_utils.rt_utils.categorize_time_of_day(self.median_time)
        self.total_meters = (self.cleaned_positions.shape_meters.max() - self.cleaned_positions.shape_meters.min())
        self.total_seconds = (self.cleaned_positions.vehicle_timestamp.max() - self.cleaned_positions.vehicle_timestamp.min()).seconds
        self.logassert(self.total_meters > 1000, "less than 1km of data")
        self.logassert(self.total_seconds > 60, "less than 60 seconds of data")
        self.mean_speed_mph = (self.total_meters / self.total_seconds) * shared_utils.rt_utils.MPH_PER_MPS
        
    def logassert(self, conditon, message):
        if conditon:
            return
        elif hasattr(self, 'calitp_itp_id') and hasattr(self, 'trip_id'):
            logging.error(f'{self.calitp_itp_id}:{self.trip_id}:{message}')
            raise AssertionError
        else:
            logging.error(message)
            raise AssertionError  
            
    def _linear_reference(self):
        raw_positions = self.vp_trip_gdf.copy()
        raw_positions = raw_positions >> arrange(self.time_col)
        raw_position_array = raw_positions.shape_meters.to_numpy()
        # filter to positions that have progressed from the most recent position
        cast_monotonic = np.maximum.accumulate(raw_position_array)
        raw_positions.shape_meters = cast_monotonic
        raw_positions = raw_positions.drop_duplicates(subset=['shape_meters'], keep = 'last')
        raw_positions['secs_from_last'] = raw_positions[self.time_col].diff()
        raw_positions.secs_from_last = (raw_positions.secs_from_last
                                        .apply(lambda x: x.seconds))
        raw_positions['meters_from_last'] = raw_positions.shape_meters.diff()
        raw_positions['speed_from_last'] = (raw_positions.meters_from_last
                                                     / raw_positions.secs_from_last) ## meters/second
        self.cleaned_positions = raw_positions
        self._shape_array = self.cleaned_positions.shape_meters.to_numpy()
        self._dt_array = (self.cleaned_positions[self.time_col].to_numpy()
                                          .astype('datetime64[s]')
                                          .astype('float64')
                                         )
        return
    
    def _attach_shape(self, shape_gdf):
        ''' Filters gtfs shapes to the shape served by this trip. Additionally, projects positions_gdf to a linear
        position along the shape and calls _linear_reference() to calculate speeds+times for valid positions
        shape_gdf: a gdf with line geometries for each shape
        '''
        shape_geo = (shape_gdf >> filter(_.shape_id == self.shape_id)).geometry
        self.logassert(len(shape_geo) > 0 and shape_geo.iloc[0], f'shape empty for trip {self.trip_id}!')
        self.shape = shape_geo.iloc[0]
        self.vp_trip_gdf['shape_meters'] = (self.vp_trip_gdf.geometry
                                .apply(lambda x: self.shape.project(x)))
        self._linear_reference()
        
        origin = (self.vp_trip_gdf >> filter(_.shape_meters == _.shape_meters.min())
        ).geometry.iloc[0]
        destination = (self.vp_trip_gdf >> filter(_.shape_meters == _.shape_meters.max())
        ).geometry.iloc[0]
        self.direction = shared_utils.rt_utils.primary_cardinal_direction(origin, destination)
        
        
    def time_at_position(self, desired_position):
        ''' Returns an estimate of this trip's arrival time at any linear position along the shape, based
        on the nearest two positions.
        desired_position: int (meters from start of shape)
        '''
        interpolation = shared_utils.rt_utils.time_at_position_numba(desired_position, self._shape_array, self._dt_array)
        if interpolation:
            return dt.datetime.utcfromtimestamp(interpolation)
        else:
            return None
        
    def detailed_speed_map(self):
        ''' Generates a detailed map of speeds along the trip based on all valid position data
        '''
        gdf = self.cleaned_positions.copy()
        gdf['speed_mph'] = gdf.speed_from_last * shared_utils.rt_utils.MPH_PER_MPS
        gdf = gdf.round({'speed_mph': 1, 'shape_meters': 0})
        gdf['time'] = gdf[self.time_col].apply(lambda x: x.strftime('%H:%M:%S'))
        gdf = gdf >> select(_.geometry, _.time,
                            _.shape_meters, _.speed_mph)
        gdf['last_loc'] = gdf.shape_meters.shift(1)
        gdf = gdf.iloc[1:,:] ## remove first row (inaccurate shape data)
        gdf.geometry = gdf.apply(lambda x: shapely.ops.substring(self.shape,
                                                                x.last_loc,
                                                                x.shape_meters), axis = 1)
        # return gdf ## debug return
        ## shift to right side of road to display direction
        gdf.geometry = gdf.geometry.apply(lambda x:
            x.parallel_offset(25, 'right') if isinstance(x, shapely.geometry.LineString) else x)
        self.detailed_map_view = gdf.copy()
        ## create clips, integrate buffer+simplify?
        gdf.geometry = gdf.geometry.apply(shared_utils.rt_utils.arrowize_segment).simplify(tolerance=5)
        gdf = gdf >> filter(gdf.geometry.is_valid)
        # gdf.geometry = gdf.geometry.apply(clip_along_shape)
        gdf = gdf >> filter(-gdf.geometry.is_empty)
        
        gdf = gdf.to_crs(WGS84)
        centroid = gdf.dissolve().centroid
        gdf = gdf >> filter(_.speed_mph > 0)

        gdf['shape_id'] = self.shape_id
        gdf['direction_id'] = self.direction_id
        gdf['trip_id'] = self.trip_id

        if gdf.speed_mph.max() > 80: ## TODO better system to raise errors on impossibly fast speeds
            print(f'speed above 80 for trip {self.trip_key}, dropping')
            gdf = gdf >> filter(_.speed_mph < 80)
        if gdf.speed_mph.max() < 0: ## TODO better system to raise errors on impossibly fast speeds
            print(f'negative speed for trip {self.trip_key}, dropping')
            gdf = gdf >> filter(_.speed_mph > 0)
        # self.debug_dict['map_gdf'] = gdf

        colorscale = branca.colormap.step.RdYlGn_08.scale(vmin=gdf.speed_mph.min(), 
                     vmax=gdf.speed_mph.max())
        colorscale.caption = "Speed (miles per hour)"

        popup_dict = {
            "speed_mph": "Speed (miles per hour)",
            "shape_meters": "Distance along route (meters)",
            "shape_id": "Shape ID",
            "direction_id": "Direction ID",
            "trip_id": "Trip ID",
            "time": "Time"
        }

        g = make_folium_choropleth_map(
            gdf,
            plot_col = 'speed_mph',
            popup_dict = popup_dict,
            tooltip_dict = popup_dict,
            colorscale = colorscale,
            fig_width = 1000, fig_height = 700,
            zoom = 13,
            centroid = [centroid.y, centroid.x],
            title=f"Trip Speed Map (Route {self.route_short_name}, {self.direction}, {self.time_of_day})",
            legend_name = "Speed (miles per hour)",
            highlight_function=lambda x: {
                'fillColor': '#DD1C77',
                "fillOpacity": 0.6,
            }
        )

        return g
    
    def export_detailled_map(self, folder_name):
        '''
        Export a detailled map of this trip to a specified folder. File will be named according to trip data,
        for example: {calitp_itp_id}_rt_{route_id}_tr_{trip_id}.html
        '''
        m = self.detailed_speed_map()
        m.save(f'./{folder_name}/{self.calitp_itp_id}_rt_{self.route_id}_tr_{self.trip_id}.html')

class OperatorDayAnalysis:
    '''New top-level class for rt delay/speed analysis of a single operator on a single day
    '''
    def __init__(self, itp_id, analysis_date, pbar = None):
        self.pbar = pbar
        self.debug_dict = {}
        '''
        itp_id: an itp_id (string or integer)
        analysis date: datetime.date
        '''
        self.calitp_itp_id = int(itp_id)
        assert type(analysis_date) == dt.date, 'analysis date must be a datetime.date object'
        self.analysis_date = analysis_date
        self.display_date = self.analysis_date.strftime('%b %d (%a)')
        self.vehicle_positions = shared_utils.rt_utils.get_vehicle_positions(self.calitp_itp_id, self.analysis_date)
        self.trips = shared_utils.rt_utils.get_trips(self.calitp_itp_id, self.analysis_date)
        self.stop_times = shared_utils.rt_utils.get_stop_times(self.calitp_itp_id, self.analysis_date)
        self.stops = shared_utils.rt_utils.get_stops(self.calitp_itp_id, self.analysis_date)
        trips = self.trips >> select(-_.calitp_url_number, -_.calitp_extracted_at, -_.calitp_deleted_at)
        positions = self.vehicle_positions >> select(-_.calitp_url_number)
        self.trips_positions_joined = (trips >> inner_join(_, positions, on= ['trip_id', 'calitp_itp_id']))
        assert not self.trips_positions_joined.empty, 'vehicle positions empty, or vp trip ids not in schedule'
        self.trips_positions_joined = gpd.GeoDataFrame(self.trips_positions_joined,
                                    geometry=gpd.points_from_xy(self.trips_positions_joined.vehicle_longitude,
                                                                self.trips_positions_joined.vehicle_latitude),
                                    crs=WGS84).to_crs(CA_NAD83Albers)
        self.routelines = shared_utils.rt_utils.get_routelines(self.calitp_itp_id, self.analysis_date)
        self.routelines = self.routelines.dropna(subset=['geometry']) ## invalid geos are nones in new df...
        assert type(self.routelines) == type(gpd.GeoDataFrame()) and not self.routelines.empty, 'routelines must not be empty'
        self.trs = self.trips >> select(_.shape_id, _.trip_id)
        self.trs = self.trs >> inner_join(_, self.stop_times >> select(_.trip_id, _.stop_id), on = 'trip_id')
        self.trs = self.trs >> distinct(_.stop_id, _.shape_id)
        self.trs = self.stops >> select(_.stop_id, _.stop_name, _.geometry) >> inner_join(_, self.trs, on = 'stop_id')
        if not self.trs.shape_id.isin(self.routelines.shape_id).all():
            no_shape_trs = self.trs >> filter(-_.shape_id.isin(self.routelines.shape_id))
            print(f'{no_shape_trs.shape[0]} scheduled trips out of {self.trs.shape[0]} have no shape, dropping')
            assert no_shape_trs.shape[0] < self.trs.shape[0] / 10, '>10% of trips have no shape!'
            self.trs = self.trs >> filter(_.shape_id.isin(self.routelines.shape_id))
        ## project scheduled stops to shape, TODO evaluate accuracy/replace alongside improving vp projection
        self.trs['shape_meters'] = (self.trs.apply(lambda x:
                (self.routelines >> filter(_.shape_id == x.shape_id)).geometry.iloc[0].project(x.geometry),
         axis = 1))
        self.routelines = self.routelines.apply(self._ix_from_routeline, axis=1)
        self.vp_obs_by_trip = self.vehicle_positions >> count(_.trip_id) >> arrange(-_.n)
        
        self._generate_position_interpolators()
        self.rt_trips = self.trips.copy() >> filter(_.trip_id.isin(self.position_interpolators.keys()))
        self.debug_dict['rt_trips'] = self.rt_trips
        self.rt_trips['median_time'] = self.rt_trips.apply(lambda x: self.position_interpolators[x.trip_id]['rt'].median_time.time(), axis = 1)
        self.rt_trips['direction'] = self.rt_trips.apply(lambda x: self.position_interpolators[x.trip_id]['rt'].direction, axis = 1)
        self.rt_trips['mean_speed_mph'] = self.rt_trips.apply(lambda x: self.position_interpolators[x.trip_id]['rt'].mean_speed_mph, axis = 1)
        self.pct_trips_valid_rt = self.rt_trips.trip_id.nunique() / self.trips.trip_id.nunique()

        self._generate_stop_delay_view()
        ## TODO replace/include in initial queries, avoid seperate warehouse call
        self.calitp_agency_name = (tbls.views.gtfs_schedule_dim_feeds()
             >> filter(_.calitp_itp_id == self.calitp_itp_id, _.calitp_deleted_at == _.calitp_deleted_at.max())
             >> collect()
            ).calitp_agency_name.iloc[0]
        self.rt_trips['calitp_agency_name'] = self.calitp_agency_name
        
    def _ix_from_routeline(self, routeline):
        try:
            km_index = np.arange(1000, routeline.geometry.length, 1000)
            stops_filtered = (self.trs
                              >> filter(_.shape_id == routeline.shape_id)
                              >> arrange(_.shape_meters))
            stop_loc_array = stops_filtered.shape_meters.to_numpy()
            # https://stackoverflow.com/questions/56024634/minimum-distance-for-each-value-in-array-respect-to-other
            # get minimum distance to any existing stop for each kilometer segment
            idx = np.searchsorted(stop_loc_array, km_index, side='right')
            result = km_index-stop_loc_array[idx-1] # substract one for proper index
            # if kilometer segment is < 1km from an existing stop, drop
            km_index = km_index[result > 1000]
            routeline['km_index'] = km_index
        except:
            routeline['km_index'] = np.zeros(0)
            print(f'could not interpolate segments for shape {routeline.shape_id}')
        return routeline
        
    def _generate_position_interpolators(self):
        '''For each trip_key in analysis, generate vehicle positions and schedule interpolator objects'''
        self.position_interpolators = {}
        if type(self.pbar) != type(None):
            self.pbar.reset(total=self.vehicle_positions.trip_id.nunique())
            self.pbar.desc = f'Generating position interpolators itp_id: {self.calitp_itp_id}'
        for trip_id in self.vehicle_positions.trip_id.unique():
            trip_positions_joined = self.trips_positions_joined >> filter(_.trip_id == trip_id)
            # self.debug_dict[f'{trip_id}_vp'] = trip_positions_joined
            try:
                self.position_interpolators[trip_id] = {'rt': VehiclePositionsInterpolator(trip_positions_joined, self.routelines)}
            except AssertionError as e:
                # print(e)
                continue
            if type(self.pbar) != type(None):
                self.pbar.update()
        if type(self.pbar) != type(None):
            self.pbar.refresh()
            
    def _add_km_segments(self, _delay):
        ''' Experimental to break up long segments
            To filter these out, self.stop_delay_view.dropna(subset=['stop_id'])
        '''
        
        new_ix = (self.routelines >> filter(_.shape_id == _delay.shape_id.iloc[0])).km_index.iloc[0]
        if np.any(new_ix):
            _delay = _delay.set_index('shape_meters')
            first_shape_meters = (_delay >> filter(_.stop_sequence == _.stop_sequence.min())).index.to_numpy()[0]
            new_ix = new_ix[new_ix > first_shape_meters]
            new_df = pd.DataFrame(index=new_ix)
            new_df.index.name = 'shape_meters'
            appended = pd.concat([_delay, new_df])
            appended.trip_id = appended.trip_id.fillna(method='ffill').fillna(method='bfill')
            appended.shape_id = appended.shape_id.fillna(method='ffill').fillna(method='bfill')
            appended.route_id = appended.route_id.fillna(method='ffill').fillna(method='bfill')
            appended.route_short_name = appended.route_short_name.fillna(method='ffill').fillna(method='bfill')                                              
            appended = appended >> arrange(_.shape_meters)
            appended.stop_sequence = appended.stop_sequence.interpolate()
            appended = appended.reset_index()
            appended['actual_time'] = appended.apply(lambda x: 
                        self.position_interpolators[x.trip_id]['rt'].time_at_position(x.shape_meters),
                        axis = 1)
            return appended
        else:
            return _delay
    
    def _generate_stop_delay_view(self):
        ''' Creates a (filtered) view with delays for each trip at each stop
        '''
        
        trips = self.rt_trips >> select(_.trip_id, _.route_id, _.route_short_name, _.direction_id, _.shape_id)
        st = self.stop_times >> select(_.trip_key, _.trip_id, _.stop_id, _.stop_sequence, _.arrival_time)
        delays = self.trs >> inner_join(_, st, on = 'stop_id')
        delays = delays >> inner_join(_, trips, on = ['trip_id', 'shape_id'])
        ## changed to stop sequence which should catch more duplicates, but a pain point from url number handling...
        delays = delays >> distinct(_.trip_id, _.stop_sequence, _keep_all=True) 
        self.debug_dict['delays'] = delays
        _delays = gpd.GeoDataFrame()
        
        if type(self.pbar) != type(None):
            self.pbar.reset(total=len(delays.trip_id.unique()))
            self.pbar.desc = f'Generating stop delay view itp_id: {self.calitp_itp_id}'
        for trip_id in delays.trip_id.unique():
            try:
                _delay = delays.copy() >> filter(_.trip_id == trip_id) >> distinct(_.stop_id, _keep_all = True)
                _delay['actual_time'] = _delay.apply(lambda x: 
                                self.position_interpolators[x.trip_id]['rt'].time_at_position(x.shape_meters),
                                axis = 1)
                _delay = _delay.dropna(subset=['actual_time'])
                _delay.arrival_time = _delay.arrival_time.map(lambda x: shared_utils.rt_utils.fix_arrival_time(x)[0]) ## reformat 25:xx GTFS timestamps to standard 24 hour time
                _delay['arrival_time'] = _delay.apply(lambda x:
                                    pd.Timestamp(dt.datetime.combine(x.actual_time.date(),
                                                        dt.datetime.strptime(x.arrival_time, '%H:%M:%S').time())) if x.arrival_time else np.nan,
                                                        axis = 1) ## format scheduled arrival times
                _delay = _delay >> filter(_.arrival_time.apply(lambda x: x.date()) == _.actual_time.iloc[0].date())
                if _delay.arrival_time.isnull().any():
                    _delay = shared_utils.rt_utils.interpolate_arrival_times(_delay)
                # _delay['arrival_time'] = _delay.arrival_time.astype('datetime64') # deprecated, replace with pd.Timestamp constuctor
                # self.debug_dict[f'{trip_id}_times'] = _delay
                _delay['delay'] = _delay.actual_time - _delay.arrival_time
                _delay['delay'] = _delay.delay.apply(lambda x: dt.timedelta(seconds=0) if x.days == -1 else x)
                _delay['delay_seconds'] = _delay.delay.map(lambda x: x.seconds)

                self.debug_dict[f'{trip_id}_stopsegs'] = _delay
                try:
                    _delay = self._add_km_segments(_delay)
                except:
                    print(f'could not add km segments trip: {_delay.trip_id.iloc[0]}')
                    continue

                _delays = pd.concat((_delays, _delay))
                self.debug_dict[f'{trip_id}_delay'] = _delay
            except Exception as e:
                print(f'could not generate delays for trip {trip_id}')
                print(e)
                
            if type(self.pbar) != type(None):
                self.pbar.update()
        if type(self.pbar) != type(None):
            self.pbar.refresh()
                
        self.stop_delay_view = _delays.reset_index(drop=True)
        return
    
    def export_views_gcs(self):
        ''' Exports stop delay view, rt_trips. Supports robust mapping and analysis without intensive regeneration of interpolator objects.
        '''
        day = str(self.analysis_date.day).zfill(2)
        month = str(self.analysis_date.month).zfill(2)
        date_iso = self.analysis_date.isoformat()
        
        stop_delay_to_parquet = self.stop_delay_view.copy()
        stop_delay_to_parquet['delay_seconds'] = stop_delay_to_parquet.delay.map(lambda x: x.seconds)
        stop_delay_to_parquet['arrival_time'] = stop_delay_to_parquet.arrival_time.map(lambda x: x.isoformat())
        stop_delay_to_parquet['actual_time'] = stop_delay_to_parquet.actual_time.map(lambda x: x.isoformat())
        stop_delay_to_parquet = stop_delay_to_parquet >> select(-_.delay)
        shared_utils.utils.geoparquet_gcs_export(stop_delay_to_parquet,
                                         f'{shared_utils.rt_utils.GCS_FILE_PATH}stop_delay_views/',
                                        f'{self.calitp_itp_id}_{date_iso}'
                                        )
        self.rt_trips.to_parquet(f'{shared_utils.rt_utils.GCS_FILE_PATH}rt_trips/{self.calitp_itp_id}_{date_iso}.parquet')

        return
    
def run_operators(analysis_date, operator_list, pbar=None):
    """
    Wrapper function for generating rt_trips and stop_delay_views in GCS for operators on a given day, after checking existence with shared_utils.rt_utils.get_operators.

    analysis_date: datetime.date
    operator_list: list of itp_id's
    pbar: tqdm.notebook.tqdm(), optional progress bar for generation
    """
    op_dict_runstatus = shared_utils.rt_utils.get_operators(analysis_date, operator_list)
    
    op_list_notrun = []
    for key, value in op_dict_runstatus.items():
        if value == "not_yet_run":
            op_list_notrun.append(key)
    
    for itp_id in op_list_notrun:
        print(f"calculating for agency: {itp_id}...")
        try:
            rt_day = OperatorDayAnalysis(itp_id, analysis_date, pbar)
            rt_day.export_views_gcs()
            print(f"complete for agency: {itp_id}")
        except Exception as e:
            print(f"rt failed for agency {itp_id}")
            print(e)