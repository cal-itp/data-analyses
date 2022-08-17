import shared_utils
from shared_utils.geography_utils import WGS84, CA_NAD83Albers
from shared_utils.map_utils import make_folium_choropleth_map
from shared_utils.rt_utils import *

import branca

from siuba import *

import pandas as pd
import numpy as np
import geopandas as gpd
import shapely

import datetime as dt
from tqdm import tqdm

# import numpy as np
from calitp.tables import tbl
import seaborn as sns
import matplotlib.pyplot as plt



class TripPositionInterpolator:
    ''' Interpolates the location of a specific trip using either rt or schedule data
    '''
    def __init__(self, position_gdf, shape_gdf, addl_info_cols = []):
        ''' positions_gdf: a gdf with either rt vehicle positions or joined stops+stop_times
        shape_gdf: a gdf with line geometries for each shape
        '''
        # self.debug_dict = {}
        assert type(position_gdf) == type(gpd.GeoDataFrame()) and not position_gdf.empty, "positions gdf must not be empty"
        assert type(shape_gdf) == type(gpd.GeoDataFrame()) and not shape_gdf.empty, "shape gdf must not be empty"
        assert position_gdf.crs == CA_NAD83Albers and shape_gdf.crs == CA_NAD83Albers, f"position and shape CRS must be {CA_NAD83Albers}"
        assert position_gdf.trip_key.nunique() == 1, "non-unique trip key in position gdf"
        
        trip_info_cols = ['service_date', 'trip_key', 'trip_id', 'route_id', 'route_short_name',
                          'shape_id', 'direction_id', 'calitp_itp_id'] + addl_info_cols
        assert set(trip_info_cols).issubset(position_gdf.columns), f"position_gdf must contain columns: {trip_info_cols}"
        for col in trip_info_cols:
            setattr(self, col, position_gdf[col].iloc[0]) ## allow access to trip_id, etc. using self.trip_id
            
        assert (shape_gdf.calitp_itp_id == self.calitp_itp_id).all(), "position_gdf and shape_gdf itp_id should match"
        self.position_gdf = position_gdf.drop(columns = trip_info_cols)
        self._attach_shape(shape_gdf)
        self.median_time = self.position_gdf[self.time_col].median()
        self.time_of_day = categorize_time_of_day(self.median_time)
        self.total_meters = (self.cleaned_positions.shape_meters.max() - self.cleaned_positions.shape_meters.min())
        self.total_seconds = (self.cleaned_positions.vehicle_timestamp.max() - self.cleaned_positions.vehicle_timestamp.min()).seconds
        assert self.total_meters > 1000, "less than 1km of data"
        assert self.total_seconds > 60, "less than 60 seconds of data"
        self.mean_speed_mph = (self.total_meters / self.total_seconds) * MPH_PER_MPS
        
    def _attach_shape(self, shape_gdf):
        ''' Filters gtfs shapes to the shape served by this trip. Additionally, projects positions_gdf to a linear
        position along the shape and calls _linear_reference() to calculate speeds+times for valid positions
        shape_gdf: a gdf with line geometries for each shape
        '''
        shape_geo = (shape_gdf >> filter(_.shape_id == self.shape_id)).geometry
        assert len(shape_geo) > 0 and shape_geo.iloc[0], f'shape empty for trip {self.trip_id}!'
        self.shape = shape_geo.iloc[0]
        self.position_gdf['shape_meters'] = (self.position_gdf.geometry
                                .apply(lambda x: self.shape.project(x)))
        self._linear_reference()
        
        origin = (self.position_gdf >> filter(_.shape_meters == _.shape_meters.min())
        ).geometry.iloc[0]
        destination = (self.position_gdf >> filter(_.shape_meters == _.shape_meters.max())
        ).geometry.iloc[0]
        self.direction = primary_cardinal_direction(origin, destination)
        
        
    def time_at_position(self, desired_position):
        ''' Returns an estimate of this trip's arrival time at any linear position along the shape, based
        on the nearest two positions.
        desired_position: int (meters from start of shape)
        '''
        interpolation = time_at_position_numba(desired_position, self._shape_array, self._dt_array)
        if interpolation:
            return dt.datetime.utcfromtimestamp(interpolation)
        else:
            return None
        
    def detailed_speed_map(self):
        ''' Generates a detailed map of speeds along the trip based on all valid position data
        '''
        gdf = self.cleaned_positions.copy()
        gdf['speed_mph'] = gdf.speed_from_last * MPH_PER_MPS
        gdf = gdf.round({'speed_mph': 1, 'shape_meters': 0})
        gdf['time'] = gdf[self.time_col].apply(lambda x: x.strftime('%H:%M:%S'))
        gdf = gdf >> select(_.geometry, _.time,
                            _.shape_meters, _.speed_mph)
        gdf['last_loc'] = gdf.shape_meters.shift(1)
        gdf = gdf.iloc[1:,:] ## remove first row (inaccurate shape data)
        gdf.geometry = gdf.apply(lambda x: shapely.ops.substring(self.shape,
                                                                x.last_loc,
                                                                x.shape_meters), axis = 1)
        ## shift to right side of road to display direction
        gdf.geometry = gdf.geometry.apply(lambda x:
            x.parallel_offset(25, 'right') if isinstance(x, shapely.geometry.LineString) else x)
        self.detailed_map_view = gdf.copy()
        ## create clips, integrate buffer+simplify?
        gdf.geometry = gdf.geometry.apply(arrowize_segment).simplify(tolerance=5)
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

class VehiclePositionsInterpolator(TripPositionInterpolator):
    '''Interpolate arrival times along a trip from GTFS-RT Vehicle Positions data.
    '''
    def __init__(self, vp_gdf, shape_gdf):
        # print(vp_gdf.head(1))
        # print(shape_gdf.head(1))
        assert type(vp_gdf) == type(gpd.GeoDataFrame()) and not vp_gdf.empty, "vehicle positions gdf must not be empty"
        assert type(shape_gdf) == type(gpd.GeoDataFrame()) and not shape_gdf.empty, "shape gdf must not be empty"
        
        self.debug_dict = {}
        
        self.position_type = 'rt'
        self.time_col = 'vehicle_timestamp'
        self._position_cleaning_count = 0
        TripPositionInterpolator.__init__(self, position_gdf = vp_gdf, shape_gdf = shape_gdf,
                                          addl_info_cols = ['entity_id', 'vehicle_id'])
        self.position_gdf = self.position_gdf >> distinct(_.vehicle_timestamp, _keep_all=True)
        
    def _linear_reference(self):
        
        self.position_gdf = self._shift_calculate(self.position_gdf)
        self.progressing_positions = self.position_gdf >> filter(_.progressed)
        self.debug_dict['clean_0'] = self.progressing_positions.copy()
        ## check if positions have progressed from immediate previous point, but not previous point of forwards progression
        while not self.progressing_positions.shape_meters.is_monotonic:
            self._position_cleaning_count += 1
            # print(f'check location data for trip {self.trip_id}')
            self.progressing_positions = self._shift_calculate(self.progressing_positions)
            self.progressing_positions = self.progressing_positions >> filter(_.progressed)
            self.debug_dict[f'clean_{self._position_cleaning_count}'] = self.progressing_positions.copy()
        self.cleaned_positions = self.progressing_positions ## for position and map methods in TripPositionInterpolator
        self.cleaned_positions = self.cleaned_positions >> arrange(self.time_col)
        self._shape_array = self.cleaned_positions.shape_meters.to_numpy()
        self._dt_array = (self.cleaned_positions[self.time_col].to_numpy()
                                      .astype('datetime64[s]')
                                      .astype('float64')
                                     )

    def _shift_calculate(self, vehicle_positions):
        
        vehicle_positions = vehicle_positions >> arrange(self.time_col) ## unnecessary?
        vehicle_positions['secs_from_last'] = vehicle_positions[self.time_col].diff()
        vehicle_positions.secs_from_last = (vehicle_positions.secs_from_last
                                        .apply(lambda x: x.seconds))
        vehicle_positions['meters_from_last'] = vehicle_positions.shape_meters.diff()
        vehicle_positions['progressed'] = vehicle_positions['meters_from_last'] > 0 ## has the bus moved ahead?
        vehicle_positions.iloc[0, vehicle_positions.columns.get_loc('progressed')] = True ## avoid dropping start point
        vehicle_positions['speed_from_last'] = (vehicle_positions.meters_from_last
                                                     / vehicle_positions.secs_from_last) ## meters/second
        return vehicle_positions

class ScheduleInterpolator(TripPositionInterpolator):
    '''Interpolate arrival times along a trip from schedule data.
    '''
    def __init__(self, trip_st_gdf, shape_gdf):
        '''trip_st_gdf: gdf of GTFS trip (filtered to 1 trip) joined w/ GTFS stop times and stops,
        shape_gdf: gdf of geometries for each shape'''
        self.position_type = 'schedule'
        self.time_col = 'arrival_dt'
        trip_st_gdf = trip_st_gdf.apply(gtfs_time_to_dt, axis=1)
        TripPositionInterpolator.__init__(self, position_gdf = trip_st_gdf, shape_gdf = shape_gdf)
    
    def _linear_reference(self):
        
        self.position_gdf['last_time'] = self.position_gdf[self.time_col].shift(1)
        self.position_gdf['last_loc'] = self.position_gdf.shape_meters.shift(1)
        self.position_gdf['secs_from_last'] = self.position_gdf[self.time_col] - self.position_gdf.last_time
        self.position_gdf.secs_from_last = (self.position_gdf.secs_from_last
                                        .apply(lambda x: x.seconds))
        self.position_gdf['meters_from_last'] = (self.position_gdf.shape_meters
                                                      - self.position_gdf.last_loc)
        self.position_gdf['speed_from_last'] = (self.position_gdf.meters_from_last
                                                     / self.position_gdf.secs_from_last) ## meters/second
        self.cleaned_positions = self.position_gdf ## for position and map methods in TripPositionInterpolator
        self.cleaned_positions = self.cleaned_positions >> arrange(self.time_col)

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
        self.vehicle_positions = get_vehicle_positions(self.calitp_itp_id, self.analysis_date)
        self.trips = get_trips(self.calitp_itp_id, self.analysis_date)
        self.stop_times = get_stop_times(self.calitp_itp_id, self.analysis_date)
        self.stops = get_stops(self.calitp_itp_id, self.analysis_date)
        trips = self.trips >> select(-_.calitp_url_number, -_.calitp_extracted_at, -_.calitp_deleted_at)
        positions = self.vehicle_positions >> select(-_.calitp_url_number)
        self.trips_positions_joined = (trips
                                        >> inner_join(_, positions, on= ['trip_id', 'calitp_itp_id'])
                                       ) ##TODO check info cols here...
        assert not self.trips_positions_joined.empty, 'vehicle positions trip ids not in schedule'
        self.trips_positions_joined = gpd.GeoDataFrame(self.trips_positions_joined,
                                    geometry=gpd.points_from_xy(self.trips_positions_joined.vehicle_longitude,
                                                                self.trips_positions_joined.vehicle_latitude),
                                    crs=WGS84).to_crs(CA_NAD83Albers)
        self.routelines = get_routelines(self.calitp_itp_id, self.analysis_date)
        self.routelines = self.routelines.dropna(subset=['geometry']) ## invalid geos are nones in new df...
        assert type(self.routelines) == type(gpd.GeoDataFrame()) and not self.routelines.empty, 'routelines must not be empty'
        self.trs = self.trips >> select(_.shape_id, _.trip_id)
        self.trs = self.trs >> inner_join(_, self.stop_times >> select(_.trip_id, _.stop_id), on = 'trip_id')
        self.trs = self.trs >> distinct(_.stop_id, _.shape_id)
        self.trs = self.stops >> select(_.stop_id, _.stop_name, _.geometry) >> inner_join(_, self.trs, on = 'stop_id')
        # print(f'projecting')
        if not self.trs.shape_id.isin(self.routelines.shape_id).all():
            no_shape_trs = self.trs >> filter(-_.shape_id.isin(self.routelines.shape_id))
            print(f'{no_shape_trs.shape[0]} scheduled trips out of {self.trs.shape[0]} have no shape, dropping')
            assert no_shape_trs.shape[0] < self.trs.shape[0] / 10, '>10% of trips have no shape!'
            self.trs = self.trs >> filter(_.shape_id.isin(self.routelines.shape_id))
        self.trs['shape_meters'] = (self.trs.apply(lambda x:
                (self.routelines >> filter(_.shape_id == x.shape_id)).geometry.iloc[0].project(x.geometry),
         axis = 1))
        # print(f'done')
        self.vp_obs_by_trip = self.vehicle_positions >> count(_.trip_id) >> arrange(-_.n)
        self._generate_position_interpolators() ## comment out for first test
        self.rt_trips = self.trips.copy() >> filter(_.trip_id.isin(self.position_interpolators.keys()))
        self.debug_dict['rt_trips'] = self.rt_trips
        self.rt_trips['median_time'] = self.rt_trips.apply(lambda x: self.position_interpolators[x.trip_id]['rt'].median_time.time(), axis = 1)
        self.rt_trips['direction'] = self.rt_trips.apply(lambda x: self.position_interpolators[x.trip_id]['rt'].direction, axis = 1)
        self.rt_trips['mean_speed_mph'] = self.rt_trips.apply(lambda x: self.position_interpolators[x.trip_id]['rt'].mean_speed_mph, axis = 1)
        self.pct_trips_valid_rt = self.rt_trips.trip_id.nunique() / self.trips.trip_id.nunique()

        self._generate_stop_delay_view()
        self.endpoint_delay_view = (self.stop_delay_view
                      >> group_by(_.trip_id)
                      >> filter(_.stop_sequence == _.stop_sequence.max())
                      >> ungroup()
                      >> mutate(arrival_hour = _.arrival_time.apply(lambda x: x.hour))
                      >> inner_join(_, self.rt_trips >> select(_.trip_id, _.mean_speed_mph), on = 'trip_id')
                     )
        self.endpoint_delay_summary = (self.endpoint_delay_view
                      >> group_by(_.direction_id, _.route_id, _.arrival_hour)
                      >> summarize(n_trips = _.route_id.size, mean_end_delay = _.delay.mean())
                     )
        self.filter = None
        self.filter_formatted = ''
        self.hr_duration_in_filter = (self.vehicle_positions.vehicle_timestamp.max() - 
                                         self.vehicle_positions.vehicle_timestamp.min()).seconds / 60**2
        self.calitp_agency_name = (tbl.views.gtfs_schedule_dim_feeds()
             >> filter(_.calitp_itp_id == self.calitp_itp_id, _.calitp_deleted_at == _.calitp_deleted_at.max())
             >> collect()
            ).calitp_agency_name.iloc[0]
        self.rt_trips['calitp_agency_name'] = self.calitp_agency_name
        
    def _generate_position_interpolators(self):
        '''For each trip_key in analysis, generate vehicle positions and schedule interpolator objects'''
        self.position_interpolators = {}
        if type(self.pbar) != type(None):
            self.pbar.reset(total=self.vehicle_positions.trip_id.nunique())
            self.pbar.desc = 'Generating position interpolators'
        for trip_id in self.vehicle_positions.trip_id.unique():
            # print(trip_id)
            trip = self.trips.copy() >> filter(_.trip_id == trip_id)
            # self.debug_dict[f'{trip_id}_trip'] = trip
            st_trip_joined = (trip
                              >> inner_join(_, self.stop_times, on = ['calitp_itp_id', 'trip_id', 'trip_key'])
                              >> inner_join(_, self.stops, on = ['stop_id', 'calitp_itp_id'])
                             )
            st_trip_joined = gpd.GeoDataFrame(st_trip_joined, geometry=st_trip_joined.geometry, crs=CA_NAD83Albers)
            trip_positions_joined = self.trips_positions_joined >> filter(_.trip_id == trip_id)
            # self.debug_dict[f'{trip_id}_st'] = st_trip_joined
            # self.debug_dict[f'{trip_id}_vp'] = trip_positions_joined
            try:
                self.position_interpolators[trip_id] = {'rt': VehiclePositionsInterpolator(trip_positions_joined, self.routelines),
                                                           # 'schedule': ScheduleInterpolator(st_trip_joined, self.routelines) ## probably need to save memory for now ?
                                                       }
            except AssertionError as e:
                print(e)
                continue
            if type(self.pbar) != type(None):
                self.pbar.update()
        if type(self.pbar) != type(None):
            self.pbar.refresh()

            # TODO better checking for incomplete trips (either here or in interpolator...)
            
    def _add_km_segments(self, _delay):
        ''' Experimental to break up long segments
        '''
        
        _delay = _delay.set_index('shape_meters')
        new_ix = np.arange(_delay.index.min(), _delay.index.max(), 1000)
        first_shape_meters = (_delay >> filter(_.stop_sequence == _.stop_sequence.min())).index.to_numpy()[0]
        new_ix = new_ix[new_ix > first_shape_meters]
        new_df = pd.DataFrame(index=new_ix)
        new_df.index.name = 'shape_meters'
        appended = pd.concat([_delay, new_df])
        appended.trip_id = appended.trip_id.fillna(method='ffill').fillna(method='bfill')
        appended.shape_id = appended.shape_id.fillna(method='ffill').fillna(method='bfill')
        appended = appended >> arrange(_.shape_meters)
        appended.stop_sequence = appended.stop_sequence.interpolate()
        appended = appended.reset_index()
        appended['actual_time'] = appended.apply(lambda x: 
                    self.position_interpolators[x.trip_id]['rt'].time_at_position(x.shape_meters),
                    axis = 1)
        return appended
    
    def _generate_stop_delay_view(self):
        ''' Creates a (filtered) view with delays for each trip at each stop
        '''
        
        trips = self.trips >> select(_.trip_id, _.route_id, _.route_short_name, _.direction_id, _.shape_id)
        trips = trips >> filter(_.trip_id.isin(list(self.position_interpolators.keys())))
        st = self.stop_times >> select(_.trip_key, _.trip_id, _.stop_id, _.stop_sequence, _.arrival_time)
        delays = self.trs >> inner_join(_, st, on = 'stop_id')
        delays = delays >> inner_join(_, trips, on = ['trip_id', 'shape_id'])
        ## changed to stop sequence which should catch more duplicates, but a pain point from url number handling...
        delays = delays >> distinct(_.trip_id, _.stop_sequence, _keep_all=True) 
        self.debug_dict['delays'] = delays
        _delays = gpd.GeoDataFrame()
        
        if type(self.pbar) != type(None):
            self.pbar.reset(total=len(delays.trip_id.unique()))
            self.pbar.desc = 'Generating stop delay view'
        for trip_id in delays.trip_id.unique():
            try:
                _delay = delays.copy() >> filter(_.trip_id == trip_id) >> distinct(_.stop_id, _keep_all = True)
                _delay['actual_time'] = _delay.apply(lambda x: 
                                self.position_interpolators[x.trip_id]['rt'].time_at_position(x.shape_meters),
                                axis = 1)
                _delay = _delay.dropna(subset=['actual_time'])
                _delay.arrival_time = _delay.arrival_time.map(lambda x: fix_arrival_time(x)[0]) ## reformat 25:xx GTFS timestamps to standard 24 hour time
                _delay['arrival_time'] = _delay.apply(lambda x:
                                    dt.datetime.combine(x.actual_time.date(),
                                                        dt.datetime.strptime(x.arrival_time, '%H:%M:%S').time()),
                                                        axis = 1) ## format scheduled arrival times
                _delay = _delay >> filter(_.arrival_time.apply(lambda x: x.date()) == _.actual_time.iloc[0].date())
                _delay['arrival_time'] = _delay.arrival_time.astype('datetime64')
                # self.debug_dict[f'{trip_id}_times'] = _delay
                _delay['delay'] = _delay.actual_time - _delay.arrival_time
                _delay['delay'] = _delay.delay.apply(lambda x: dt.timedelta(seconds=0) if x.days == -1 else x)
                _delay['delay_seconds'] = _delay.delay.map(lambda x: x.seconds)
                
                self.debug_dict[f'{trip_id}_stopsegs'] = _delay
                # try:
                _delay = self._add_km_segments(_delay)
                # except:
                #     print(f'could not add km segments trip: {_delay.trip_id.iloc[0]}')
                #     continue
                
                _delays = pd.concat((_delays, _delay))
                self.debug_dict[f'{trip_id}_delay'] = _delay
                # return
            except Exception as e:
                print(f'could not generate delays for trip {trip_id}')
                print(e)
                # return
                
            if type(self.pbar) != type(None):
                self.pbar.update()
        if type(self.pbar) != type(None):
            self.pbar.refresh()
                
        self.stop_delay_view = _delays
        return
    
    def export_views_gcs(self):
        ''' Exports stop delay view, rt_trips, and endpoint delay view to GCS. Supports robust mapping and analysis without intensive regeneration of interpolator objects.
        '''
        day = str(self.analysis_date.day).zfill(2)
        month = str(self.analysis_date.month).zfill(2)
        
        stop_delay_to_parquet = self.stop_delay_view.copy()
        stop_delay_to_parquet['delay_seconds'] = stop_delay_to_parquet.delay.map(lambda x: x.seconds)
        stop_delay_to_parquet['arrival_time'] = stop_delay_to_parquet.arrival_time.map(lambda x: x.isoformat())
        stop_delay_to_parquet['actual_time'] = stop_delay_to_parquet.actual_time.map(lambda x: x.isoformat())
        stop_delay_to_parquet = stop_delay_to_parquet >> select(-_.delay)
        shared_utils.utils.geoparquet_gcs_export(stop_delay_to_parquet,
                                         f'{GCS_FILE_PATH}stop_delay_views/',
                                        f'{self.calitp_itp_id}_{month}_{day}'
                                        )
        self.rt_trips.to_parquet(f'{GCS_FILE_PATH}rt_trips/{self.calitp_itp_id}_{month}_{day}.parquet')
        end_delay_to_parquet = self.endpoint_delay_view.copy()
        end_delay_to_parquet['delay_seconds'] = end_delay_to_parquet.delay.map(lambda x: x.seconds)
        end_delay_to_parquet = end_delay_to_parquet >> select(-_.delay)
        shared_utils.utils.geoparquet_gcs_export(end_delay_to_parquet,
                                         f'{GCS_FILE_PATH}endpoint_delay_views/',
                                        f'{self.calitp_itp_id}_{month}_{day}_pm_peak'
                                        )
        return