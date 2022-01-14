import shared_utils
from shared_utils.geography_utils import WGS84, CA_NAD83Albers
from shared_utils.map_utils import make_folium_choropleth_map
import branca

from utils import *
from siuba import *

import pandas as pd
import geopandas as gpd
import shapely

import datetime as dt
import time
from zoneinfo import ZoneInfo

class TripPositionInterpolator:
    ''' Interpolates the location of a specific trip using either rt or schedule data
    '''
    def __init__(self, position_gdf, shape_gdf, addl_info_cols = []):
        ''' positions_gdf: a gdf with either rt vehicle positions or joined stops+stop_times
        shape_gdf: a gdf with line geometries for each shape
        '''
        self.debug_dict = {}
        assert type(position_gdf) == type(gpd.GeoDataFrame()) and not position_gdf.empty, "positions gdf must not be empty"
        assert type(shape_gdf) == type(gpd.GeoDataFrame()) and not shape_gdf.empty, "shape gdf must not be empty"
        assert position_gdf.crs == CA_NAD83Albers and shape_gdf.crs == CA_NAD83Albers, f"position and shape CRS must be {CA_NAD83Albers}"
        assert position_gdf.trip_key.nunique() == 1, "non-unique trip key in position gdf"
        
        trip_info_cols = ['service_date', 'trip_key', 'trip_id', 'route_id', 'shape_id',
                         'direction_id', 'calitp_itp_id'] + addl_info_cols
        assert set(trip_info_cols).issubset(position_gdf.columns), f"position_gdf must contain columns: {trip_info_cols}"
        for col in trip_info_cols:
            setattr(self, col, position_gdf[col].iloc[0])
            
        assert (shape_gdf.calitp_itp_id == self.calitp_itp_id).all(), "position_gdf and shape_gdf itp_id should match"
        self.position_gdf = position_gdf.drop(columns = trip_info_cols)
        self._attach_shape(shape_gdf)
        
    def _attach_shape(self, shape_gdf):
        self.shape = (shape_gdf
                        >> filter(_.shape_id == self.shape_id)
                        >> select(_.shape_id, _.geometry))
        self.position_gdf['shape_meters'] = (self.position_gdf.geometry
                                .apply(lambda x: self.shape.geometry.iloc[0].project(x)))
        self._linear_reference() ##TODO define in subclasses
        
        origin = (self.position_gdf >> filter(_.shape_meters == _.shape_meters.min())
        ).geometry.iloc[0]
        destination = (self.position_gdf >> filter(_.shape_meters == _.shape_meters.max())
        ).geometry.iloc[0]
        self.direction = primary_cardinal_direction(origin, destination)
        
    def time_at_position(self, desired_position):
        
        global bounding_points
        
        try:
            next_point = (self.cleaned_positions ##TODO define in subclasses
                  >> filter(_.shape_meters > desired_position)
                  >> filter(_.shape_meters == _.shape_meters.min())
                 )
            prev_point = (self.cleaned_positions
                  >> filter(_.shape_meters < desired_position)
                  >> filter(_.shape_meters == _.shape_meters.max())
                 )
            bounding_points = (prev_point.append(next_point).copy().reset_index(drop=True)
                    >> select(-_.secs_from_last, -_.meters_from_last, -_.speed_from_last)) ## drop in case bounding points are nonconsecutive, TODO implement in subclass
            secs_from_last = (bounding_points.loc[1][self.time_col] - bounding_points.loc[0][self.time_col]).seconds
            meters_from_last = bounding_points.loc[1].shape_meters - bounding_points.loc[0].shape_meters
            speed_from_last = meters_from_last / secs_from_last

            meters_position_to_next = bounding_points.loc[1].shape_meters - desired_position
            est_seconds_to_next = meters_position_to_next / speed_from_last
            est_td_to_next = dt.timedelta(seconds=est_seconds_to_next)
            est_dt = bounding_points.iloc[-1][self.time_col] - est_td_to_next ##TODO time_col in subclass

            return est_dt
        except KeyError:
            print(f'insufficient bounding points for trip {self.trip_key}, location {desired_position}', end=': ')
            print(f'start/end of route?')
            return None
        
    def detailed_speed_map(self):

        gdf = self.cleaned_positions.copy()
        gdf['time'] = gdf[self.time_col].apply(lambda x: x.strftime('%H:%M:%S'))
        gdf = gdf >> select(_.geometry, _.time,
                            _.shape_meters, _.last_loc, _.speed_from_last)
        gdf['speed_mph'] = gdf.speed_from_last * MPH_PER_MPS
        gdf.geometry = gdf.apply(lambda x: shapely.ops.substring(self.shape.geometry.iloc[0],
                                                                x.last_loc,
                                                                x.shape_meters), axis = 1)
        gdf.geometry = gdf.buffer(25)
        gdf = gdf.to_crs(WGS84)
        centroid = gdf.dissolve().centroid
        gdf = gdf >> filter(_.speed_mph > 0)
        gdf = gdf >> mutate(speed_mph = _.speed_mph.round(1),
                           shape_meters = _.shape_meters.round(0))

        gdf['shape_id'] = self.shape_id
        gdf['direction_id'] = self.direction_id
        gdf['trip_id'] = self.trip_id

        if gdf.speed_mph.max() > 70: ## TODO better system to raise errors on impossibly fast speeds
            print(f'speed above 70 for trip {self.trip_key}, dropping')
            gdf = gdf >> filter(_.speed_mph < 70)

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
            title=f"Trip Speed Map (Route {self.route_id}, {self.direction}, ** Peak)", ##TODO time classification, remove hardcode
            highlight_function=lambda x: {
                'fillColor': '#DD1C77',
                "fillOpacity": 0.6,
            }
        )

        return g

class VehiclePositionsInterpolator(TripPositionInterpolator):
    '''Interpolate arrival times along a trip from GTFS-RT Vehicle Positions data.
    '''
    def __init__(self, vp_gdf, shape_gdf):
        # print(vp_gdf.head(1))
        # print(shape_gdf.head(1))
        assert type(vp_gdf) == type(gpd.GeoDataFrame()) and not vp_gdf.empty, "vehicle positions gdf must not be empty"
        assert type(shape_gdf) == type(gpd.GeoDataFrame()) and not shape_gdf.empty, "shape gdf must not be empty"
        
        self.position_type = 'rt'
        self.time_col = 'vehicle_timestamp'
        TripPositionInterpolator.__init__(self, position_gdf = vp_gdf, shape_gdf = shape_gdf,
                                          addl_info_cols = ['entity_id', 'vehicle_id'])
        self.position_gdf = self.position_gdf >> distinct(_.vehicle_timestamp, _keep_all=True)
        
    def _linear_reference(self):
        
        self.position_gdf = self._shift_calculate(self.position_gdf)
        self.progressing_positions = self.position_gdf >> filter(_.progressed)
        ## check if positions have progressed from immediate previous point, but not previous point of forwards progression
        while not self.progressing_positions.shape_meters.is_monotonic:
            print(f'check location data for trip {self.trip_key}')
            self.progressing_positions = self._shift_calculate(self.progressing_positions)
            self.progressing_positions = self.progressing_positions >> filter(_.progressed)
        self.cleaned_positions = self.progressing_positions ## for position and map methods in TripPositionInterpolator
        self.cleaned_positions = self.cleaned_positions >> arrange(self.time_col)

    
    def _shift_calculate(self, vehicle_positions):
        
        print('sc_called')
        if hasattr(self, "progressing_positions"):
            print(self.progressing_positions.shape)
            self.debug_dict[self.progressing_positions.shape[0]] = self.progressing_positions.copy()
        
        vehicle_positions = vehicle_positions >> arrange(self.time_col) ## unnecessary?
        vehicle_positions['last_time'] = vehicle_positions[self.time_col].shift(1)
        vehicle_positions['last_loc'] = vehicle_positions.shape_meters.shift(1)
        vehicle_positions['secs_from_last'] = vehicle_positions[self.time_col] - vehicle_positions.last_time
        vehicle_positions.secs_from_last = (vehicle_positions.secs_from_last
                                        .apply(lambda x: x.seconds))
        vehicle_positions['meters_from_last'] = (vehicle_positions.shape_meters
                                                      - vehicle_positions.last_loc)
        vehicle_positions['progressed'] = vehicle_positions['meters_from_last'] > 0 ## has the bus moved ahead?
        vehicle_positions['speed_from_last'] = (vehicle_positions.meters_from_last
                                                     / vehicle_positions.secs_from_last) ## meters/second
        return vehicle_positions

class ScheduleInterpolator(TripPositionInterpolator):
    '''Interpolate arrival times along a trip from GTFS-RT Vehicle Positions data.
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
    def __init__(self, itp_id, analysis_date):
        self.debug_dict = {}
        '''
        itp_id: an itp_id (string or integer)
        analysis date: datetime.date
        '''
        self.itp_id = int(itp_id)
        assert type(analysis_date) == dt.date, 'analysis date must be a datetime.date object'
        self.analysis_date = analysis_date
        ## get view df/gdfs TODO implement temporary caching with parquets in bucket...
        self.vehicle_positions = get_vehicle_positions(self.itp_id, self.analysis_date)
        self.trips = get_trips(self.itp_id, self.analysis_date)
        self.stop_times = get_stop_times(self.itp_id, self.analysis_date)
        self.stops = get_stops(self.itp_id, self.analysis_date)
        trips = self.trips >> select(-_.calitp_url_number, -_.calitp_extracted_at, -_.calitp_deleted_at)
        positions = self.vehicle_positions >> select(-_.calitp_url_number)
        self.trips_positions_joined = (trips
                                        >> inner_join(_, positions, on= ['trip_id', 'calitp_itp_id'])
                                       ) ##TODO check info cols here...
        assert not self.trips_positions_joined.empty, 'vehicle positions trip ids not in schedule'
        self.trips_positions_joined = gpd.GeoDataFrame(self.trips_positions_joined,
                                    geometry=gpd.points_from_xy(self.trips_positions_joined.vehicle_position_longitude,
                                                                self.trips_positions_joined.vehicle_position_latitude),
                                    crs=WGS84).to_crs(CA_NAD83Albers)
        # self.routelines = shared_utils.geography_utils.make_routes_shapefile([self.itp_id], CA_NAD83Albers)
        self.routelines = get_routelines(self.itp_id)
        assert type(self.routelines) == type(gpd.GeoDataFrame()) and not self.routelines.empty, 'routelines must not be empty'
        ## end of caching...
        self.vp_trip_ids = (self.vehicle_positions >> distinct(_.trip_id)).trip_id
        # self.vp_trip_keys = (self.vehicle_positions >> distinct(_.trip_key)).trip_key ##TODO switch
        self.scheduled_trip_rt_coverage = self.vp_trip_ids.size / (self.trips >> distinct(_.trip_id)).trip_id.size
        ## ^ should refactor
        self._generate_position_interpolators() ## comment out for first test
        # self._generate_stop_delay_view()
        
    def _generate_position_interpolators(self):
        '''For each trip_key in analysis, generate vehicle positions and schedule interpolator objects'''
        self.position_interpolators = {}
        # for trip_id in self.vp_trip_ids[:5]: ##small test
        for trip_id in self.vp_trip_ids: ##big test
            print(trip_id)
            trip = self.trips.copy() >> filter(_.trip_id == trip_id)
            self.debug_dict[f'{trip_id}_trip'] = trip
            st_trip_joined = (trip
                              >> inner_join(_, self.stop_times, on = ['calitp_itp_id', 'trip_id', 'service_date', 'trip_key'])
                              >> inner_join(_, self.stops, on = ['stop_id', 'calitp_itp_id'])
                             )
            st_trip_joined = gpd.GeoDataFrame(st_trip_joined, geometry=st_trip_joined.geometry, crs=CA_NAD83Albers)
            trip_positions_joined = self.trips_positions_joined >> filter(_.trip_id == trip_id)
            self.debug_dict[f'{trip_id}_st'] = st_trip_joined
            self.debug_dict[f'{trip_id}_vp'] = trip_positions_joined
            try:
                self.position_interpolators[trip_id] = {'rt': VehiclePositionsInterpolator(trip_positions_joined, self.routelines),
                                                           # 'schedule': ScheduleInterpolator(st_trip_joined, self.routelines) ## probably need to save memory for now
                                                       }
            except AssertionError as e:
                print(e)
            # except Exception as e:
            #     print(f'could not generate interpolators for {trip_id}')
            #     print(e)
            # TODO better checking for incomplete trips (either here or in interpolator...)
    
    def _generate_stop_delay_view(self):
        
        trips = self.trips >> select(_.trip_id, _.route_id, _.direction_id, _.shape_id)
        st = self.stop_times >> select(_.trip_key, _.trip_id, _.stop_id, _.stop_sequence, _.arrival_time)
        delays = self.stops >> select(_.stop_id, _.stop_name, _.geometry) >> inner_join(_, st, on = 'stop_id')
        delays = delays >> inner_join(_, trips, on = 'trip_id')
        delays['shape_meters'] = (delays.apply(lambda x:
                                (self.routelines >> filter(_.shape_id == x.shape_id)).geometry.iloc[0].project(x.geometry),
                         axis = 1))
        delays = delays >> filter(_.trip_id.isin(list(self.position_interpolators.keys())))
        _delays = gpd.GeoDataFrame()
        for trip_id in delays.trip_id.unique():
            # try:
            _delay = delays >> filter(_.trip_id == trip_id)
            _delay['actual_time'] = _delay.apply(lambda x: 
                            self.position_interpolators[x.trip_id]['rt'].time_at_position(x.shape_meters),
                            axis = 1)
            _delay = _delay.dropna(subset=['actual_time'])
            _delay['arrival_time'] = _delay.apply(lambda x:
                                dt.datetime.combine(x.actual_time.date(),
                                                    dt.datetime.strptime(x.arrival_time, '%H:%M:%S').time()),
                                                    axis = 1) ## format scheduled arrival times
            _delay = _delay >> filter(_.arrival_time.apply(lambda x: x.date()) == _.actual_time.iloc[0].date())
            _delay['arrival_time'] = _delay.arrival_time.astype('datetime64')
            return _delay
            _delay['delay'] = _delay.actual_time - delays.arrival_time
            _delay['delay'] = _delay.delay.apply(lambda x: dt.timedelta(seconds=0) if x.days == -1 else x)
            _delays = _delays.append(_delay)
            # except:
            #     print(f'could not generate delays for trip {trip_id}')
        self.stop_delay_view = _delays
        return
    
    def set_filter(self, start_time = None, end_time = None, route_ids = None, direction_id = None, direction = None):
        '''
        start_time, end_time: string %H:%M, for example '11:00' and '14:00'
        route_ids: list or pd.Series of route_ids
        direction_id: 0 or 1
        direction: string 'north', 'east', 'south', 'west' (experimental)
        '''
        assert start_time or end_time or route_ids or direction_id or direction, 'must supply at least 1 argument to filter'
        assert not start_time or type(dt.datetime.strptime(start_time, '%H:%M') == type(dt.datetime)), 'invalid time string'
        assert not end_time or type(dt.datetime.strptime(end_time, '%H:%M') == type(dt.datetime)), 'invalid time string'
        assert not route_ids or type(route_ids) == list or type(route_ids) == type(pd.Series())

        self.filter = {}
        self.filter['start_time'] = start_time
        self.filter['end_time'] = end_time
        self.filter['route_ids'] = route_ids
        self.filter['direction_id'] = direction_id
        self.filter['direction'] = direction
        
        # if start_time:
        #     view = view.filter(_.vehicle_timestamp.apply(lambda x: x.strftime('%H:%M')))
            
    def reset_filter(self):
        self.filter = None
        
    def map_segment_speeds(self, how = 'high_delay', segments = 'stops', trip_keys = None): ##TODO split out segment speed view?

        if  type(trip_keys) != type(None): ## trip_keys could potentially be a list or pd.Series...
            gdf = self.stop_delay_view.copy() >> filter(_.trip_key.isin(trip_keys))
        else:
            gdf = self.stop_delay_view.copy()
        
        assert how in ['low_speeds', 'average']
        assert segments in ['stops', 'detailed']
        
        speed_calculators = {'low_speeds': _.speed_mph.quantile(.2), ## 20th percentile speed
                            'average': _.speed_mph.mean()} ## average speed
                    
        all_stop_speeds = gpd.GeoDataFrame()
        for shape_id in gdf.shape_id.unique():
            for direction_id in gdf.direction_id.unique():
                this_shape_direction = (gdf
                             >> filter((_.shape_id == shape_id) & (_.direction_id == direction_id))).copy()
                stop_speeds = (this_shape_direction
                             >> group_by(_.trip_key)
                             >> arrange(_.stop_sequence)
                             >> mutate(seconds_from_last = (_.actual_time - _.actual_time.shift(1)).apply(lambda x: x.seconds))
                             >> mutate(last_loc = _.shape_meters.shift(1))
                             >> mutate(meters_from_last = (_.shape_meters - _.last_loc))
                             >> mutate(speed_from_last = _.meters_from_last / _.seconds_from_last) 
                             >> ungroup()
                            )
                stop_speeds.geometry = stop_speeds.apply(
                    lambda x: shapely.ops.substring(
                                self.trip_vehicle_positions[x.trip_key].shape.geometry.iloc[0],
                                x.last_loc,
                                x.shape_meters),
                                                axis = 1)
                stop_speeds = stop_speeds.dropna(subset=['last_loc']).set_crs(shared_utils.geography_utils.CA_NAD83Albers)

                try:
                    stop_speeds = (stop_speeds
                         >> mutate(speed_mph = _.speed_from_last * MPH_PER_MPS)
                         >> group_by(_.stop_sequence)
                         >> mutate(speed_mph = speed_calculators[how])
                         >> mutate(speed_mph = _.speed_mph.round(1))
                         >> mutate(shape_meters = _.shape_meters.round(0))
                         >> distinct(_.stop_sequence, _keep_all=True)
                         >> ungroup()
                         >> select(-_.arrival_time, -_.actual_time, -_.delay,
                                   -_.trip_id, -_.trip_key)
                        )
                except Exception as e:
                    print(f'stop_speeds shape: {stop_speeds.shape}, shape_id: {shape_id}, direction_id: {direction_id}')
                    continue

                print(stop_speeds.shape_id.iloc[0], stop_speeds.shape)
                if stop_speeds.speed_mph.max() > 70:
                    print(f'speed above 70 for shape {stop_speeds.shape_id.iloc[0]}, dropping')
                    stop_speeds = stop_speeds >> filter(_.speed_mph < 70)
                all_stop_speeds = all_stop_speeds.append(stop_speeds)

        self.segment_speed_view = all_stop_speeds
        return self._generate_segment_map(how = how)
    
    def _generate_segment_map(self, how, colorscale = None, size = [900, 550]):
        
        how_formatted = {'average': 'Average', 'low_speeds': '20th Percentile'}

        gdf = self.segment_speed_view >> select(-_.service_date)
        gdf.geometry = gdf.set_crs(CA_NAD83Albers).buffer(25)
        gdf = gdf.to_crs(WGS84)
        centroid = gdf.dissolve().centroid

        if not colorscale:
            colorscale = branca.colormap.step.RdYlGn_10.scale(vmin=gdf.speed_mph.min(), 
             vmax=gdf.speed_mph.max())
            colorscale.caption = "Speed (miles per hour)"

        popup_dict = {
            "speed_mph": "Speed (miles per hour)",
            "shape_meters": "Distance along route (meters)",
            "route_id": "Route",
            "direction": "Direction",
            "shape_id": "Shape ID",
            "direction_id": "Direction ID",
            "stop_id": "Next Stop ID",
            "stop_sequence": "Next Stop Sequence"
        }

        g = make_folium_choropleth_map(
            gdf,
            plot_col = 'speed_mph',
            popup_dict = popup_dict,
            tooltip_dict = popup_dict,
            colorscale = colorscale,
            fig_width = size[0], fig_height = size[1],
            zoom = 13,
            centroid = [centroid.y, centroid.x],
            title=f"Long Beach Transit {how_formatted[how]} Bus Speeds Between Stops, ** Peak",
            highlight_function=lambda x: {
                'fillColor': '#DD1C77',
                "fillOpacity": 0.6,
            }
        )

        return g  