import shared_utils
import branca
from utils import *

from siuba import *
import pandas as pd
import geopandas as gpd
import shapely

import datetime as dt
import time
from zoneinfo import ZoneInfo

class VehiclePositionsTrip:
    '''Trip data and useful methods for analyzing GTFS-RT vehicle positions data'''
    
    def __init__(self, vp_gdf, shape_gdf):
        
        global vp_gdff
        vp_gdff = vp_gdf
        
        assert vp_gdf.crs == shared_utils.geography_utils.CA_NAD83Albers
        vp_gdf = vp_gdf >> distinct(_.trip_id, _.vehicle_timestamp, _keep_all=True)
        
        self.date = vp_gdf.date.iloc[0]
        self.trip_id = vp_gdf.trip_id.iloc[0]
        self.route_id = vp_gdf.route_id.iloc[0]
        self.shape_id = vp_gdf.shape_id.iloc[0]
        self.direction_id = vp_gdf.direction_id.iloc[0]
        self.entity_id = vp_gdf.entity_id.iloc[0]
        self.vehicle_id = vp_gdf.vehicle_id.iloc[0]
        self.calitp_itp_id = vp_gdf.calitp_itp_id.iloc[0]
        self.calitp_url_number = vp_gdf.calitp_url_number.iloc[0]
        self.vehicle_positions = vp_gdf >> select(_.vehicle_timestamp,
                                              _.header_timestamp,
                                              _.geometry)
        self._attach_shape(shape_gdf)
        
    def _attach_shape(self, shape_gdf):
        assert shape_gdf.crs == shared_utils.geography_utils.CA_NAD83Albers
        assert shape_gdf.calitp_itp_id.iloc[0] == self.calitp_itp_id
        self.shape = (shape_gdf
                        >> filter(_.shape_id == self.shape_id)
                        >> select(_.shape_id, _.geometry))
        self.vehicle_positions['shape_meters'] = (self.vehicle_positions.geometry
                                .apply(lambda x: self.shape.geometry.iloc[0].project(x)))
        self._linear_reference()
        
        origin = (self.vehicle_positions >> filter(_.shape_meters == _.shape_meters.min())
        ).geometry.iloc[0]
        destination = (self.vehicle_positions >> filter(_.shape_meters == _.shape_meters.max())
        ).geometry.iloc[0]
        self.direction = primary_cardinal_direction(origin, destination)
        
    def _linear_reference(self):
        
        self.vehicle_positions = self._shift_calculate(self.vehicle_positions)
        self.progressing_positions = self.vehicle_positions >> filter(_.progressed)
        ## check if positions have progressed from immediate previous point, but not previous point of forwards progression
        if not self.progressing_positions.shape_meters.is_monotonic:
            print(f'check location data for trip {self.trip_id}')
            self._fix_progression()
    
    def _fix_progression(self):
        
        self.progressing_positions = self._shift_calculate(self.progressing_positions)
        self.progressing_positions = self.progressing_positions >> filter(_.progressed)
        ## check if positions have progressed from immediate previous point, but not previous point of forwards progression
        if not self.progressing_positions.shape_meters.is_monotonic:
            # print(f'recheck location data for trip {self.trip_id}')
            self._fix_progression()
    
    def _shift_calculate(self, vehicle_positions):
        vehicle_positions['last_time'] = vehicle_positions.vehicle_timestamp.shift(1)
        vehicle_positions['last_loc'] = vehicle_positions.shape_meters.shift(1)
        vehicle_positions['secs_from_last'] = vehicle_positions.vehicle_timestamp - vehicle_positions.last_time
        vehicle_positions.secs_from_last = (vehicle_positions.secs_from_last
                                        .apply(lambda x: x.seconds))
        vehicle_positions['meters_from_last'] = (vehicle_positions.shape_meters
                                                      - vehicle_positions.last_loc)
        vehicle_positions['progressed'] = vehicle_positions['meters_from_last'] > 0 ## has the bus moved ahead?
        vehicle_positions['speed_from_last'] = (vehicle_positions.meters_from_last
                                                     / vehicle_positions.secs_from_last) ## meters/second
        return vehicle_positions
        
        
#     def position_at_time(self, dt): ## implement if/when needed
        
    def time_at_position(self, desired_position):
        
        global bounding_points
        
        try:
            next_point = (self.progressing_positions
                  >> filter(_.shape_meters > desired_position)
                  >> filter(_.shape_meters == _.shape_meters.min())
                 )
            prev_point = (self.progressing_positions
                  >> filter(_.shape_meters < desired_position)
                  >> filter(_.shape_meters == _.shape_meters.max())
                 )
            bounding_points = (prev_point.append(next_point).copy().reset_index(drop=True)
                    >> select(-_.secs_from_last, -_.meters_from_last, -_.speed_from_last)) ## drop in case bounding points are nonconsecutive
            secs_from_last = (bounding_points.loc[1].vehicle_timestamp - bounding_points.loc[0].vehicle_timestamp).seconds
            meters_from_last = bounding_points.loc[1].shape_meters - bounding_points.loc[0].shape_meters
            speed_from_last = meters_from_last / secs_from_last

            meters_position_to_next = bounding_points.loc[1].shape_meters - desired_position
            est_seconds_to_next = meters_position_to_next / speed_from_last
            est_td_to_next = dt.timedelta(seconds=est_seconds_to_next)
            est_dt = bounding_points.iloc[-1].vehicle_timestamp - est_td_to_next

            return est_dt
        except KeyError:
            print(f'insufficient bounding points for trip {self.trip_id}, location {desired_position}', end=': ')
            print(f'start/end of route?')
            return None
        
    def detailed_speed_map(self):
        
        gdf = self.vehicle_positions.copy()
        gdf['time'] = gdf.vehicle_timestamp.apply(lambda x: x.strftime('%H:%M:%S'))

        gdf = gdf >> select(_.geometry, _.time,
                            _.shape_meters, _.last_loc, _.speed_from_last)
        gdf['speed_mph'] = gdf.speed_from_last * MPH_PER_MPS
        
        gdf.geometry = gdf.apply(lambda x: shapely.ops.substring(self.shape.geometry.iloc[0],
                                                                x.last_loc,
                                                                x.shape_meters),
                                axis = 1)
        
        gdf.geometry = gdf.buffer(25)
        gdf = gdf.to_crs(shared_utils.geography_utils.WGS84)
        gdf = gdf >> filter(_.speed_mph > 0)
        gdf = gdf >> mutate(speed_mph = _.speed_mph.round(1),
                           shape_meters = _.shape_meters.round(0))
        
        gdf['shape_id'] = self.shape_id
        gdf['direction_id'] = self.direction_id
        gdf['trip_id'] = self.trip_id
        # display(gdf)
        
        if gdf.speed_mph.max() > 70:
            print(f'speed above 70 for trip {self.trip_id}, dropping')
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
        
        g = shared_utils.map_utils.make_folium_choropleth_map(
            gdf,
            plot_col = 'speed_mph',
            popup_dict = popup_dict,
            tooltip_dict = popup_dict,
            colorscale = colorscale,
            fig_width = 1000, fig_height = 700,
            zoom = 13,
            centroid = [33.790, -118.154],
            title=f"Trip Speed Map (Route {self.route_id}, {self.direction}, PM Peak)", ##TODO time classification, remove hardcode
            highlight_function=lambda x: {
                'fillColor': '#DD1C77',
                "fillOpacity": 0.6,
            }
        )

        return g
    
class RtAnalysis:
    '''Current top-level class for GTFS-RT analysis'''
    
    def __init__(self, trips_positions_joined, stop_times, stops, shape_gdf, trip_ids): ## trips_position_joined is temporary
        
        for df in (trips_positions_joined, stop_times, stops, shape_gdf):
            assert df.calitp_itp_id.nunique() == 1
            assert df.calitp_url_number.nunique() == 1
        
        self.trips_positions = trips_positions_joined >> filter(_.trip_id.isin(trip_ids))
        self.stop_times = stop_times >> filter(_.trip_id.isin(trip_ids))
        self.stops = stops
        self.st_geo = self.stops >> inner_join(_, self.stop_times, on = ['calitp_itp_id', 'calitp_url_number','stop_id'])
        self.shape_gdf = shape_gdf
        self.trip_ids = trip_ids
        self.generate_vp_trips()
        
    def generate_vp_trips(self):
        self.trip_vehicle_positions = {}
        for trip_id in self.trip_ids:
            self.trip_vehicle_positions[trip_id] = VehiclePositionsTrip(
                                            self.trips_positions >> filter(_.trip_id == trip_id),
                                            self.shape_gdf)
        
    def generate_delay_view(self, trip_ids = None):
        print('gdv called')

        if  type(trip_ids) == type(None): ## trip_ids could potentially be a list or pd.Series...
            trip_ids = self.trip_ids
        
        self._delay_view_inputs = trip_ids
        self.delay_view = gpd.GeoDataFrame()
        trips_processed = 0
        for trip_id in trip_ids:
            try:
                trip_rt_data = self.trip_vehicle_positions[trip_id]
                trip_st = (self.stop_times >> filter(_.trip_id == trip_id)).copy()
                trip_st_geo = (self.st_geo >> filter(_.trip_id == trip_id)).copy()
                trip_st_geo['route_id'] = trip_rt_data.route_id
                trip_st_geo['shape_id'] = trip_rt_data.shape_id
                trip_st_geo['direction_id'] = trip_rt_data.direction_id
                trip_st_geo['direction'] = trip_rt_data.direction
                trip_st_geo['shape_meters'] = (trip_st_geo.geometry
                                                .apply(lambda x: trip_rt_data.shape.project(x)))
                trip_st_geo['actual_time'] = (trip_st_geo.shape_meters
                                              .apply(lambda x: trip_rt_data.time_at_position(x)))
                trip_st_geo = trip_st_geo.dropna(subset=['actual_time'])
                trip_st_geo['arrival_time'] = trip_st_geo.apply(lambda x:
                                            dt.datetime.combine(x.actual_time.date(),
                                                                dt.datetime.strptime(x.arrival_time, '%H:%M:%S').time()),
                                                                axis = 1) ## format scheduled arrival times
                # _debug = trip_st_geo
                trip_st_geo['delay'] = trip_st_geo.actual_time - trip_st_geo.arrival_time
                trip_st_geo['date'] = trip_rt_data.date
                trip_view = trip_st_geo.dropna(subset=['delay']) >> arrange(_.arrival_time) >> select(
                                                                _.arrival_time, _.actual_time, _.delay,
                                                             _.stop_id, _.trip_id, _.shape_id, _.direction_id,
                                                            _.direction, _.stop_sequence, _.route_id,
                                                            _.shape_meters, _.date, _.geometry)
                self.delay_view = self.delay_view.append(trip_view)
                trips_processed += 1
                if trips_processed % 5 == 0:
                    print(f'{trips_processed} trips processed')
            except Exception as e:
                print(trip_id, e)
        self.delay_view = (self.delay_view >> arrange(_.stop_sequence, _.trip_id)).set_crs(self.st_geo.crs)
        return self.delay_view
    
    def generate_delay_summary(self, trip_ids = None):       

        if  type(trip_ids) == type(None): ## trip_ids could potentially be a list or pd.Series...
            trip_ids = self.trip_ids
        
        if hasattr(self, 'delay_view') and set(list(self.delay_view.trip_id.unique())) == set(list(trip_ids)):
        ## if delay view exists and matches request, use it
            stop_geos = self.delay_view >> select(_.stop_id, _.geometry) >> distinct(_.stop_id, _keep_all=True)
            self.delay_summary = (self.delay_view
                     >> group_by(_.stop_id, _.stop_sequence,)
                     >> summarize(avg_delay = _.delay.mean(), max_delay = _.delay.max())
                     # >> inner_join(_, stop_geos, on = 'stop_id')
                     >> arrange(_.stop_sequence)
                    )
            self.delay_summary = stop_geos >> inner_join(_, self.delay_summary, on = 'stop_id')
        else:
            self.generate_delay_view(trip_ids) ## generate new delay view if necessary
            return self.generate_delay_summary(trip_ids)
        return self.delay_summary
    
    def map_stop_delays(self, how = 'max',  trip_ids = None):
                
        if  type(trip_ids) == type(None): ## trip_ids could potentially be a list or pd.Series...
            trip_ids = self.trip_ids
        assert how in ['max', 'average']
        self.generate_delay_summary(trip_ids)
        
        gdf = self.delay_summary.copy()
        if how == 'max':
            gdf['delay_minutes'] = gdf.max_delay.apply(lambda x: x.seconds / 60)
        elif how == 'average':
            gdf['delay_minutes'] = gdf.avg_delay.apply(lambda x: x.seconds / 60)
        gdf['delay_minutes'] = gdf.delay_minutes.round(0)
        gdf = gdf >> select(_.stop_id, _.geometry, _.delay_minutes)
        gdf.geometry = gdf.buffer(50)
        
        gdf = gdf.to_crs(shared_utils.geography_utils.WGS84)

        colorscale = reversed_colormap(branca.colormap.step.RdYlGn_08.scale(vmin=0, 
                     vmax=gdf.delay_minutes.max()))
        colorscale.caption = "Delay (minutes)"
        
        popup_dict = {
            "delay_minutes": "Delay (minutes)",
            "stop_id": "Stop ID",
        }
        
        g = shared_utils.map_utils.make_folium_choropleth_map(
            gdf,
            plot_col = 'delay_minutes',
            popup_dict = popup_dict,
            tooltip_dict = popup_dict,
            colorscale = colorscale,
            fig_width = 1000, fig_height = 700,
            zoom = 13,
            centroid = [33.790, -118.154],
            title="Stop Delay Map"
        )

        return g

        
    def map_segment_speeds(self, how = 'high_delay', segments = 'stops', trip_ids = None): ##TODO split out segment speed view?

        if  type(trip_ids) == type(None): ## trip_ids could potentially be a list or pd.Series...
            trip_ids = self.trip_ids
        
        assert how in ['low_speeds', 'average']
        assert segments in ['stops', 'detailed']
        
        speed_calculators = {'low_speeds': _.speed_mph.quantile(.2), ## 20th percentile speed
                            'average': _.speed_mph.mean()} ## average speed
        
        if hasattr(self, 'delay_view') and set(list(self._delay_view_inputs)) == set(list(trip_ids)):
            
            all_stop_speeds = gpd.GeoDataFrame()
            for shape_id in self.delay_view.shape_id.unique():
                for direction_id in self.delay_view.direction_id.unique():
                    stop_speeds = (self.delay_view
                                 >> filter((_.shape_id == shape_id) & (_.direction_id == direction_id))
                                 >> group_by(_.trip_id)
                                 >> arrange(_.stop_sequence)
                                 >> mutate(seconds_from_last = (_.actual_time - _.actual_time.shift(1)).apply(lambda x: x.seconds))
                                 >> mutate(last_loc = _.shape_meters.shift(1))
                                 >> mutate(meters_from_last = (_.shape_meters - _.last_loc))
                                 >> mutate(speed_from_last = _.meters_from_last / _.seconds_from_last) 
                                 >> ungroup()
                                )
                    stop_speeds.geometry = stop_speeds.apply(
                        lambda x: shapely.ops.substring(
                                    self.trip_vehicle_positions[x.trip_id].shape.geometry.iloc[0],
                                    x.last_loc,
                                    x.shape_meters),
                                                    axis = 1)
                    stop_speeds = stop_speeds.dropna(subset=['last_loc']).set_crs(shared_utils.geography_utils.CA_NAD83Albers)

                    stop_speeds = (stop_speeds
                         >> mutate(speed_mph = _.speed_from_last * MPH_PER_MPS)
                         >> group_by(_.stop_sequence)
                         >> mutate(speed_mph = speed_calculators[how])
                         >> mutate(speed_mph = _.speed_mph.round(1))
                         >> mutate(shape_meters = _.shape_meters.round(0))
                         >> distinct(_.stop_sequence, _keep_all=True)
                         >> ungroup()
                         >> select(-_.arrival_time, -_.actual_time, -_.delay, -_.trip_id)
                        )
                    print(stop_speeds.shape_id.iloc[0], stop_speeds.shape)
                    if stop_speeds.speed_mph.max() > 70:
                        print(f'speed above 70 for shape {stop_speeds.shape_id.iloc[0]}, dropping')
                        stop_speeds = stop_speeds >> filter(_.speed_mph < 70)
                    all_stop_speeds = all_stop_speeds.append(stop_speeds)
            
            self.segment_speed_view = all_stop_speeds
            return self._generate_segment_map(how = how)
            
        else:
            self.generate_delay_view(trip_ids) ## generate new delay view if necessary
            return self.map_segment_speeds(how, segments, trip_ids = trip_ids)
        
    def _generate_segment_map(self, how, colorscale = None, size = [900, 550]):
        
        how_formatted = {'average': 'Average', 'low_speeds': '20th Percentile'}

        gdf = self.segment_speed_view >> select(-_.date)
        gdf.geometry = gdf.set_crs(shared_utils.geography_utils.CA_NAD83Albers).buffer(25)
        
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

        g = shared_utils.map_utils.make_folium_choropleth_map(
            gdf,
            plot_col = 'speed_mph',
            popup_dict = popup_dict,
            tooltip_dict = popup_dict,
            colorscale = colorscale,
            fig_width = size[0], fig_height = size[1],
            zoom = 13,
            centroid = [33.790, -118.154],
            title=f"Long Beach Transit {how_formatted[how]} Bus Speeds Between Stops, Afternoon Peak",
            highlight_function=lambda x: {
                'fillColor': '#DD1C77',
                "fillOpacity": 0.6,
            }
        )

        return g  
    
    def route_coverage_summary(self):
        
        if hasattr(self, 'delay_view'):
            summary = (self.delay_view
             >> group_by(_.trip_id, _.shape_id, _.direction_id)
             >> summarize(min_meters = _.shape_meters.min(),
                         min_stop = _.stop_sequence.min(),
                         max_meters = _.shape_meters.max(),
                         max_stop = _.stop_sequence.max())
            )
            return summary
        else:
            self.generate_delay_view()
            return self.route_coverage_summary()