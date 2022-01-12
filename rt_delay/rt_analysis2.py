import shared_utils
from shared_utils.geography_utils import CA_NAD83Albers
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
        
        assert position_gdf.crs == CA_NAD83Albers and shape_gdf.crs == CA_NAD83Albers, f"vehicle positions and shape CRS must be {CA_NAD83Albers}"
        assert position_gdf.trip_key.nunique() == 1, "non-unique trip key in vp_gdf"
        
        trip_info_cols = ['service_date', 'trip_key', 'trip_id', 'route_id', 'shape_id',
                         'direction_id', 'calitp_itp_id',
                         'calitp_url_number'] + addl_info_cols
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
        gdf = gdf.to_crs(shared_utils.geography_utils.WGS84)
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
            centroid = [33.790, -118.154],
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
    
    def _shift_calculate(self, vehicle_positions):
        
        print('sc_called')
        if hasattr(self, "progressing_positions"):
            print(self.progressing_positions.shape)
            self.debug_dict[self.progressing_positions.shape[0]] = self.progressing_positions.copy()
        else:
            print(self.position_gdf.shape)
            self.debug_dict[self.position_gdf.shape[0]] = self.position_gdf.copy()
        
        # vehicle_positions = vehicle_positions >> arrange(_.vehicle_timestamp) ## unnecessary?
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
    