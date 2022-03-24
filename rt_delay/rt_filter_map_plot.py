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
from tqdm import tqdm

import numpy as np
from calitp.tables import tbl
import seaborn as sns
import matplotlib.pyplot as plt

class RtFilterMapper:
    '''
    Collects filtering and mapping functions, init with stop segment speed view and rt_trips
    '''
    def __init__(self, rt_trips, segment_speed_view):
        self.rt_trips = rt_trips
        self.stop_segment_speed_view = segment_speed_view
        self.stop_segment_speed_view = self.stop_segment_speed_view >> filter(_.speed_mph < 70)
        self.calitp_agency_name = rt_trips.calitp_agency_name.iloc[0]
        self.analysis_date = rt_trips.service_date.iloc[0]
        self.display_date = self.analysis_date.strftime('%b %d (%a)')

    def set_filter(self, start_time = None, end_time = None, route_names = None,
                   shape_ids = None, direction_id = None, direction = None):
        '''
        start_time, end_time: string %H:%M, for example '11:00' and '14:00'
        route_names: list or pd.Series of route_names (GTFS route_short_name)
        direction_id: '0' or '1'
        direction: string 'Northbound', 'Eastbound', 'Southbound', 'Westbound' (experimental)
        '''
        assert start_time or end_time or route_names or direction_id or direction or shape_ids, 'must supply at least 1 argument to filter'
        assert not start_time or type(dt.datetime.strptime(start_time, '%H:%M') == type(dt.datetime)), 'invalid time string'
        assert not end_time or type(dt.datetime.strptime(end_time, '%H:%M') == type(dt.datetime)), 'invalid time string'
        assert not route_names or type(route_names) == list or type(route_names) == tuple or type(route_names) == type(pd.Series())
        if route_names:
            # print(route_names)
            # print(type(route_names))
            assert pd.Series(route_names).isin(self.rt_trips.route_short_name).all(), 'at least 1 route not found in self.rt_trips'
        assert not direction_id or type(direction_id) == str and len(direction_id) == 1
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
        self.filter['shape_ids'] = shape_ids
        if shape_ids:
            shape_trips = (self.rt_trips >> filter(_.shape_id.isin(shape_ids))
                           >> distinct(_.route_short_name, _keep_all=True)
                           >> collect()
                          )
            self.filter['route_names'] = list(shape_trips.route_short_name)
            if len(shape_ids) == 1:
                direction = (self.rt_trips >> filter(_.shape_id == shape_ids[0])).direction.iloc[0]
        self.filter['direction_id'] = direction_id
        self.filter['direction'] = direction
        if start_time and end_time:
            self.hr_duration_in_filter = (dt.datetime.combine(self.analysis_date, self.filter['end_time'])
                                 - dt.datetime.combine(self.analysis_date, self.filter['start_time'])
                                ).seconds / 60**2
        else:
            self.hr_duration_in_filter = (self.vehicle_positions.vehicle_timestamp.max() - 
                                         self.vehicle_positions.vehicle_timestamp.min()).seconds / 60**2
        if self.filter['route_names'] and len(self.filter['route_names']) < 5:
            rts = 'Route(s) ' + ', '.join(self.filter['route_names'])
        elif self.filter['route_names'] and len(self.filter['route_names']) > 5:
            rts = 'Multiple Routes'
        elif not self.filter['route_names']:
            rts = 'All Routes'
            
        print(self.filter)
        ## properly format for pm peak, TODO add other periods
        if start_time and end_time and start_time == '15:00' and end_time == '19:00':
            period = 'PM Peak'
        elif not start_time and not end_time:
            period = 'All Day'
        else:
            period = f'{start_time}–{end_time}'
        elements_ordered = [rts, direction, period, self.display_date]
        self.filter_formatted = ', ' + ', '.join([str(x) for x in elements_ordered if x])
            
    def reset_filter(self):
        self.filter = None
    
    def _filter(self, df):
        '''Filters a df (containing trip_id) on trip_id based on any set filter.
        '''
        if self.filter == None:
            return df
        else:
            trips = self.rt_trips.copy()
            print(f'view filter: {self.filter}')
        if self.filter['start_time']:
            trips = trips >> filter(_.median_time > self.filter['start_time'])
        if self.filter['end_time']:
            trips = trips >> filter(_.median_time < self.filter['end_time'])
        if self.filter['route_names']:
            trips = trips >> filter(_.route_short_name.isin(self.filter['route_names']))
        if self.filter['shape_ids']:
            trips = trips >> filter(_.shape_id.isin(self.filter['shape_ids']))
        if self.filter['direction_id']:
            trips = trips >> filter(_.direction_id == self.filter['direction_id'])
        if self.filter['direction']:
            trips = trips >> filter(_.direction == self.filter['direction'])
        return df.copy() >> inner_join(_, trips >> select(_.trip_id), on = 'trip_id')
    
## TODO, optionally port over segment speed map from rt_analysis, would require stop delay view
## alternatively, simply export full day segment speed view
    
    def show_speed_map(self, how = 'average',
                          colorscale = ZERO_THIRTY_COLORSCALE, size = [900, 550]):
        
        gdf = self.stop_segment_speed_view.copy()
        gdf = self._filter(gdf)
        # print(gdf.dtypes)
        singletrip = gdf.trip_id.nunique() == 1
        gdf = gdf >> distinct(_.shape_id, _.stop_sequence, _keep_all=True) ## essential here for reasonable map size!
        orig_rows = gdf.shape[0]
        gdf = gdf.round({'avg_mph': 1, '_20p_mph': 1, 'shape_meters': 0,
                        'trips_per_hour': 1}) ##round for display
        
        how_speed_col = {'average': 'avg_mph', 'low_speeds': '_20p_mph'}
        how_formatted = {'average': 'Average', 'low_speeds': '20th Percentile'}

        gdf = gdf >> select(-_.service_date)
        gdf = gdf >> arrange(_.trips_per_hour)
        gdf = gdf.set_crs(CA_NAD83Albers)
        
        ## shift to right side of road to display direction
        gdf.geometry = gdf.geometry.apply(try_parallel)
        self.detailed_map_view = gdf.copy()
        ## create clips, integrate buffer+simplify?
        gdf.geometry = gdf.geometry.apply(arrowize_segment).simplify(tolerance=5)
        gdf = gdf >> filter(gdf.geometry.is_valid)
        gdf = gdf >> filter(-gdf.geometry.is_empty)
        
        assert gdf.shape[0] >= orig_rows*.99, 'over 1% of geometries invalid after buffer+simplify'
        gdf = gdf.to_crs(WGS84)
        centroid = gdf.dissolve().centroid 
        name = self.calitp_agency_name

        popup_dict = {
            how_speed_col[how]: "Speed (miles per hour)",
            "shape_meters": "Distance along route (meters)",
            "route_short_name": "Route",
            "shape_id": "Shape ID",
            "direction_id": "Direction ID",
            "stop_id": "Next Stop ID",
            "stop_sequence": "Next Stop Sequence",
            "trips_per_hour": "Trips per Hour" 
        }
        if singletrip:
            popup_dict["delay_sec"] = "Current Delay (seconds)"
            popup_dict["delay_chg_sec"] = "Change in Delay (seconds)"

        g = make_folium_choropleth_map(
            gdf,
            plot_col = how_speed_col[how],
            popup_dict = popup_dict,
            tooltip_dict = popup_dict,
            colorscale = colorscale,
            fig_width = size[0], fig_height = size[1],
            zoom = 13,
            centroid = [centroid.y, centroid.x],
            title=f"{name} {how_formatted[how]} Vehicle Speeds Between Stops{self.filter_formatted}",
            legend_name = "Speed (miles per hour)",
            highlight_function=lambda x: {
                'fillColor': '#DD1C77',
                "fillOpacity": 0.6,
            }
        )

        return g  

    def chart_delays(self):
        '''
        A bar chart showing delays grouped by arrival hour for current filtered selection
        '''
        filtered_endpoint = self._filter(self.endpoint_delay_view)
        grouped = (filtered_endpoint >> group_by(_.arrival_hour)
                   >> summarize(mean_end_delay = _.delay.mean())
                  )
        grouped['Minutes of Delay at Endpoint'] = grouped.mean_end_delay.apply(lambda x: x.seconds / 60)
        grouped['Hour'] = grouped.arrival_hour
        sns_plot = (sns.barplot(x=grouped['Hour'], y=grouped['Minutes of Delay at Endpoint'], ci=None, 
                       palette=[shared_utils.calitp_color_palette.CALITP_CATEGORY_BOLD_COLORS[1]])
            .set_title(f"{self.calitp_agency_name} Mean Delays by Arrival Hour{self.filter_formatted}")
           )
        chart = sns_plot.get_figure()
        chart.tight_layout()
        return chart
    
    def chart_variability(self, min_stop_seq = None, max_stop_seq = None):
        '''
        Chart trip speed variability, as speed between each stop segments.
        stop_sequence_range: (min_stop, max_stop)
        '''
        sns.set(rc = {'figure.figsize':(13,6)})
        assert (self.filter['shape_ids']
                and len(self.filter['shape_ids']) == 1), 'must filter to a single shape_id'
        to_chart = self._filter(self.stop_segment_speed_view.copy())
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
        variability_plt = sns.swarmplot(x = to_chart['Segment Cross Street'], y=to_chart['Segement Speed (mph)'],
              palette=shared_utils.calitp_color_palette.CALITP_CATEGORY_BRIGHT_COLORS,
             ).set_title(f"{self.calitp_agency_name} Speed Variability by Stop Segment{self.filter_formatted}")
        return variability_plt