import pandas as pd
import numpy as np
import geopandas as gpd
import datetime as dt

import shared_utils

import calitp
from calitp.tables import tbl
from siuba import *

import requests
import intake

from ipyleaflet import Map, GeoJSON, projections, basemaps, GeoData, LayersControl, WidgetControl, GeoJSON, LegendControl
from ipywidgets import Text, HTML

## get a key here if needed: https://www.census.gov/data/developers/guidance.html
# census_api_key = "&key="
census_api_key = ""

GCS_FILE_PATH = 'gs://calitp-analytics-data/data-analyses/msd_dashboard_metric/'

catalog = intake.open_catalog('./catalog.yml')

# Old CA block group geometry
def get_ca_block_group_geo():
    ca_block_geo = catalog.ca_block_groups.read()
    ca_block_geo = ca_block_geo.to_crs('EPSG:4326')
    stanford_shorelines = catalog.stanford_shorelines.read()
    ca_shoreline = stanford_shorelines >> filter(_.STFIPS == '06')
    ca_block_geo = ca_block_geo.clip(ca_shoreline)
    ca_block_geo = ca_block_geo.to_crs(shared_utils.geography_utils.CA_NAD83Albers)
    
    return ca_block_geo

# Use this one, move to TIGER file
def get_ca_block_geo():
    # Bring in block geometry
    ca_blocks = gpd.read_parquet(f'{utils.GCS_FILE_PATH}2020_tiger_block_geo.parquet')
    ca_blocks = (ca_blocks >> filter(_.ALAND20 > 10) ## remove water
                 >> select(_.county == _.COUNTYFP20, 
                           _.tract == _.TRACTCE20, 
                           _.block == _.BLOCKCE20,
                           _.geo_id == _.GEOID20, _.geometry))
    ca_blocks = ca_blocks.to_crs(shared_utils.geography_utils.CA_NAD83Albers)
    
    # Bring in block population
    ca_block_pop = catalog.ca_block_population.read()
    ca_block_pop['GEO_ID'] = ca_block_pop.GEO_ID.apply(lambda x: x.split('US')[1])
    ca_block_pop = ca_block_pop >> select(_.geo_id == _.GEO_ID, 
                                          _.block_pop == _.P1_001N)
    
    # Merge block geometry with population
    ca_block_joined = ca_blocks >> inner_join(_, ca_block_pop, on='geo_id')
        
    ca_block_joined = ca_block_joined.astype({
        'county':'int64', 
        'tract':'int64', 
        'block':'int64', 
        'geo_id':'int64'})
    ca_block_joined['area'] = ca_block_joined.geometry.area
    ca_block_joined['geometry'] = ca_block_joined.geometry.simplify(tolerance=100)
    
    return ca_block_joined


def get_stops_and_trips(filter_accessible):
    '''
    Returns a basic view of stops x modes serving stop.
    
    If filter_accessible == True, return only stops that explicitly
    provide wheelchair boarding and are served by a wheelchair accessible trip
    '''
    stops = warehouse_queries.stops
    trips = warehouse_queries.trips
    
    if filter_accessible:
        stops = stops >> filter(_.wheelchair_boarding == '1')
        trips = trips >> filter(_.wheelchair_accessible == '1')
    
    route_mode = (tbl.gtfs_schedule.routes()
                    >> select(_.calitp_itp_id, _.calitp_url_number, 
                              _.route_id, _.route_type)
                 )
    trips_route_joined = trips >> inner_join(_, route_mode, 
                                             on=['calitp_itp_id', 
                                                 'calitp_url_number', 'route_id'])
    stops_trips = (tbl.gtfs_schedule.stop_times()
      >> select(_.calitp_itp_id, _.calitp_url_number, _.trip_id,
               _.stop_id)
      >> inner_join(_, trips_route_joined, on=['calitp_itp_id',
                            'calitp_url_number', 'trip_id'])
      >> inner_join(_, stops, on=['calitp_itp_id',
                            'calitp_url_number', 'stop_id'])
      # >> collect()
      ## actually a trip count could be cool? (another use for a frequency table...)
      >> distinct(_.stop_id, _.route_type, _.stop_lon, _.stop_lat,
                  _.calitp_itp_id, _.calitp_url_number, _.wheelchair_boarding,
                 _.wheelchair_accessible)
      >> collect()
     )
    
    stops_trips = (gpd.GeoDataFrame(stops_trips,
                        geometry=gpd.points_from_xy(stops_trips.stop_lon,
                                                   stops_trips.stop_lat),
                        crs = 'EPSG:4326')
                   .to_crs(shared_utils.geography_utils.CA_NAD83Albers)
                  )
    
    return stops_trips


def get_census_ca_counties(census_vars, geography='tract'):
    
    ca_counties = requests.get('https://api.census.gov/data/2019/acs/acs5?'
                               f'get=NAME,B01001_001E&for=county:*&in=state:06{census_api_key}')
    ca_county_codes = [x[-1] for x in ca_counties.json()[1:]]

    census_df = pd.DataFrame()

    for county in ca_county_codes:

        query = f'''\
            https://api.census.gov/data/2019/acs/acs5?get=NAME,\
            {census_vars}&for={geography}:*&\
            in=state:06%20county:{county}{census_api_key}\
        '''
        r = requests.get(query)
        print(query)
        # print(r.status_code)
        json = r.json()
        cols = json[0]
        data = json[1:]
        census_df = census_df.append(pd.DataFrame(data, columns=cols))
        census_df = census_df.drop(columns=['NAME']).astype('int64')
        
    return census_df


def simple_map(gdf, mouseover=None):
    
    x = gdf.to_crs('EPSG:4326').geometry.iloc[0].centroid.x
    y = gdf.to_crs('EPSG:4326').geometry.iloc[0].centroid.y
    
    m = Map(basemap=basemaps.CartoDB.Positron, center=[y, x], zoom=11)

    if mouseover:
        html = HTML(f'hover to see {mouseover}')
        html.layout.margin = '0px 20px 20px 20px'
        control = WidgetControl(widget=html, position='topright')
        m.add_control(control)

        def update_html(feature,  **kwargs):
            html.value = '''
                <h3><b>{}</b></h3>
            '''.format(feature['properties'][mouseover])
            
        
    geo_data = GeoData(geo_dataframe = gdf.to_crs('EPSG:4326'),
                   style={'color': 'black', 'fillColor': '#998ec3',
                                'opacity':0.2, 'weight':.5, 'dashArray':'2', 'fillOpacity':0.3},
                   hover_style={'fillColor': 'red' , 'fillOpacity': 0.3},
                   name = 'data')

    m.add_layer(geo_data)
        
    if mouseover:
        geo_data.on_hover(update_html)

    m.add_control(LayersControl())

    return m


def calculate_access_proportion(num_df, denom_df, col):
    proportion = num_df[col].sum() / denom_df[col].sum()
    percentage = (proportion * 100).round(2)
    return percentage