import calitp
from calitp.tables import tbl
from siuba import *
import shared_utils

import pandas as pd
import numpy as np
import geopandas as gpd
import fiona
import datetime as dt

import shapely
from shapely.geometry import LineString, MultiPoint
from shapely.ops import split, substring

from ipyleaflet import Map, GeoJSON, projections, basemaps, GeoData, LayersControl, WidgetControl, GeoJSON
from ipywidgets import Text, HTML

import gcsfs
fs = gcsfs.GCSFileSystem()
import os

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/high_quality_transit_areas"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

itp_blue = shared_utils.calitp_color_palette.CALITP_CATEGORY_BOLD_COLORS[0]

def create_segments(geometry):
    """Splits a Shapely LineString into smaller LineStrings. If a MultiLineString passed,
    splits each LineString in that collection.
    """

    lines = []
    segment_distance_meters = 1250
    geometry = geometry.iloc[0]
    if hasattr(geometry, "geoms"):  ##check if MultiLineString
        linestrings = geometry.geoms
    else:
        linestrings = [geometry]
    for linestring in linestrings:
        for i in range(0, int(linestring.length), segment_distance_meters):
            lines.append(substring(linestring, i, i + segment_distance_meters))
    return lines

def find_stop_with_high_trip_count(segment, stops, stop_times, rank, calculated_stops):
    """Given a shape segment, finds the stop serving the most (or other rank) trips within that segment.
    Adds that stop's stop_id to segment data (a row).
    """

    stops_in_seg = gpd.clip(stops, segment.geometry)
    if stops_in_seg.size == 0:
        return segment

    stop_times_in_seg = stops_in_seg >> inner_join(_, stop_times, on="stop_id")
    trip_count_by_stop = (
        stop_times_in_seg >> count(_.stop_id) >> arrange(-_.n) >> rename(n_trips=_.n)
    )
    try:
        stop_id = trip_count_by_stop["stop_id"].iloc[rank - 1]

        if stop_id in list(calculated_stops):
            return segment
        segment["stop_id"] = stop_id
        segment["n_trips"] = trip_count_by_stop["n_trips"].iloc[rank - 1]
        return segment
    except IndexError:
        return segment

def fix_arrival_time(gtfs_timestring):
    """Reformats a GTFS timestamp (which allows the hour to exceed 24 to mark service day continuity)
    to standard 24-hour time.
    """
    split = gtfs_timestring.split(":")
    hour = int(split[0])
    if hour >= 24:
        split[0] = str(hour - 24)
        corrected = (":").join(split)
        return corrected.strip()
    else:
        return gtfs_timestring.strip()
    

def map_hqta(gdf, mouseover=None, name='gdf'):
    global nix_list
    nix_list = []
    
    if 'calitp_extracted_at' in gdf.columns:
        gdf = gdf.drop(columns='calitp_extracted_at')
    gdf = gdf.to_crs('EPSG:6414') ## https://epsg.io/6414 (meters)
    if gdf.geometry.iloc[0].geom_type == 'Point':
        gdf.geometry = gdf.geometry.buffer(200)
    
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
            
        def add_to_nix(feature, **kwargs):
            nix_list.append(feature['properties'][mouseover])
            
    if 'hq_transit_corr' in gdf.columns:
        geo_data_hq = GeoData(geo_dataframe = gdf[gdf['hq_transit_corr']].to_crs('EPSG:4326'),
                               style={'color': 'black', 'fillColor': '#08589e',
                                            'opacity':0.4, 'weight':.5, 'dashArray':'2', 'fillOpacity':0.3},
                               hover_style={'fillColor': 'red' , 'fillOpacity': 0.2},
                               name = 'HQTA')
        #a8ddb5
        geo_data_not_hq = GeoData(geo_dataframe = gdf[~gdf['hq_transit_corr']].to_crs('EPSG:4326'),
                               style={'color': 'black', 'fillColor': '#fec44f',
                                            'opacity':0.2, 'weight':.5, 'dashArray':'2', 'fillOpacity':0.3},
                               hover_style={'fillColor': 'red' , 'fillOpacity': 0.2},
                               name = 'non-HQTA')

        m.add_layer(geo_data_hq)
        m.add_layer(geo_data_not_hq)
    
    else:
    
        geo_data_hq = GeoData(geo_dataframe = gdf.to_crs('EPSG:4326'),
                               style={'color': 'black', 'fillColor': itp_blue,
                                            'opacity':0.4, 'weight':.5, 'dashArray':'2', 'fillOpacity':0.3},
                               hover_style={'fillColor': 'red' , 'fillOpacity': 0.2},
                               name = name)
        m.add_layer(geo_data_hq)
    
    if mouseover:
        geo_data_hq.on_hover(update_html)
        geo_data_hq.on_hover(add_to_nix)

    m.add_control(LayersControl())

    return m

# Something like this (psuedo code, you can fill it out with more detailed definitions since you're more familiar)
def hqta_details(row):
    if row.hqta_type == 'major_stop_bus':
        if row.calitp_itp_id_primary != int(row.calitp_itp_id_secondary):
            return 'intersection_2_bus_routes_different_operators'
        else:
            return 'intersection_2_bus_routes_same_operator'
    elif row.hqta_type == 'hq_corridor_bus':
          return 'stop_along_hq_bus_corridor_single_operator'
    elif row.hqta_type in ['major_stop_ferry', 'major_stop_brt', 'major_stop_rail']:     
         # (not sure if ferry, brt, rail, primary/secondary ids are filled in.)
        return row.hqta_type + '_single_operator'