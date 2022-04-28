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
from calitp.storage import get_fs
fs = get_fs()
import os

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/high_quality_transit_areas"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

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
    
def get_operator_views(itp_id):
    """Returns relevant views from the data warehouse for a single transit operator."""
    ## TODO refactor to new shapes?
    ## actually refactor this whole thing to use views, a new definite date, etc...
    shapes = (
        tbl.gtfs_schedule.shapes()
        >> filter(_.calitp_itp_id == int(itp_id))
        >> collect()
    )
    shapes = gpd.GeoDataFrame(
        shapes,
        geometry=gpd.points_from_xy(shapes.shape_pt_lon, shapes.shape_pt_lat),
        crs="EPSG:4326",
    ).to_crs(shared_utils.geography_utils.CA_NAD83Albers)
    most_recent_wed = (
        tbl.views.dim_date()
        >> filter(_.full_date < dt.datetime.now().date())
        >> filter(_.day_name == "Wednesday")
        >> filter(_.full_date == _.full_date.max())
        >> select(_.service_date == _.full_date)
    )
    wednesday = (
        tbl.views.gtfs_schedule_fact_daily_service()
        >> filter(_.calitp_itp_id == int(itp_id))
        >> inner_join(_, most_recent_wed, on="service_date")
        >> collect()
    )
    wednesday = wednesday >> select(_.calitp_itp_id, _.calitp_url_number, _.service_id)

    bus_routes = (
        tbl.gtfs_schedule.routes()
        >> filter(_.calitp_itp_id == int(itp_id))
        >> filter((_.route_type == "3") | (_.route_type == "11"))
        >> select(_.calitp_itp_id, _.calitp_url_number, _.route_id)
        >> collect()
    )
    print("loaded bus routes")

    trips = (
        tbl.gtfs_schedule.trips()
        >> filter(_.calitp_itp_id == int(itp_id))
        >> collect()
        >> inner_join(
            _, bus_routes, on=["calitp_itp_id", "calitp_url_number", "route_id"]
        )
        >> inner_join(
            _, wednesday, on=["calitp_itp_id", "calitp_url_number", "service_id"]
        )
    )
    print("loaded trips")
    stop_times = (
        tbl.gtfs_schedule.stop_times()
        >> filter(_.calitp_itp_id == int(itp_id))
        >> collect()
    )
    stop_times = (
        stop_times
        >> inner_join(_, trips, on=["calitp_itp_id", "calitp_url_number", "trip_id"])
        >> select(
            -_.stop_headsign,
            -_.pickup_type,
            -_.drop_off_type,
            -_.continuous_pickup,
            -_.continuous_drop_off,
            -_.shape_dist_travelled,
            -_.timepoint,
        )
    )
    print("loaded stop times")

    stops = (
        tbl.gtfs_schedule.stops()
        >> filter(_.calitp_itp_id == itp_id)
        >> select(_.stop_id, _.stop_lat, _.stop_lon)
        >> collect()
    )
    stops = gpd.GeoDataFrame(
        stops,
        geometry=gpd.points_from_xy(stops.stop_lon, stops.stop_lat),
        crs="EPSG:4326",
    ).to_crs(shared_utils.geography_utils.CA_NAD83Albers)
    print("loaded stops")

    return shapes, trips, stop_times, stops

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
    

def map_hqta(gdf, mouseover=None):
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
                               style={'color': 'black', 'fillColor': '#08589e',
                                            'opacity':0.4, 'weight':.5, 'dashArray':'2', 'fillOpacity':0.3},
                               hover_style={'fillColor': 'red' , 'fillOpacity': 0.2},
                               name = 'gdf')
        m.add_layer(geo_data_hq)
    
    if mouseover:
        geo_data_hq.on_hover(update_html)
        geo_data_hq.on_hover(add_to_nix)

    m.add_control(LayersControl())

    return m

def geoparquet_gcs_export(gdf, name):
    '''
    Save geodataframe as parquet locally, then move to GCS bucket and delete local file.
    '''
    gdf.to_parquet(f"{name}.parquet")
    fs.put(f"./{name}.parquet", f"{GCS_FILE_PATH}{name}.parquet")
    os.remove(f"./{name}.parquet")
    return