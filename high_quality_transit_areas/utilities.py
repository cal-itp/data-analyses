import pandas as pd
import numpy as np
import geopandas as gpd
import fiona

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