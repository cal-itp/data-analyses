from ipyleaflet import Map, GeoJSON, projections, basemaps, GeoData, LayersControl, WidgetControl, GeoJSON, LegendControl
from ipywidgets import Text, HTML

import gcsfs
import shapely

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/rt_delay"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

MPH_PER_MPS = 2.237 ## use to convert meters/second to miles/hour

def simple_map(gdf, mouseover=None):
    
    gdf = gdf.copy()
    
    if type(gdf.geometry.iloc[0]) == shapely.geometry.point.Point:
        gdf.geometry = gdf.buffer(50)
    elif type(gdf.geometry.iloc[0]) == shapely.geometry.linestring.LineString:
        gdf.geometry = gdf.buffer(50)
    
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
                   style={'color': 'black', 'fillColor': '#2ca25f',
                                'opacity':0.2, 'weight':.5, 'dashArray':'2', 'fillOpacity':0.3},
                   hover_style={'fillColor': 'red' , 'fillOpacity': 0.3},
                   name = 'data')

    m.add_layer(geo_data)
        
    if mouseover:
        geo_data.on_hover(update_html)

    m.add_control(LayersControl())

    return m