from ipyleaflet import Map, GeoJSON, projections, basemaps, GeoData, LayersControl, WidgetControl, GeoJSON, LegendControl
from ipywidgets import Text, HTML

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
                   style={'color': 'black', 'fillColor': '#2ca25f',
                                'opacity':0.2, 'weight':.5, 'dashArray':'2', 'fillOpacity':0.3},
                   hover_style={'fillColor': 'red' , 'fillOpacity': 0.3},
                   name = 'data')

    m.add_layer(geo_data)
        
    if mouseover:
        geo_data.on_hover(update_html)

    m.add_control(LayersControl())

    return m