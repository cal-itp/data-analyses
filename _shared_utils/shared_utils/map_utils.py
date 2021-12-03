"""
Functions to create choropleth maps.
# https://stackoverflow.com/questions/64792041/white-gap-between-python-folium-map-and-jupyter-notebook-cell
# Folium examples: https://nbviewer.jupyter.org/github/python-visualization/folium/blob/master/examples/Colormaps.ipynb
# https://nbviewer.jupyter.org/github/python-visualization/folium/blob/master/examples/GeoJsonPopupAndTooltip.ipynb
# https://medium.com/analytics-vidhya/create-and-visualize-choropleth-map-with-folium-269d3fd12fa0
# https://stackoverflow.com/questions/47846744/create-an-asymmetric-colormap
"""
import altair
import folium
import ipyleaflet
import inspect
import json

from branca.element import Figure
from folium.features import GeoJsonPopup, GeoJsonTooltip
from ipyleaflet import Map, GeoData, LayersControl, basemaps, WidgetControl
from ipywidgets import link, FloatSlider, Text, HTML
from ipywidgets.embed import embed_minimal_html


# Centroids for various regions and zoom level
REGION_CENTROIDS = {
    "Alameda":  [[37.84, -122.27], 12],
    "Los Angeles": [[34.00, -118.18], 11],
    "CA": [[35.8, -119.4], 6],
}


def make_folium_choropleth_map(df, plot_col, 
                               popup_dict, tooltip_dict, colorscale, 
                               fig_width, fig_height, 
                               zoom=REGION_CENTROIDS["CA"][1], 
                               centroid = REGION_CENTROIDS["CA"][0], 
                               title="Chart Title", **kwargs,
                              ):
    '''
    Parameters:
    
    Pros/Cons: 
    folium can handle multiple columns displayed in popup/tooltip.
    ipyleaflet can only handle one.
    
    folium can export as HTML map; ipyleaflet can't.
    folium can't render in notebook currently; ipyleaflet can.
    '''
    
    # Pass more kwargs through various sub-functions
    # https://stackoverflow.com/questions/26534134/python-pass-different-kwargs-to-multiple-functions
    fig_args = [k for k, v in inspect.signature(Figure).parameters.items()]
    fig_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in fig_args}
    
    fig = Figure(width = fig_width, height = fig_height, **fig_dict)
    
    map_args = [k for k, v in inspect.signature(folium.Map).parameters.items()]
    map_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in map_args}
    
    m = folium.Map(location=centroid, tiles='cartodbpositron', 
               zoom_start=zoom, width=fig_width,height=fig_height, **map_dict)

    title_html = f'''
             <h3 align="center" style="font-size:20px"><b>{title}</b></h3>
             '''
    
    fig.get_root().html.add_child(folium.Element(title_html))
        
    TOOLTIP_KWARGS = {
        "min_width": 50,
            "max_width": 100,
        "font_size": "12px",
    }
    
    popup = GeoJsonPopup(
        fields=list(popup_dict.keys()),
        aliases=list(popup_dict.values()),
        localize=True,
        labels=True,
        style=f"background-color: light_gray;",
        min_width = TOOLTIP_KWARGS["min_width"],
        max_width = TOOLTIP_KWARGS["max_width"],
    )

    tooltip = GeoJsonTooltip(
        fields=list(tooltip_dict.keys()),
        aliases=list(tooltip_dict.values()),
        # localize = True sets the commas for numbers, but zipcode should be displayed as string
        #localize=True,
        sticky=False,
        labels=True,
        style=f"""
            background-color: "gray";
            border: 0px #FFFFFF;
            border-radius: 0px;
            box-shadow: 0px;
            font-size: {TOOLTIP_KWARGS["font_size"]};
        """,
        min_width=TOOLTIP_KWARGS["min_width"],
        max_width=TOOLTIP_KWARGS["max_width"],
    )

    #https://medium.com/analytics-vidhya/create-and-visualize-choropleth-map-with-folium-269d3fd12fa0
    geojson_args = [k for k, v in inspect.signature(folium.GeoJson).parameters.items()]
    geojson_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in geojson_args}
    
    g = folium.GeoJson(
        df,
        style_function=lambda x: {
            "fillColor": colorscale(x["properties"][plot_col])
            if x["properties"][plot_col] is not None
            else f"gray",
            "color": "#FFFFFF",
            "fillOpacity": 0.8,
            "weight": 0.2,
        },
        tooltip=tooltip,
        popup=popup,
        **geojson_dict
    ).add_to(m)
    
    colorscale.caption = "Legend"
    colorscale.add_to(m)
    
    fig.add_child(m)
    
    return fig


def make_ipyleaflet_choropleth_map(gdf, plot_col, geometry_col,
                                   choropleth_dict,
                                   colorscale, 
                                   zoom=REGION_CENTROIDS["CA"][1], 
                                   centroid = REGION_CENTROIDS["CA"][0], 
                                   **kwargs,
                                  ):
    '''
    Parameters:
    
    gdf: geopandas.GeoDataFrame
    plot_col: str, name of the column to map
    geometry_col: str, e.g., "Tract" or "District"
    
    choropleth_dict: dict. Takes this format:
        {
            "layer_name": string, human-readable layer name,
            "MIN_VALUE": numeric,
            "MAX_VALUE": numeric,
            "plot_col_name": string, human-readable column name,
            "fig_width": string, e.g., '100%'
            "fig_height": string, e.g., '100%'
            "fig_min_width_px": string, e.g., '600px'
            "fig_min_height_px": string, e.g., '600px'
        }
    '''
    
    # An error might come up if the geometry_col (Tract) is string in geo_data and numeric in choro_data
    # Coerce it to be string in both
    gdf = gdf.astype({geometry_col: str})
    geo_data = json.loads(gdf.set_index(geometry_col).to_json())
    
    # Take what we want to map and turn it into a dictionary
    # Can only include the key-value pair, the value you want to map, nothing more.
    choro_data = dict(zip(gdf[geometry_col].tolist(), gdf[plot_col].tolist()))
    
    map_args = [k for k, v in inspect.signature(ipyleaflet.Map).parameters.items()]
    map_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in map_args}
    
    m = ipyleaflet.Map(center = centroid, zoom = zoom,
                      basemap = basemaps.CartoDB.Positron, **map_dict)
    
    choro_args = [k for k, v in inspect.signature(ipyleaflet.Choropleth).parameters.items()]
    choro_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in choro_args}
    
    layer = ipyleaflet.Choropleth(
        geo_data = geo_data,
        choro_data = choro_data, 
        colormap = colorscale,
        border_color = '#999999',
        style = {'fillOpacity': 0.6, 'weight': 0.5, 'color': '#999999', 'opacity': 0.8},
        value_min = choropleth_dict["MIN_VALUE"],
        value_max = choropleth_dict["MAX_VALUE"],
        name = choropleth_dict["layer_name"],
        **choro_dict
    )
    
    
    html = HTML(''' 
        Hover over a tract
    ''')

    html.layout.margin = '0 px 10px 10px 10px'
    
        
    def on_hover(**kwargs):
        properties = kwargs.get("feature", {}).get("properties")
        id = kwargs.get("feature", {}).get("id")
        if not properties:
            return
        html.value=f"""
        <b>{geometry_col.title()}: </b>{id} <br>
        <b>{choropleth_dict['name']}: </b> {properties[plot_col]:,g} <br>
        """ 

    layer.on_hover(on_hover)
    
    m.add_layer(layer)
    
    widget_args = [k for k, v in inspect.signature(ipyleaflet.WidgetControl).parameters.items()]
    widget_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in widget_args}
    
    control = ipyleaflet.WidgetControl(widget = html, position = 'topright', **widget_dict)
    
    layers_args = [k for k, v in inspect.signature(ipyleaflet.LayersControl).parameters.items()]
    layers_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in layers_args}
    
    layers_control = ipyleaflet.LayersControl(position = 'topright', **layers_dict)

    m.add_control(control)
    m.add_control(layers_control)

    m.layout.height = choropleth_dict["fig_height"]
    m.layout.width = choropleth_dict["fig_width"]
    m.layout.min_height = choropleth_dict["fig_min_height_px"] 
    m.layout.min_width = choropleth_dict["fig_min_width_px"]
    
    return m
    