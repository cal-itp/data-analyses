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
                               title="Chart Title",
                              ):
    '''
    Parameters:
    
    Pros/Cons: 
    folium can handle multiple columns displayed in popup/tooltip.
    ipyleaflet can only handle one.
    
    folium can export as HTML map; ipyleaflet can't.
    folium can't render in notebook currently; ipyleaflet can.
    '''
    
    fig = Figure(width = fig_width, height = fig_height)
    
    m = folium.Map(location=centroid, tiles='cartodbpositron', 
               zoom_start=zoom, width=fig_width,height=fig_height)

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
    ).add_to(m)
    
    colorscale.caption = "Legend"
    colorscale.add_to(m)
    
    fig.add_child(m)
    
    return fig


def make_ipyleaflet_choropleth_map(choropleth_dict, plot_col, 
                               colorscale, 
                               zoom=REGION_CENTROIDS["CA"][1], 
                               centroid = REGION_CENTROIDS["CA"][0], 
                                  ):
    
    '''
    Parameters:
    
    choropleth_dict: dict. Takes this format:
        {
            "geo_data": geo_data,
            "choro_data": choro_data,
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
    m = ipyleaflet.Map(center = centroid, zoom = zoom,
                      basemap = basemaps.CartoDB.Positron)
    
    layer = ipyleaflet.Choropleth(
        geo_data = choropleth_dict["geo_data"],
        choro_data = choropleth_dict["choro_data"], 
        colormap = colorscale,
        border_color = '#999999',
        style = {'fillOpacity': 0.6, 'weight': 0.5, 'color': '#999999', 'opacity': 0.8},
        value_min = choropleth_dict["MIN_VALUE"],
        value_max = choropleth_dict["MAX_VALUE"],
        name = choropleth_dict["layer_name"]
    )

    """
    html = HTML(''' 
        Hover over a tract
    ''')

    html.layout.margin = '0 px 10px 10px 10px'
    """
    
    '''
    def on_hover(**kwargs):
        properties = kwargs.get("feature", {}).get("properties")
        id = kwargs.get("feature", {}).get("id")
        if not properties:
            return
        html.value=f"""
        <b>Tract: </b>{id} <br>
        <b>Daily Arrivals per 1k: </b> {properties[plot_col]:,g} <br>
        """ 

    layer.on_hover(on_hover)
    '''
    m.add_layer(layer)
    
    control = ipyleaflet.WidgetControl(widget = html, position = 'topright')
    layers_control = ipyleaflet.LayersControl(position = 'topright')

    m.add_control(control)
    m.add_control(layers_control)

    m.layout.height = choropleth_dict["fig_height"]
    m.layout.width = choropleth_dict["fig_width"]
    m.layout.min_height = choropleth_dict["fig_min_height_px"] 
    m.layout.min_width = choropleth_dict["fig_min_width_px"]

    return m