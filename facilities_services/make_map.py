from shared_utils.map_utils import TOOLTIP_KWARGS, format_folium_popup, format_folium_tooltip, REGION_CENTROIDS

import branca
import folium
import inspect

from branca.element import Figure
from folium.features import GeoJsonPopup, GeoJsonTooltip
from folium.plugins import FloatImage


def setup_folium_figure(fig_width, fig_height, title, **fig_dict):
    fig = Figure(fig_width, fig_height, **fig_dict)
    title_html = f"""
         <h3 align="center" style="font-size:20px"><b>{title}</b></h3>
         """

    fig.get_root().html.add_child(folium.Element(title_html))
    
    return fig


def setup_folium_map(fig_width, fig_height, centroid, zoom, **map_dict):
    m = folium.Map(
        location=centroid,
        tiles="cartodbpositron",
        zoom_start=zoom,
        width=fig_width,
        height=fig_height,
        **map_dict,
    )
    
    return m


def make_geojson_layer(single_layer_dict, layer_name):
    # Grab keys from the layer dictionary
    plot_col = single_layer_dict["plot_col"]
    colorscale = single_layer_dict["colorscale"]
    popup = format_folium_popup(single_layer_dict["popup_dict"])
    tooltip = format_folium_tooltip(single_layer_dict["tooltip_dict"])
    
    # Allow for styling to overwrite default
    # If style_function is present, use that, otherwise, use this default for choropleth
    default_style_function = lambda x: {
        "fillColor": colorscale(x["properties"][plot_col])
            if x["properties"][plot_col] is not None
            else "gray",
            "color": "#FFFFFF",
            "fillOpacity": 0.8,
            "weight": 0.2,
    }
    
    selected_style_function = single_layer_dict.get("style_function", default_style_function)
    
    
    geojson_args = [k for k, v in inspect.signature(folium.GeoJson).parameters.items()]
    geojson_args.remove("style_function")
    geojson_dict = {k: single_layer_dict.pop(k) for k in dict(single_layer_dict) 
                    if k in geojson_args} 
    
    g = folium.GeoJson(
        single_layer_dict["df"],
        style_function=selected_style_function,
        tooltip=tooltip,
        popup=popup,
        name=layer_name,
        **geojson_dict,
    )
    
    return g

def setup_folium_legend(legend_dict):
    legend = FloatImage(
        legend_dict["legend_url"],
        legend_dict["legend_bottom"],
        legend_dict["legend_left"],
    )
    
    return legend


def make_folium_multiple_layers_map(
    LAYERS_DICT,
    fig_width = 300,
    fig_height = 300,
    zoom=REGION_CENTROIDS["CA"]["zoom"],
    centroid=REGION_CENTROIDS["CA"]["centroid"],
    title="Chart Title",
    legend_dict={"legend_url": "", "legend_bottom": 85, "legend_left": 5},
    **kwargs,
):
 

    # Pass more kwargs through various sub-functions
    # https://stackoverflow.com/questions/26534134/python-pass-different-kwargs-to-multiple-functions
    fig_args = [k for k, v in inspect.signature(Figure).parameters.items()]
    fig_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in fig_args}

    fig = setup_folium_figure(fig_width, fig_height, title, **fig_dict)
                             
    map_args = [k for k, v in inspect.signature(folium.Map).parameters.items()]
    map_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in map_args}

    m = setup_folium_map(fig_width, fig_height, centroid, zoom, **map_dict)

    for layer_name, layer_dict in LAYERS_DICT.items():
        g = make_geojson_layer(layer_dict, layer_name)
        g.add_to(m)

        
    # Legend doesn't show up with multiple layers
    # One way around, create the colorscale(s) as one image and save it
    # Then, insert that legend as a URL to be an image
    # I think legend_bottom and legend_left numbers must be 0-100?
    # Going even 95 pushes it to the top edge of the figure
    legend = setup_folium_legend(legend_dict)
    legend.add_to(m)
    
    folium.LayerControl("topright", collapsed=False).add_to(m)

    fig.add_child(m)

    return fig

