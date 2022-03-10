"""
Functions to create choropleth maps.
# https://stackoverflow.com/questions/64792041/white-gap-between-python-folium-map-and-jupyter-notebook-cell
# Folium examples: https://nbviewer.jupyter.org/github/python-visualization/folium/blob/master/examples/Colormaps.ipynb
# https://nbviewer.jupyter.org/github/python-visualization/folium/blob/master/examples/GeoJsonPopupAndTooltip.ipynb
# https://medium.com/analytics-vidhya/create-and-visualize-choropleth-map-with-folium-269d3fd12fa0
# https://stackoverflow.com/questions/47846744/create-an-asymmetric-colormap
"""
import inspect
import json

import folium
import ipyleaflet
import pandas as pd
from branca.element import Figure
from folium.features import GeoJsonPopup, GeoJsonTooltip
from folium.plugins import FloatImage
from ipyleaflet import basemaps
from ipywidgets import HTML


# Centroids for various regions and zoom level
def grab_region_centroids():
    # This parquet is created in shared_utils/shared_data.py
    df = pd.read_parquet(
        "gs://calitp-analytics-data/data-analyses/ca_county_centroids.parquet"
    )

    df = df.assign(centroid=df.centroid.apply(lambda x: x.tolist()))

    # Manipulate parquet file to be dictionary to use in map_utils
    region_centroids = dict(
        zip(df.county_name, df[["centroid", "zoom"]].to_dict(orient="records"))
    )

    return region_centroids


REGION_CENTROIDS = grab_region_centroids()

# ------------------------------------------------------------------------#
# Folium
# ------------------------------------------------------------------------#
# Move popup and tooltip functions out, since styling is similar
# Pass in make_folium_choropleth_map() and make_folium_multiple_layers_map()
TOOLTIP_KWARGS = {
    "min_width": 50,
    "max_width": 100,
    "font_size": "12px",
}


def format_folium_popup(popup_dict):
    popup = GeoJsonPopup(
        fields=list(popup_dict.keys()),
        aliases=list(popup_dict.values()),
        # localize=True,
        labels=True,
        style="background-color: light_gray;",
        min_width=TOOLTIP_KWARGS["min_width"],
        max_width=TOOLTIP_KWARGS["max_width"],
    )
    return popup


def format_folium_tooltip(tooltip_dict):
    tooltip = GeoJsonTooltip(
        fields=list(tooltip_dict.keys()),
        aliases=list(tooltip_dict.values()),
        # localize = True sets the commas for numbers, but zipcode should be displayed as string
        # localize=True,
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
    return tooltip


def make_folium_choropleth_map(
    df,
    plot_col,
    popup_dict,
    tooltip_dict,
    colorscale,
    fig_width,
    fig_height,
    zoom=REGION_CENTROIDS["CA"]["zoom"],
    centroid=REGION_CENTROIDS["CA"]["centroid"],
    title="Chart Title",
    legend_name="Legend",
    **kwargs,
):
    """
    Parameters:

    df: geopandas.GeoDataFrame
    plot_col: str, name of the column to map
    popup_dict: dict
                key: column name; value: display name in popup
    tooltip_dict: dict
                key: column name; value: display name in tooltip
    colorscale: branca.colormap element
    fig_width: int. Ex: 500
    fig_height: int. Ex: 500
    zoom: int.
    centroid: list, of the format [latitude, longitude]
    title: str.
    legend_name: str
    **kwargs: any other keyword arguments that can passed into existing folium functions
            that are used in this function

    Pros/Cons:
    folium can handle multiple columns displayed in popup/tooltip.
    ipyleaflet can only handle one.

    folium can export as HTML map; ipyleaflet can't.
    folium can't render in notebook currently; ipyleaflet can.
    """

    # Pass more kwargs through various sub-functions
    # https://stackoverflow.com/questions/26534134/python-pass-different-kwargs-to-multiple-functions
    fig_args = [k for k, v in inspect.signature(Figure).parameters.items()]
    fig_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in fig_args}

    fig = Figure(width=fig_width, height=fig_height, **fig_dict)

    map_args = [k for k, v in inspect.signature(folium.Map).parameters.items()]
    map_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in map_args}

    m = folium.Map(
        location=centroid,
        tiles="cartodbpositron",
        zoom_start=zoom,
        width=fig_width,
        height=fig_height,
        **map_dict,
    )

    title_html = f"""
             <h3 align="center" style="font-size:20px"><b>{title}</b></h3>
             """

    fig.get_root().html.add_child(folium.Element(title_html))

    popup = format_folium_popup(popup_dict)
    tooltip = format_folium_tooltip(tooltip_dict)

    # https://medium.com/analytics-vidhya/create-and-visualize-choropleth-map-with-folium-269d3fd12fa0
    geojson_args = [k for k, v in inspect.signature(folium.GeoJson).parameters.items()]
    geojson_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in geojson_args}

    folium.GeoJson(
        df,
        style_function=lambda x: {
            "fillColor": colorscale(x["properties"][plot_col])
            if x["properties"][plot_col] is not None
            else "gray",
            "color": "#FFFFFF",
            "fillOpacity": 0.8,
            "weight": 0.2,
        },
        tooltip=tooltip,
        popup=popup,
        name={legend_name},
        **geojson_dict,
    ).add_to(m)

    colorscale.caption = f"{legend_name}"
    colorscale.add_to(m)

    fig.add_child(m)

    return fig


# Adjust function to have multiple layers in folium
# Modify the original function...but generalize the unpacking of the layer portion
# Keep original function the same, don't break other ppl's work
def make_folium_multiple_layers_map(
    LAYERS_DICT,
    fig_width,
    fig_height,
    zoom=REGION_CENTROIDS["CA"]["zoom"],
    centroid=REGION_CENTROIDS["CA"]["centroid"],
    title="Chart Title",
    legend_dict={"legend_url": "", "legend_bottom": 85, "legend_left": 5},
    **kwargs,
):
    """
    Parameters:
    LAYERS_DICT: dict. Can contain as many other polygon layers as needed.

        Must contain the following key-value pairs
        key: name of the layer
        value: dict with relevant info for plotting the layer
                df, plot_col, popup_dict, tooltip_dict, colorscale

        LAYERS_DICT = {
            "layer_name1": {"df": geopandas.GeoDataFrame,
                "plot_col": str,
                "popup_dict": dict,
                "tooltip_dict": dict,
                "colorscale": branca.colormap element
            },
            "layer_name2": {"df": geopandas.GeoDataFrame,
                "plot_col": str,
                "popup_dict": dict,
                "tooltip_dict": dict,
                "colorscale": branca.colormap element
            },
        }

    fig_width: int. Ex: 500
    fig_height: int. Ex: 500
    zoom: int.
    centroid: list, of the format [latitude, longitude]
    title: str.
    legend_dict: dict

        legend_dict = {
            "legend_url": str
                        GitHub url to the image of the legend manually created
                        Ex:  ('https://raw.githubusercontent.com/cal-itp/data-analyses/'
                                'more-highways/bus_service_increase/'
                                'img/legend_intersecting_parallel.png'
                        )

            "legend_bottom": int
                            value between 0-100, relative to the bottom edge of figure
            "legend_left": int
                            value between 0-100, relative to the left edge of figure
        }
    **kwargs: any other keyword arguments that can passed into existing folium functions
            that are used in this function
    """

    # Pass more kwargs through various sub-functions
    # https://stackoverflow.com/questions/26534134/python-pass-different-kwargs-to-multiple-functions
    fig_args = [k for k, v in inspect.signature(Figure).parameters.items()]
    fig_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in fig_args}

    fig = Figure(width=fig_width, height=fig_height, **fig_dict)

    map_args = [k for k, v in inspect.signature(folium.Map).parameters.items()]
    map_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in map_args}

    m = folium.Map(
        location=centroid,
        tiles="cartodbpositron",
        zoom_start=zoom,
        width=fig_width,
        height=fig_height,
        **map_dict,
    )

    title_html = f"""
         <h3 align="center" style="font-size:20px"><b>{title}</b></h3>
         """

    fig.get_root().html.add_child(folium.Element(title_html))

    # Define function that can theoretically pop out as many polygon layers as needed
    def get_layer(
        df, plot_col, popup_dict, tooltip_dict, colorscale, layer_name, **kwargs
    ):

        popup = format_folium_popup(popup_dict)
        tooltip = format_folium_tooltip(tooltip_dict)

        # https://medium.com/analytics-vidhya/create-and-visualize-choropleth-map-with-folium-269d3fd12fa0
        geojson_args = [
            k for k, v in inspect.signature(folium.GeoJson).parameters.items()
        ]
        geojson_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in geojson_args}

        g = folium.GeoJson(
            df,
            style_function=lambda x: {
                "fillColor": colorscale(x["properties"][plot_col])
                if x["properties"][plot_col] is not None
                else "gray",
                "color": "#FFFFFF",
                "fillOpacity": 0.8,
                "weight": 0.2,
            },
            tooltip=tooltip,
            popup=popup,
            name=layer_name,
            **geojson_dict,
        )

        return g

    # Now, loop through the keys in the LAYERS_DICT,
    # Unpack the dictionary associated with each layer,
    # Then attach that layer to the Map element
    for key, nested_dict in LAYERS_DICT.items():
        # key: layer name or layer number
        # value: dictionary of all the components associated with folium layer
        d = nested_dict
        layer = get_layer(
            df=d["df"],
            plot_col=d["plot_col"],
            popup_dict=d["popup_dict"],
            tooltip_dict=d["tooltip_dict"],
            colorscale=d["colorscale"],
            layer_name=key,
        )
        layer.add_to(m)

    # Legend doesn't show up with multiple layers
    # One way around, create the colorscale(s) as one image and save it
    # Then, insert that legend as a URL to be an image
    # I think legend_bottom and legend_left numbers must be 0-100?
    # Going even 95 pushes it to the top edge of the figure
    FloatImage(
        legend_dict["legend_url"],
        legend_dict["legend_bottom"],
        legend_dict["legend_left"],
    ).add_to(m)

    folium.LayerControl("topright", collapsed=False).add_to(m)

    # Now, attach everything to Figure
    fig.add_child(m)

    return fig


# ------------------------------------------------------------------------#
# ipyleaflet
# ------------------------------------------------------------------------#
def make_ipyleaflet_choropleth_map(
    gdf,
    plot_col,
    geometry_col,
    choropleth_dict,
    colorscale,
    zoom=REGION_CENTROIDS["CA"]["zoom"],
    centroid=REGION_CENTROIDS["CA"]["centroid"],
    **kwargs,
):
    """
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
    """

    # An error might come up if the geometry_col (Tract) is string in geo_data and numeric in choro_data
    # Coerce it to be string in both
    gdf = gdf.astype({geometry_col: str})
    geo_data = json.loads(gdf.set_index(geometry_col).to_json())

    # Take what we want to map and turn it into a dictionary
    # Can only include the key-value pair, the value you want to map, nothing more.
    choro_data = dict(zip(gdf[geometry_col].tolist(), gdf[plot_col].tolist()))

    map_args = [k for k, v in inspect.signature(ipyleaflet.Map).parameters.items()]
    map_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in map_args}

    m = ipyleaflet.Map(
        center=centroid, zoom=zoom, basemap=basemaps.CartoDB.Positron, **map_dict
    )

    choro_args = [
        k for k, v in inspect.signature(ipyleaflet.Choropleth).parameters.items()
    ]
    choro_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in choro_args}

    layer = ipyleaflet.Choropleth(
        geo_data=geo_data,
        choro_data=choro_data,
        colormap=colorscale,
        border_color="#999999",
        style={"fillOpacity": 0.6, "weight": 0.5, "color": "#999999", "opacity": 0.8},
        value_min=choropleth_dict["MIN_VALUE"],
        value_max=choropleth_dict["MAX_VALUE"],
        name=choropleth_dict["layer_name"],
        **choro_dict,
    )

    html = HTML(
        """
        Hover over a tract
    """
    )

    html.layout.margin = "0 px 10px 10px 10px"

    def on_hover(**kwargs):
        properties = kwargs.get("feature", {}).get("properties")
        id = kwargs.get("feature", {}).get("id")
        if not properties:
            return
        html.value = f"""
        <b>{geometry_col.title()}: </b>{id} <br>
        <b>{choropleth_dict['name']}: </b> {properties[plot_col]:,g} <br>
        """

    layer.on_hover(on_hover)

    m.add_layer(layer)

    widget_args = [
        k for k, v in inspect.signature(ipyleaflet.WidgetControl).parameters.items()
    ]
    widget_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in widget_args}

    control = ipyleaflet.WidgetControl(widget=html, position="topright", **widget_dict)

    layers_args = [
        k for k, v in inspect.signature(ipyleaflet.LayersControl).parameters.items()
    ]
    layers_dict = {k: kwargs.pop(k) for k in dict(kwargs) if k in layers_args}

    layers_control = ipyleaflet.LayersControl(position="topright", **layers_dict)

    m.add_control(control)
    m.add_control(layers_control)

    m.layout.height = choropleth_dict["fig_height"]
    m.layout.width = choropleth_dict["fig_width"]
    m.layout.min_height = choropleth_dict["fig_min_height_px"]
    m.layout.min_width = choropleth_dict["fig_min_width_px"]

    return m
