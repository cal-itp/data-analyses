import datetime as dt

import calitp
import fiona
import gcsfs
import geopandas as gpd
import intake
import numpy as np
import pandas as pd
import os
import shapely

from calitp.tables import tbl
from ipyleaflet import GeoData, GeoJSON, LayersControl, Map, WidgetControl, basemaps, projections
from ipywidgets import HTML, Text
from shapely.geometry import LineString, MultiPoint
from shapely.ops import split, substring
from shared_utils import calitp_color_palette, geography_utils
from siuba import *

fs = gcsfs.GCSFileSystem()

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/high_quality_transit_areas"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

catalog = intake.open_catalog("./*.yml")

# Colors
BLUE = "#08589e"
ORANGE = "#fec44f"
ITP_BLUE = calitp_color_palette.CALITP_CATEGORY_BOLD_COLORS[0]


def create_segments(geometry):
    """Splits a Shapely LineString into smaller LineStrings. If a MultiLineString passed,
    splits each LineString in that collection.
    """

    lines = []
    segment_distance_meters = 1_250
    geometry = geometry.iloc[0]
    if hasattr(geometry, "geoms"):  ##check if MultiLineString
        linestrings = geometry.geoms
    else:
        linestrings = [geometry]
    for linestring in linestrings:
        for i in range(0, int(linestring.length), segment_distance_meters):
            lines.append(substring(linestring, i, i + segment_distance_meters))
    return lines


def map_hqta(gdf, mouseover=None, name="gdf"):
    global nix_list
    nix_list = []

    if "calitp_extracted_at" in gdf.columns:
        gdf = gdf.drop(columns="calitp_extracted_at")
    gdf = gdf.to_crs("EPSG:6414")  ## https://epsg.io/6414 (meters)
    if gdf.geometry.iloc[0].geom_type == "Point":
        gdf.geometry = gdf.geometry.buffer(200)

    x = gdf.to_crs(geography_utils.WGS84).geometry.iloc[0].centroid.x
    y = gdf.to_crs(geography_utils.WGS84).geometry.iloc[0].centroid.y

    m = Map(basemap=basemaps.CartoDB.Positron, center=[y, x], zoom=11)

    if mouseover:
        html = HTML(f"hover to see {mouseover}")
        html.layout.margin = "0px 20px 20px 20px"
        control = WidgetControl(widget=html, position="topright")
        m.add_control(control)

        def update_html(feature, **kwargs):
            html.value = """
                <h3><b>{}</b></h3>
            """.format(
                feature["properties"][mouseover]
            )

        def add_to_nix(feature, **kwargs):
            nix_list.append(feature["properties"][mouseover])

    LAYER_STYLE = {
        "color": "black",
        "opacity": 0.4,
        "weight": 0.5,
        "dashArray": "2",
        "fillOpacity": 0.3,
    }
    HOVER_STYLE = {"fillColor": "red", "fillOpacity": 0.2}

    if "hq_transit_corr" in gdf.columns:
        geo_data_hq = GeoData(
            geo_dataframe=(gdf[gdf["hq_transit_corr"]].to_crs(geography_utils.WGS84)),
            style={**{"fillColor": BLUE}, **LAYER_STYLE},
            hover_style={**HOVER_STYLE},
            name="HQTA",
        )
        geo_data_not_hq = GeoData(
            geo_dataframe=(gdf[~gdf["hq_transit_corr"]].to_crs(geography_utils.WGS84)),
            style={**{"fillColor": ORANGE}, **LAYER_STYLE},
            hover_style={**HOVER_STYLE},
            name="non-HQTA",
        )

        m.add_layer(geo_data_hq)
        m.add_layer(geo_data_not_hq)

    else:

        geo_data_hq = GeoData(
            geo_dataframe=gdf.to_crs(geography_utils.WGS84),
            style={**{"fillColor": ITP_BLUE}, **LAYER_STYLE},
            hover_style={**HOVER_STYLE},
            name=name,
        )
        m.add_layer(geo_data_hq)

    if mouseover:
        geo_data_hq.on_hover(update_html)
        geo_data_hq.on_hover(add_to_nix)

    m.add_control(LayersControl())

    return m


# Fill out HQTA details of why nulls are present
# based on feedback from open data publishing process
# Concise df can be confusing to users if they don't know how to interpret presence of nulls
# and which cases of HQTA definitions those correspond to
def hqta_details(row):
    if row.hqta_type == "major_stop_bus":
        if row.calitp_itp_id_primary != int(row.calitp_itp_id_secondary):
            return "intersection_2_bus_routes_different_operators"
        else:
            return "intersection_2_bus_routes_same_operator"
    elif row.hqta_type == "hq_corridor_bus":
        return "stop_along_hq_bus_corridor_single_operator"
    elif row.hqta_type in ["major_stop_ferry", "major_stop_brt", "major_stop_rail"]:
        # (not sure if ferry, brt, rail, primary/secondary ids are filled in.)
        return row.hqta_type + "_single_operator"
    

def catalog_filepath(file):
    return catalog[file].urlpath