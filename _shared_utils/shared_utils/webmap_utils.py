"""
Convienience functions for using web app maps via https://github.com/cal-itp/data-infra/tree/main/apps/maps.

These maps perform much better on large datasets, and include a download link. They support a generic style,
or certain predefined styles, for example for speedmaps and the state highway system.
"""
import pandas as pd
import geopandas as gpd
import gzip
import base64
import branca
from IPython.display import display, Markdown, IFrame
from calitp_data_analysis import get_fs, geography_utils
from typing import Literal
import json


fs = get_fs()

SPA_MAP_SITE = "https://embeddable-maps.calitp.org/"
SPA_MAP_BUCKET = "calitp-map-tiles/"

def spa_map_export_link(
    gdf: gpd.GeoDataFrame,
    path: str,
    state: dict,
    site: str = SPA_MAP_SITE,
    cache_seconds: int = 3600,
    verbose: bool = False,
) -> str:
    """
    Called via set_state_export. Handles stream writing of gzipped geojson to GCS bucket,
    encoding spa state as base64 and URL generation.
    """
    assert cache_seconds in range(3601), "cache must be 0-3600 seconds"
    geojson_str = gdf.to_json()
    geojson_bytes = geojson_str.encode("utf-8")
    if verbose:
        print(f"writing to {path}")
    with fs.open(path, "wb") as writer:  # write out to public-facing GCS?
        with gzip.GzipFile(fileobj=writer, mode="w") as gz:
            gz.write(geojson_bytes)
    if cache_seconds != 3600:
        fs.setxattrs(path, fixed_key_metadata={"cache_control": f"public, max-age={cache_seconds}"})
    base64state = base64.urlsafe_b64encode(json.dumps(state).encode()).decode()
    spa_map_url = f"{site}?state={base64state}"
    return spa_map_url


def set_state_export(
    gdf,
    bucket: str = SPA_MAP_BUCKET,
    subfolder: str = "testing/",
    filename: str = "test2",
    map_type: Literal[
        "speedmap",
        "speed_variation",
        "new_speedmap",
        "new_speed_variation",
        "hqta_areas",
        "hqta_stops",
        "state_highway_network",
    ] = None,
    map_title: str = "Map",
    cmap: branca.colormap.ColorMap = None,
    color_col: str = None,
    legend_url: str = None,
    existing_state: dict = {},
    cache_seconds: int = 3600,
    manual_centroid: list = None,
) -> dict:
    """
    Applies light formatting to gdf for successful spa display. Will pass map_type
    if supported by the spa and provided. GCS bucket is preset to the publically
    available one.
    Supply cmap and color_col for coloring based on a Branca ColorMap and a column
    to apply the color to.
    Cache is 1 hour by default, can set shorter time in seconds for
    "near realtime" applications (suggest 120) or development (suggest 0)

    Returns dict with state dictionary and map URL. Can call multiple times and supply
    previous state as existing_state to create multilayered maps.
    """
    assert not gdf.empty, "geodataframe is empty!"
    spa_map_state = existing_state or {"name": "null", "layers": [], "lat_lon": (), "zoom": 13}
    path = f"{bucket}{subfolder}{filename}.geojson.gz"
    gdf = gdf.to_crs(geography_utils.WGS84)
    if cmap and color_col:
        gdf["color"] = gdf[color_col].apply(lambda x: cmap.rgb_bytes_tuple(x))
    gdf = gdf.round(2)  # round for map display
    this_layer = [
        {
            "name": f"{map_title}",
            "url": f"https://storage.googleapis.com/{path}",
            "properties": {"stroked": False, "highlight_saturation_multiplier": 0.5},
        }
    ]
    if map_type:
        this_layer[0]["type"] = map_type
    if map_type in ["new_speedmap", "speedmap"]:
        this_layer[0]["properties"]["tooltip_speed_key"] = "p20_mph"
    spa_map_state["layers"] += this_layer
    if manual_centroid:
        centroid = manual_centroid
    else:
        centroid = (gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean())
    spa_map_state["lat_lon"] = centroid
    if legend_url:
        spa_map_state["legend_url"] = legend_url
        
    return {
        "state_dict": spa_map_state,
        "spa_link": spa_map_export_link(gdf=gdf, path=path, state=spa_map_state, cache_seconds=cache_seconds),
    }


def render_spa_link(spa_map_url: str, text='Full Map') -> None:
    
    display(Markdown(f'<a href="{spa_map_url}" target="_blank">Open {text} in New Tab</a>'))
    return


def display_spa_map(spa_map_url: str, width: int=1000, height: int=650) -> None:
    '''
    Display map from external simple web app in the notebook/JupyterBook context via an IFrame.
    Width/height defaults are current best option for JupyterBook, don't change for portfolio use
    width, height: int (pixels)
    '''
    i = IFrame(spa_map_url, width=width, height=height)
    display(i)
    return
