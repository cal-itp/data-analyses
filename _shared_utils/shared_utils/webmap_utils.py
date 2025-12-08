"""
Convienience functions for using web app maps via https://github.com/cal-itp/data-infra/tree/main/apps/maps.

These maps perform much better on large datasets, and include a download link. They support a generic style,
or certain predefined styles, for example for speedmaps and the state highway system.
"""

import base64
import gzip
import json

import branca
import geopandas as gpd
from calitp_data_analysis import geography_utils, get_fs
from IPython.display import IFrame, Markdown, display

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
    overwrite: bool = False,
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
    if fs.exists(path) and not overwrite:
        print(f"file already exists at {path}, not overwriting, use overwrite = True to overwrite")
    else:
        with fs.open(path, "wb") as writer:  # write out to public-facing GCS?
            with gzip.GzipFile(fileobj=writer, mode="w") as gz:
                gz.write(geojson_bytes)
    if cache_seconds != 3600:
        fs.setxattrs(path, fixed_key_metadata={"cache_control": f"public, max-age={cache_seconds}"})
    base64state = base64.urlsafe_b64encode(json.dumps(state).encode()).decode()
    spa_map_url = f"{site}?state={base64state}"
    return spa_map_url


SPA_MAP_TYPES = [
    "speedmap",
    "speed_variation",
    "new_speedmap",
    "new_speed_variation",
    "hqta_areas",
    "hqta_stops",
    "state_highway_network",
    None,
]


def set_state_export(
    gdf,
    bucket: str = SPA_MAP_BUCKET,
    subfolder: str = "testing/",
    filename: str = "test2",
    map_type=None,
    map_title: str = "Map",
    cmap: branca.colormap.ColorMap = None,
    color_col: str = None,
    legend_url: str = None,
    existing_state: dict = {},
    cache_seconds: int = 3600,
    manual_centroid: list = None,
    overwrite: bool = False,
) -> dict:
    """
    Main function to use single page application webmap.

    Applies light formatting to gdf for successful spa display. Will pass map_type
    if supported by the spa and provided, otherwise leave as None for generic style.
    GCS bucket is preset to a publically available one.
    Supply cmap and color_col for coloring based on a Branca ColorMap and a column
    to apply the color to.
    Cache is 1 hour by default, can set shorter time in seconds for
    "near realtime" applications (suggest 120) or development (suggest 0)

    Returns dict with state dictionary and map URL. Can call multiple times and supply
    previous state as existing_state to create multilayered maps.
    """
    assert (
        map_type in SPA_MAP_TYPES
    ), "map_type must be a supported type from data-infra or None (update list in webmap_utils if applicable)"
    assert not gdf.empty, "geodataframe is empty!"
    if existing_state and "state_dict" in existing_state.keys():
        existing_state = existing_state["state_dict"]
    spa_map_state = existing_state or {"name": "null", "layers": [], "lat_lon": (), "zoom": 13}
    path = f"{bucket}{subfolder}{filename}.geojson.gz"
    gdf = gdf.to_crs(geography_utils.WGS84)
    if cmap and color_col:
        gdf["color"] = gdf[color_col].apply(lambda x: cmap.rgb_bytes_tuple(x))
    elif "color" not in gdf.columns:
        gdf = gdf.assign(color=[(155, 155, 155)] * gdf.shape[0])
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
    layer_names = [layer["name"] for layer in spa_map_state["layers"]]
    assert len(layer_names) == len(set(layer_names)), "Layer map_title must be unique!"
    if manual_centroid:
        centroid = manual_centroid
    else:
        centroid = (gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean())
    spa_map_state["lat_lon"] = centroid
    if legend_url:
        spa_map_state["legend_url"] = legend_url

    return {
        "state_dict": spa_map_state,
        "spa_link": spa_map_export_link(
            gdf=gdf, path=path, state=spa_map_state, cache_seconds=cache_seconds, overwrite=overwrite
        ),
    }


def render_spa_link(spa_map_url: str, text="Full Map") -> None:
    """
    Call within a notebook to render a link to your webmap.
    """
    display(Markdown(f'<a href="{spa_map_url}" target="_blank">Open {text} in New Tab</a>'))
    return


def display_spa_map(spa_map_url: str, width: int = 1000, height: int = 650) -> None:
    """
    Display map from external simple web app in the notebook/JupyterBook context via an IFrame.
    Width/height defaults are current best option for JupyterBook, don't change for portfolio use
    width, height: int (pixels)
    """
    i = IFrame(spa_map_url, width=width, height=height)
    display(i)
    return


def add_lines_header(svg):
    """
    reformat branca cmap export for successful display
    """
    # add newlines
    svg = svg.replace("<", "\n<")

    # svg from _repr_html_ missing this apperently essential header
    svg_header = """<?xml version="1.0" standalone="no"?>
    <!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 20010904//EN"
     "http://www.w3.org/TR/2001/REC-SVG-20010904/DTD/svg10.dtd">
     <svg version="1.0" xmlns="http://www.w3.org/2000/svg" height="60" width="500"
     viewBox ="0 0 500 60">
    """
    # strip first svg tag from original string (already in header)
    svg_strip_tag = svg.split('width="500">\n')[1]
    export_svg = svg_header + svg_strip_tag
    return export_svg


def add_inner_labels_caption(svg, labels, spacing, caption):
    """
    reformat branca cmap export for successful display
    """
    labels = [round(n, 1) for n in list(labels)]
    inner_labels_str = "".join(
        [
            f'<text x="{spacing + 100 * labels.index(label)}" y="35" style="text-anchor:end;">{label}\n</text>'
            for label in labels
        ]
    )

    inner_labels_str += f'<text x="0" y="50">{caption}\n</text>'
    inner_labels_str += "\n</svg>"
    export_svg = svg.replace("\n</svg>", inner_labels_str)

    return export_svg


def export_legend(cmap: branca.colormap.StepColormap, filename: str, inner_labels: list = []):
    """
    Given a branca colormap, export its html and reformat for successful display in webmap.

    inner_labels is optional, but if provided should be four labels for correct spacing.
    """
    assert len(inner_labels) in [
        0,
        4,
    ], "currently must supply 4 or 0 inner labels for spacing, outer labels are provided by default"
    legend = add_lines_header(cmap._repr_html_())
    legend = add_inner_labels_caption(legend, inner_labels, 100, cmap.caption)

    path = f"calitp-map-tiles/{filename}"
    with fs.open(path, "w") as writer:  # write out to public-facing GCS?
        writer.write(legend)
    print(f"legend written to {path}, public_url https://storage.googleapis.com/{path}")


def categorical_cmap(cmap: branca.colormap, categories: list) -> dict:
    """
    Given a branca colormap and a list of categorical values, map unique values to each color
    """
    categories = list(set(categories))
    cmap = cmap.scale(vmin=0, vmax=len(cmap.colors))
    cmap_colors_rgb = [cmap.rgb_bytes_tuple(x) for x in range(1, len(cmap.colors) + 1)]
    n_categories = len(categories)
    n_colors = len(cmap_colors_rgb)
    if n_categories > n_colors:
        print(f"{n_categories} categories exceed {n_colors} colors, colors will be duplicated")
        cmap_multiplier = (n_categories // n_colors) + 1
        cmap_colors_rgb = cmap_colors_rgb * cmap_multiplier

    categorical_cmap_dict = dict(zip(categories, cmap_colors_rgb))

    return categorical_cmap_dict
