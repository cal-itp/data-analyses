"""
# Plot vp path for stops that have low percent of vp trips

1. `fct_vp_stop_metrics`

* no configs yet, it's a table for now with no partitioning or clustering
* partitioned by
* clustered by
* grain: `service_date-feed_key-stop_id`

Daily counts for a of how many `n_(vp)_trips` and `n_vp` showed up within 10m, 25m, 50m, and 100m of a stop.

2. `fct_vp_path`

* partitioned by `service_date`
* clustered by `(vp)_base64_url`, `schedule_base64_url`
* grain: `service_date-trip_instance_key`
Daily vp paths so we can use for a map. For the stops that had very few vp getting near it, here's where we want to see if the vp_path is detouring around it.

3. `int_gtfs_rt__vehicle_positions_trip_stop_day_map_grouping`

* partitioned by `dt`
* clustered by `dt`, `vp_base64_url`, `feed_key`
`service_date-trip_instance_key-vp_base64_url-feed_key-stop_id`
"""

import folium
import geopandas as gpd
import google.auth
import pandas as pd
import warehouse_utils
from rt_msa_utils import RT_MSA_DICT, VP_GCS
from shared_utils import catalog_utils, rt_dates

credentials, project = google.auth.default()
GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")


def prep_fct_vp_stop_metrics(
    file_name: str = RT_MSA_DICT.rt_vehicle_position_models.daily_stop_grain, **kwargs
) -> gpd.GeoDataFrame:
    """
    Any data wrangling related to fct_vp_stop_metrics.
    Take a pass through to see what can be worked into
    earlier download step or into warehouse.
    """
    stop_gdf = gpd.read_parquet(f"{VP_GCS}{file_name}.parquet", storage_options={"token": credentials.token}, **kwargs)

    # We want to capture stops that had "enough" vp trips serving the stop
    # Compare trips with vp passing through stop vs scheduled trips
    # Do not use stops that had extra low pct_vp_trips for potential detours
    stop_gdf = stop_gdf.assign(pct_vp_trips=stop_gdf.n_vp_trips.divide(stop_gdf.n_trips).round(3))

    return stop_gdf


def prep_vp_path(file_name: str = GTFS_DATA_DICT.speeds_tables.vp_path, **kwargs) -> pd.DataFrame:
    """
    Any data wrangling related to vp path.
    Take a pass through to see what can be worked into
    earlier download step or into warehouse.
    """
    SEGMENT_GCS = GTFS_DATA_DICT.speeds_tables.dir
    analysis_date = rt_dates.DATES["oct2025"]

    vp_path = pd.read_parquet(
        f"{SEGMENT_GCS}{file_name}_{analysis_date}.parquet",
        **kwargs,
        columns=["gtfs_dataset_name", "base64_url", "service_date", "trip_instance_key", "trip_id", "pt_array"],
    )

    # this needs to be handled at download
    vp_path = vp_path.assign(service_date=pd.to_datetime(vp_path.service_date))

    return vp_path


def prep_intermediate_vp_stops_trip_crosswalk(
    file_name: str = "int_gtfs_rt__vehicle_positions_trip_stop_day_map_grouping", **kwargs
) -> pd.DataFrame:
    """
    This intermediate table acts like a bridge.
    For the stops that we want to plot all the vp paths, we need to know which
    trip_instance_keys to pull from vp_path.
    """
    analysis_date = rt_dates.DATES["oct2025"]

    vp_stops_to_trips = pd.read_parquet(
        f"{VP_GCS}{file_name}_{analysis_date}_{analysis_date}.parquet",
        columns=["service_date", "base64_url", "feed_key", "trip_instance_key", "stop_id"],
        **kwargs,
    ).rename(columns={"base64_url": "vp_base64_url"})

    return vp_stops_to_trips


def plot_stops_and_exploded_vp(gdf: gpd.GeoDataFrame):
    """
    Plot stops that potentially could have detours,
    and also the exploded vp points
    """
    keep_cols = [
        "vp_name",
        "stop_id",
        "stop_name",
        "pct_vp_trips",
        "n_vp_near_10m",
        "n_vp_near_25m",
        "n_vp_near_50m",
        "n_vp_near_100m",
        "geometry",
    ]

    # Change marker size, even change according to value in column
    # https://stackoverflow.com/questions/73884448/geopandas-explore-marker-size-according-to-score
    # style_kwds={"style_function":lambda x: {"radius":x["properties"]["Score"]}}
    m = gdf[keep_cols].explore(
        "stop_id",
        legend=False,
        tiles="CartoDB Positron",
        name="vp within 10m of stop",
        marker_kwds={
            "radius": 5,
        },
    )

    vp_gdf = (
        gdf[["trip_instance_key", "pt_array"]]
        .explode("pt_array")
        .pipe(warehouse_utils.convert_to_gdf, geom_col="pt_array", geom_type="point")
    )

    # https://github.com/posit-dev/great-tables/blob/main/great_tables/_data_color/constants.py
    # quickly grab color palette we want to iterate through so each vp path is distinguishable
    viridis_color_palette = [
        # "#440154",
        # "#482878",
        "#3E4989",
        # "#31688E",
        "#26828E",
        # "#1F9E89",
        "#35B779",
        # "#6DCD59",
        "#B4DD2C",
        "#FDE725",
    ]

    # How should multiple trips be plotted?
    # Would like to select trips to display/turn off on map, but only if
    # this can get crazy when there are a lot of trips being displayed in the legend
    m = vp_gdf.explore("trip_instance_key", m=m, cmap=viridis_color_palette, name="vp path", legend=False)

    folium.LayerControl().add_to(m)

    return m


def filter_to_potential_detour_stops(
    stop_gdf: gpd.GeoDataFrame, intermediate_crosswalk: pd.DataFrame, vp_path: gpd.GeoDataFrame, cutoffs: list
) -> gpd.GeoDataFrame:
    """
    Filter to stops that potentially are detours,
    merge in crosswalk to get vp path for these stops
    """

    gdf = (
        stop_gdf[
            (stop_gdf.n_vp_near_100m > cutoffs[0])
            & (stop_gdf.n_vp_near_50m == cutoffs[1])
            & (stop_gdf.n_vp_near_25m == cutoffs[2])
            & (stop_gdf.n_vp_near_10m == cutoffs[3])
            & (stop_gdf.pct_vp_trips >= cutoffs[4])
        ]
        .merge(intermediate_crosswalk, on=["service_date", "feed_key", "vp_base64_url", "stop_id"], how="inner")
        .merge(
            vp_path.rename(
                columns={
                    "gtfs_dataset_name": "vp_name",
                    "base64_url": "vp_base64_url",
                }
            ),
            on=["service_date", "vp_base64_url", "vp_name", "trip_instance_key"],
            how="inner",
        )
    )

    return gdf


"""
# try 50, 100, 10, 75
gdf = filter_to_potential_detour_stops(
    stop_gdf,
    intermediate_df,
    vp_path,
    [50, 0, 0, 0, 0.2]
)

gdf.shape
display(gdf.vp_name.value_counts())
"""
