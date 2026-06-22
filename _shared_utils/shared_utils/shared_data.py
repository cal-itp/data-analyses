"""
One-off functions, run once, save datasets for shared use.
"""

from functools import cache

import geopandas as gpd
from calitp_data_analysis import geography_utils, utils
from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
from calitp_data_analysis.gcs_pandas import GCSPandas
from calitp_data_analysis.sql import to_snakecase
from shared_utils import catalog_utils


@cache
def gcs_pandas():
    return GCSPandas()


@cache
def gcs_geopandas():
    return GCSGeoPandas()


GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/shared_data/"
COMPILED_CACHED_GCS = "gs://calitp-analytics-data/data-analyses/rt_delay/compiled_cached_views/"


def make_clean_state_highway_network():
    """
    Create State Highway Network dataset.
    """
    URL = "https://opendata.arcgis.com/datasets/" "77f2d7ba94e040a78bfbe36feb6279da_0.geojson"

    gdf = gcs_geopandas().read_file(URL)

    # Save a raw, undissolved version
    utils.geoparquet_gcs_export(
        gdf.drop(columns=["Shape_Length", "OBJECTID"]).pipe(to_snakecase), GCS_FILE_PATH, "state_highway_network_raw"
    )

    keep_cols = ["Route", "County", "District", "RouteType", "Direction", "geometry"]

    gdf = gdf[keep_cols]
    print(f"# rows before dissolve: {len(gdf)}")

    # See if we can dissolve further - use all cols except geometry
    # Should we dissolve further and use even longer lines?
    dissolve_cols = [c for c in list(gdf.columns) if c != "geometry"]

    gdf2 = gdf.dissolve(by=dissolve_cols).reset_index()
    print(f"# rows after dissolve: {len(gdf2)}")

    # Export to GCS
    utils.geoparquet_gcs_export(gdf2, GCS_FILE_PATH, "state_highway_network")


def dissolve_shn_district() -> gpd.GeoDataFrame:
    """
    Dissolve State Highway Network so there will only be one row for each
    route name, route type, and Caltrans district. Find the length
    of the highway and do some light cleaning.
    """
    # Read in the dataset and change the CRS to one to feet.
    SHN_FILE = catalog_utils.get_catalog("shared_data_catalog").state_highway_network.urlpath

    shn = gcs_geopandas().read_parquet(SHN_FILE).to_crs(geography_utils.CA_NAD83Albers_ft)

    # Dissolve by route which represents the the route's name and drop the other columns
    # because they are no longer relevant.
    shn_dissolved = (shn.dissolve(by=["Route", "District"]).reset_index())[["Route", "District", "geometry"]]

    # Rename because I don't want any confusion between SHN route and
    # transit route.
    shn_dissolved = shn_dissolved.rename(columns={"Route": "shn_route"})
    shn_dissolved.columns = shn_dissolved.columns.str.lower()
    # Find the length of each highway.
    shn_dissolved = shn_dissolved.assign(
        highway_feet=shn_dissolved.geometry.length,
        shn_route=shn_dissolved.shn_route.astype(int).astype(str),
    )

    # Save this out so I don't have to dissolve it each time.
    gcs_geopandas().geo_data_frame_to_parquet(
        shn_dissolved, f"{GCS_FILE_PATH}shn_dissolved_by_ct_district_route.parquet"
    )
    return shn_dissolved


def buffer_shn(buffer_amount: int, file_name: str) -> gpd.GeoDataFrame:
    """
    Add a buffer to the SHN before overlaying it with
    transit routes.
    """
    # GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/state_highway_network/"

    # Read in the dissolved SHN file
    shn_df = gcs_geopandas().read_parquet(f"{GCS_FILE_PATH}{file_name}.parquet")

    # Buffer the state highway.
    shn_df_buffered = shn_df.assign(
        geometry=shn_df.geometry.buffer(buffer_amount),
    )

    # Save it out so we won't have to buffer over again and
    # can just read it in.
    gcs_geopandas().geo_data_frame_to_parquet(
        shn_df_buffered, f"{GCS_FILE_PATH}shn_buffered_{buffer_amount}_ft_{file_name}.parquet"
    )

    return shn_df_buffered


if __name__ == "__main__":
    # Run functions to create these datasets...store in GCS
    SHN_HWY_BUFFER_FEET = 50
    PARALLEL_HWY_BUFFER_FEET = geography_utils.FEET_PER_MI * 0.5

    # State Highway Network
    make_clean_state_highway_network()
    dissolve_shn_district()
    buffer_shn(SHN_HWY_BUFFER_FEET, "shn_dissolved_by_ct_district_route")
