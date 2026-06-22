"""
Crosswalk of transit operators to legislative districts.

Spatial join fct_monthly_routes to legislative district
boundaries. Any intersection means that operator's data
will be included for that legislative district.
"""

from functools import cache

import geopandas as gpd
import pandas as pd
from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
from calitp_data_analysis.gcs_pandas import GCSPandas
from update_vars import GTFS_DATA_DICT, SHARED_GCS, file_name


@cache
def gcs_pandas():
    return GCSPandas()


@cache
def gcs_geopandas():
    return GCSGeoPandas()


def sjoin_shapes_legislative_districts(file_name: str) -> pd.DataFrame:
    """
    Grab shapes (fct_monthly_routes) and do a spatial join
    with legislative district.
    Keep 1 row for every operator-legislative_district combination.
    """
    monthly_routes = gcs_geopandas().read_parquet(
        f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}raw/"
        f"{GTFS_DATA_DICT.gtfs_digest_rollup.route_map}_{file_name}.parquet",
        columns=["analysis_name", "geometry"],
    )

    legislative_districts = gcs_geopandas().read_parquet(
        f"{SHARED_GCS}legislative_districts.parquet",
    )

    crosswalk = (
        gpd.sjoin(monthly_routes, legislative_districts, how="inner", predicate="intersects")[
            ["analysis_name", "legislative_district"]
        ]
        .drop_duplicates()
        .sort_values(["analysis_name", "legislative_district"])
        .reset_index(drop=True)
    )

    gcs_pandas().data_frame_to_parquet(
        crosswalk,
        f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/{GTFS_DATA_DICT.gtfs_digest_rollup.crosswalk_legislative}_{file_name}.parquet",
    )

    return crosswalk


if __name__ == "__main__":

    sjoin_shapes_legislative_districts(file_name)
