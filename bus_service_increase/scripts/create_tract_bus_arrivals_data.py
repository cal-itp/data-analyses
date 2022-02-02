import geopandas as gpd
import pandas as pd

import utils
import prep_data
import shared_utils

from siuba import *


def create_bus_arrivals_by_tract_data():
    aggregated_stops_with_geom = pd.read_parquet(
        f"{utils.DATA_PATH}aggregated_stops_with_geom.parquet")

    census_tracts = prep_data.generate_calenviroscreen_lehd_data(prep_data.datasets)
    
    # If there are the same stops with multiple lat/lon values
    # Drop duplicates
    aggregated_stops = (aggregated_stops_with_geom
                        .sort_values(["itp_id", "stop_id", "stop_lon", "stop_lat"])
           .drop_duplicates(subset = ["itp_id", "stop_id"])
           .reset_index(drop=True)
    )
    
    print(f"# obs in bus stop arrivals: {len(aggregated_stops_with_geom)}")
    print(f"# obs in bus stop arrivals, no dups lat/lon: {len(aggregated_stops)}")
    
    # Add stop geometry column (default parameter is WGS84)
    bus_stops = shared_utils.geography_utils.create_point_geometry(aggregated_stops).drop(
        columns = ["stop_lon", "stop_lat"])
    
    gdf = gpd.sjoin(
        bus_stops, 
        census_tracts.to_crs(shared_utils.geography_utils.WGS84),
        # Use inner, or else left join will result in some NaN tracts
        how = "inner",
        predicate = "intersects"
    ).drop(columns = "index_right")
    
    # Aggregate by tract level and count bus arrivals, number of stops, etc
    gdf2 = shared_utils.geography_utils.aggregate_by_geography(
        gdf, 
        group_cols = ["Tract"], 
        sum_cols = ["num_arrivals"],
        count_cols = ["stop_id"],
        nunique_cols = ["itp_id"]
    )
    
    # Attach tract geometry back, since our previous spatial join kept bus stop's point geometry
    final = shared_utils.geography_utils.attach_geometry(
        gdf2, census_tracts, 
        merge_col = ["Tract"], join="left"
    )
    
    # Export to GCS
    shared_utils.utils.geoparquet_gcs_export(final, utils.GCS_FILE_PATH, "bus_stop_times_by_tract")


if __name__ == "__main__":
    # Run this to get the static parquet files
    
    # Get analysis dataset for service increase estimator?
    
    # Get analysis dataset for bus arrivals by tract
    create_bus_arrivals_by_tract_data()