"""
Create routes file with identifiers including
route_id, route_name, operator name.
"""
import datetime
import geopandas as gpd
import pandas as pd
import yaml

import open_data_utils
from calitp_data_analysis import utils, geography_utils
from shared_utils import gtfs_utils_v2, portfolio_utils, publish_utils
from segment_speed_utils import helpers
from update_vars import analysis_date, TRAFFIC_OPS_GCS


def create_routes_file_for_export(date: str) -> gpd.GeoDataFrame:
    """
    Create a shapes (with associated route info) file for export.
    This allows users to plot the various shapes,
    transit path options, and select between variations for 
    a given route.
    """
    # Read in local parquets
    trips = helpers.import_scheduled_trips(
        date,
        columns = [
            "gtfs_dataset_key",
            "route_id", "route_type", 
            "shape_id", "shape_array_key",
            "route_long_name", "route_short_name", "route_desc"
        ],
        get_pandas = True
    ).dropna(subset="shape_array_key")
    
    shapes = helpers.import_scheduled_shapes(
        date,
        columns = ["shape_array_key", "n_trips", "geometry"],
        get_pandas = True,
        crs = geography_utils.WGS84
    ).dropna(subset="shape_array_key")
    
    df = pd.merge(
        shapes,
        trips,
        on = "shape_array_key",
        how = "inner"
    ).drop_duplicates(subset="shape_array_key").drop(columns = "shape_array_key")
         
    drop_cols = ["route_short_name", "route_long_name", "route_desc"]
    route_shape_cols = ["schedule_gtfs_dataset_key", "route_id", "shape_id"]
    
    routes_assembled = (portfolio_utils.add_route_name(df)
                        .drop(columns = drop_cols)
                        .sort_values(route_shape_cols)
                        .drop_duplicates(subset=route_shape_cols)
                        .reset_index(drop=True)
                      )    
    routes_assembled2 = open_data_utils.standardize_operator_info_for_exports(
        routes_assembled, 
        date
    ).pipe(remove_erroneous_shapes)    
            
    return routes_assembled2


def remove_erroneous_shapes(
    shapes_with_route_info: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Check if line is simple for Amtrak. If it is, keep. 
    If it's not simple (line crosses itself), drop.
    
    In Jun 2023, some Amtrak shapes appeared to be funky, 
    but in prior months, it's been ok.
    Checking for length is fairly time-consuming.
    """
    amtrak = "Amtrak Schedule"
    
    possible_error = shapes_with_route_info[shapes_with_route_info.name==amtrak]
    ok = shapes_with_route_info[shapes_with_route_info.name != amtrak]
    
    # Check if the line crosses itself
    ok_amtrak = possible_error.assign(
        simple = possible_error.geometry.is_simple
    ).query("simple == True").drop(columns = "simple")
    
    ok_shapes = pd.concat(
        [ok, ok_amtrak], 
        axis=0
    ).reset_index(drop=True)

    return ok_shapes


def patch_previous_dates(
    current_routes: gpd.GeoDataFrame,
    current_date: str,
    published_operators_yaml: str = "../gtfs_funnel/published_operators.yml"
) -> gpd.GeoDataFrame:
    """
    Compare to the yaml for what operators we want, and
    patch in previous dates for the 10 or so operators
    that do not have data for this current date.
    """
    with open(published_operators_yaml) as f:
        published_operators_dict = yaml.safe_load(f)
    
    patch_operators_dict = {
        str(date): operator_list for 
        date, operator_list in published_operators_dict.items() 
        if str(date) != current_date
    }
    
    partial_dfs = []


    for one_date, operator_list in patch_operators_dict.items():
        df_to_add = publish_utils.subset_table_from_previous_date(
            gcs_bucket = TRAFFIC_OPS_GCS,
            filename = f"export/ca_transit_routes",
            operator_and_dates_dict = patch_operators_dict,
            date = one_date, 
            crosswalk_col = "schedule_gtfs_dataset_key",
            data_type = "gdf"
        ).pipe(open_data_utils.standardize_operator_info_for_exports, one_date)
        
        partial_dfs.append(df_to_add)

    patch_routes = pd.concat(partial_dfs, axis=0, ignore_index=True)

    published_routes = pd.concat(
        [current_routes, patch_routes], 
        axis=0, ignore_index=True
    )
    
    return published_routes
    


def finalize_export_df(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Suppress certain columns used in our internal modeling for export.
    """
    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()
    
    # Change column order
    route_cols = [
        'organization_source_record_id', 'organization_name',
        'route_id', 'route_type', 'route_name_used']
    shape_cols = ['shape_id', 'n_trips']
    agency_ids = ['base64_url']
    
    col_order = route_cols + shape_cols + agency_ids + ['geometry']
    df2 = (df[df.schedule_gtfs_dataset_key.isin(public_feeds)][col_order]
           .reindex(columns = col_order)
           .rename(columns = open_data_utils.RENAME_COLS)
           .reset_index(drop=True)
    )
    
    return df2


if __name__ == "__main__":
    time0 = datetime.datetime.now()
    
    # Make an operator-feed level file (this is published)    
    routes = create_routes_file_for_export(analysis_date)  
    
    utils.geoparquet_gcs_export(
        routes,
        TRAFFIC_OPS_GCS,
        f"export/ca_transit_routes_{analysis_date}"
    )
    
    published_routes = patch_previous_dates(
        routes, 
        analysis_date,
    ).pipe(finalize_export_df)
        
    utils.geoparquet_gcs_export(
        published_routes, 
        TRAFFIC_OPS_GCS, 
        "ca_transit_routes"
    )
    
    time1 = datetime.datetime.now()
    print(f"Execution time for routes script: {time1-time0}")
