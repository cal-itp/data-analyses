"""
Create stops file with identifiers including
route_id, route_name, agency_id, agency_name.
"""
import datetime
import geopandas as gpd
import intake
import pandas as pd
import yaml

import open_data_utils
from calitp_data_analysis import geography_utils, utils
from shared_utils import publish_utils
from update_vars import (analysis_date, 
                         GTFS_DATA_DICT,
                         TRAFFIC_OPS_GCS, 
                         RT_SCHED_GCS, SCHED_GCS
                        )

catalog = intake.open_catalog("../_shared_utils/shared_utils/shared_data_catalog.yml")

def create_stops_file_for_export(
    date: str,
) -> gpd.GeoDataFrame:
    """
    Read in scheduled stop metrics table and attach crosswalk 
    info related to organization for Geoportal.
    """
    time0 = datetime.datetime.now()

    # Read in parquets
    STOP_FILE = GTFS_DATA_DICT.rt_vs_schedule_tables.sched_stop_metrics

    stops = gpd.read_parquet(
        f"{RT_SCHED_GCS}{STOP_FILE}_{date}.parquet"
    )
    
    stops2 = open_data_utils.standardize_operator_info_for_exports(stops, date)

    time1 = datetime.datetime.now()
    print(f"get stops for date: {time1 - time0}")
    
    return stops2


def add_distance_to_state_highway(
    stops: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Bring in State Highway Network gdf and add a column that tells us
    distance (in meters) between stop and SHN.
    For stops outside of CA, this will not be that meaningful.
    See discussion in:
    https://github.com/cal-itp/data-analyses/issues/1182
    https://github.com/cal-itp/data-analyses/issues/1321
    """
    orig_crs = stops.crs
    
    shn = catalog.state_highway_network.read()[
        ["geometry"]].to_crs(geography_utils.CA_NAD83Albers_m).dissolve().geometry.iloc[0]    

    stops = stops.to_crs(geography_utils.CA_NAD83Albers_m)
    
    stops = stops.assign(
        meters_to_shn = stops.geometry.distance(shn).round(1)
    )
    
    return stops.to_crs(orig_crs)


def patch_previous_dates(
    current_stops: gpd.GeoDataFrame,
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

    STOP_FILE = GTFS_DATA_DICT.rt_vs_schedule_tables.sched_stop_metrics

    for one_date, operator_list in patch_operators_dict.items():
        df_to_add = publish_utils.subset_table_from_previous_date(
            gcs_bucket = RT_SCHED_GCS,
            filename = STOP_FILE,
            operator_and_dates_dict = patch_operators_dict,
            date = one_date, 
            crosswalk_col = "schedule_gtfs_dataset_key",
            data_type = "gdf"
        ).pipe(open_data_utils.standardize_operator_info_for_exports, one_date)
        
        partial_dfs.append(df_to_add)

    patch_stops = pd.concat(partial_dfs, axis=0, ignore_index=True)

    published_stops = pd.concat(
        [current_stops, patch_stops], 
        axis=0, ignore_index=True
    ).pipe(add_distance_to_state_highway)
    
    
    return published_stops
    

def finalize_export_df(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame: 
    """
    Suppress certain columns used in our internal modeling for export.
    """
    # Change column order
    route_cols = [
        'organization_source_record_id', 'organization_name',
    ]       
    stop_cols = [
        'stop_id', 'stop_name', 
        # add GTFS stop-related metrics
        'n_routes', 'route_ids_served', 'route_types_served', 
        'n_arrivals', 'n_hours_in_service',
        # this is derived column
        'meters_to_shn'
    ]
    agency_ids = ['base64_url', 'caltrans_district']
    
    col_order = route_cols + stop_cols + agency_ids + ['geometry']
    
    df2 = (df[col_order]
           .reindex(columns = col_order)
           .rename(columns = open_data_utils.STANDARDIZED_COLUMNS_DICT)
           .reset_index(drop=True)
    )
    
    return df2


if __name__ == "__main__":
        
    time0 = datetime.datetime.now()

    stops = create_stops_file_for_export(analysis_date)  

    published_stops = patch_previous_dates(
        stops, 
        analysis_date,
    ).pipe(finalize_export_df)    

    
    utils.geoparquet_gcs_export(
        published_stops,
        TRAFFIC_OPS_GCS,
        f"export/ca_transit_stops_{analysis_date}"
    )
    
    utils.geoparquet_gcs_export(
        published_stops, 
        TRAFFIC_OPS_GCS, 
        "ca_transit_stops"
    )
    
    time1 = datetime.datetime.now()
    print(f"Execution time for stops script: {time1 - time0}")
