"""
Create routes file with identifiers including
route_id, route_name, operator name.
"""
import datetime
import geopandas as gpd
import pandas as pd
import yaml

import open_data_utils
from calitp_data_analysis.geography_utils import WGS84
from calitp_data_analysis import utils
from shared_utils import portfolio_utils, publish_utils
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
        crs = WGS84
    ).dropna(subset="shape_array_key")
    
    df = pd.merge(
        shapes,
        trips,
        on = "shape_array_key",
        how = "inner"
    ).drop_duplicates(subset="shape_array_key").drop(columns = "shape_array_key")
         
    drop_cols = ["route_short_name", "route_long_name", "route_desc"]
    route_shape_cols = ["schedule_gtfs_dataset_key", "route_id", "shape_id"]
    
    routes_assembled = (
        portfolio_utils.add_route_name(df)
        .drop(columns = drop_cols)
        .sort_values(route_shape_cols)
        .drop_duplicates(subset=route_shape_cols)
        .reset_index(drop=True)
    )
    
    routes_assembled2 = open_data_utils.standardize_operator_info_for_exports(
        routes_assembled, 
        date
    ).pipe(remove_erroneous_shapes)    
    
    routes_assembled2 = routes_assembled2.assign(
    route_length_feet=routes_assembled2.geometry.to_crs(
        geography_utils.CA_NAD83Albers_ft
    ).length
    )
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
    # Read in the published operators file
    with open(published_operators_yaml) as f:
        published_operators_dict = yaml.safe_load(f)
    
    # Convert the published operators file into a dict mapping dates to an iterable of operators
    patch_operators_dict = {
        str(date): operator_list for 
        date, operator_list in published_operators_dict.items() 
        if str(date) != current_date # Exclude the current (analysis) date, since that does not need to be patched
    }
    
    partial_dfs = []
    
    # For each date and corresponding iterable of operators, get the data from the last time they appeared
    for one_date, operator_list in patch_operators_dict.items():
        df_to_add = publish_utils.subset_table_from_previous_date(
            gcs_bucket = TRAFFIC_OPS_GCS,
            filename = f"ca_transit_routes",
            operator_and_dates_dict = patch_operators_dict,
            date = one_date, 
            crosswalk_col = "schedule_gtfs_dataset_key",
            data_type = "gdf"
        ).pipe(open_data_utils.standardize_operator_info_for_exports, one_date)
        
        partial_dfs.append(df_to_add)

    patch_routes = pd.concat(partial_dfs, axis=0, ignore_index=True)
    
    # Concat the current data to the "backfill" data
    published_routes = pd.concat(
        [current_routes, patch_routes], 
        axis=0, ignore_index=True
    )
    
    return published_routes

def dissolve_shn(columns_to_dissolve: list, file_name: str) -> gpd.GeoDataFrame:
    """
    Dissolve State Highway Network so there will only be one row for each
    route name and route type
    """
    # Read in the dataset and change the CRS to one to feet.
    SHN_FILE = catalog_utils.get_catalog(
        "shared_data_catalog"
    ).state_highway_network.urlpath

    shn = gpd.read_parquet(
        SHN_FILE,
        storage_options={"token": credentials.token},
    ).to_crs(geography_utils.CA_NAD83Albers_ft)

    # Dissolve by route which represents the the route's name and drop the other columns
    # because they are no longer relevant.
    shn_dissolved = (shn.dissolve(by=columns_to_dissolve).reset_index())[
        columns_to_dissolve + ["geometry"]
    ]

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
    shn_dissolved.to_parquet(
        f"gs://calitp-analytics-data/data-analyses/state_highway_network/shn_dissolved_by_{file_name}.parquet",
        filesystem=fs,
    )
    return shn_dissolved

def buffer_shn(buffer_amount: int, file_name: str) -> gpd.GeoDataFrame:
    """
    Add a buffer to the SHN before overlaying it with
    transit routes.
    """
    GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/state_highway_network/"

    # Read in the dissolved SHN file
    shn_df = gpd.read_parquet(
        f"{GCS_FILE_PATH}shn_dissolved_by_{file_name}.parquet",
        storage_options={"token": credentials.token},
    )

    # Buffer the state highway.
    shn_df_buffered = shn_df.assign(
        geometry=shn_df.geometry.buffer(buffer_amount),
    )

    # Save it out so we won't have to buffer over again and
    # can just read it in.
    shn_df_buffered.to_parquet(
        f"{GCS_FILE_PATH}shn_buffered_{buffer_amount}_ft_{file_name}.parquet",
        filesystem=fs,
    )

    return shn_df_buffered

def routes_shn_intersection(
    routes_gdf: gpd.GeoDataFrame, buffer_amount: int, file_name: str
) -> gpd.GeoDataFrame:
    """
    Overlay the most recent transit routes with a buffered version
    of the SHN
    """
    GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/state_highway_network/"

    # Read in buffered shn here or re buffer if we don't have it available.
    HWY_FILE = f"{GCS_FILE_PATH}shn_buffered_{buffer_amount}_ft_{file_name}.parquet"

    if fs.exists(HWY_FILE):
        shn_routes_gdf = gpd.read_parquet(
            HWY_FILE, storage_options={"token": credentials.token}
        )
    else:
        shn_routes_gdf = buffer_shn(buffer_amount)

    # Process the most recent transit route geographies and ensure the
    # CRS matches the SHN routes' GDF so the overlay doesn't go wonky.
    routes_gdf = routes_gdf.to_crs(shn_routes_gdf.crs)

    # Overlay transit routes with the SHN geographies.
    gdf = gpd.overlay(
        routes_gdf, shn_routes_gdf, how="intersection", keep_geom_type=True
    )

    # Calcuate the percent of the transit route that runs on a highway, round it up and
    # multiply it by 100. Drop the geometry because we want the original transit route
    # shapes.
    gdf = gdf.assign(
        pct_route_on_hwy=(gdf.geometry.length / gdf.route_length_feet).round(3) * 100,
    ).drop(
        columns=[
            "geometry",
        ]
    )

    # Join back the dataframe above with the original transit route dataframes
    # so we can have the original transit route geographies.
    gdf2 = pd.merge(
        routes_gdf,
        gdf,
        on=[
            "n_trips",
            "schedule_gtfs_dataset_key",
            "route_id",
            "route_type",
            "shape_id",
            "route_name_used",
            "route_length_feet",
        ],
        how="left",
    )

    # Clean up
    gdf2.district = gdf2.district.fillna(0).astype(int)
    return gdf2

def group_route_district(df: pd.DataFrame, pct_route_on_hwy_agg: str) -> pd.DataFrame:
    """
    Aggregate by adding all the districts and SHN to a single row, rather than
    multiple and sum up the total % of SHN a transit route intersects with.

    df: the dataframe you want to aggregate
    pct_route_on_hwy_agg: whether you want to find the max, min, sum, etc on the column
    "pct_route_on_hwy_across_districts"
    """

    agg1 = (
        df.groupby(
            ["schedule_gtfs_dataset_key", "route_type", "shape_id", "route_id", "route_name_used"],
            as_index=False,
        )[["shn_route", "shn_districts", "pct_route_on_hwy_across_districts"]]
        .agg(
            {
                "shn_route": lambda x: ", ".join(set(x.astype(str))),
                "shn_districts": lambda x: ", ".join(set(x.astype(str))),
                "pct_route_on_hwy_across_districts": pct_route_on_hwy_agg,
            }
        )
        .reset_index(drop=True)
    )

    # Clean up
    agg1.pct_route_on_hwy_across_districts = (
        agg1.pct_route_on_hwy_across_districts.astype(float).round(2)
    )
    return agg1

def create_on_shs_column(df):
    df["on_shs"] = np.where(df["pct_route_on_hwy_across_districts"] == 0, "N", "Y")
    return df

def add_shn_information(gdf: gpd.GeoDataFrame, buffer_amt:int) -> pd.DataFrame:
    """
    Prepare the gdf to join with the existing transit_routes
    dataframe that is published on the Open Data Portal
    """
    # Overlay
    intersecting = routes_shn_intersection(gdf, buffer_amt, "ct_district_route")
    # Rename column
    gdf = gdf.rename(columns={"pct_route_on_hwy": "pct_route_on_hwy_across_districts",
                             "district":"shn_districts"})
    # Group the dataframe so that one route only has one
    # row instead of multiple rows after finding its
    # intersection with any SHN routes.
    agg1 = group_route_district(gdf, "sum")

    # Add yes/no column to signify if a transit route intersects
    # with a SHN route
    agg1 = create_on_shs_column(agg1)

    # Clean up rows that are tagged as "on_shs==N" but still have values
    # that appear. 
    agg1.loc[(agg1['on_shs'] == "N") & (agg1['shn_districts'] != "0"), 
                        ['shn_districts', 'shn_route']] = np.nan
    return agg1

def finalize_export_df(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Suppress certain columns used in our internal modeling for export.
    """
    # Change column order
    route_cols = [
        'organization_source_record_id', 'organization_name',
        'route_id', 'route_type', 'route_name_used']
    shape_cols = ['shape_id', 'n_trips']
    agency_ids = ['base64_url']
    shn_cols = ["shn_route","on_shs","shn_districts","pct_route_on_hwy_across_districts"]
    col_order = route_cols + shape_cols + agency_ids + shn_cols + ['geometry']
    df2 = (df[col_order]
           .reindex(columns = col_order)
           .rename(columns = open_data_utils.STANDARDIZED_COLUMNS_DICT)
           .reset_index(drop=True)
    )
    
    return df2

if __name__ == "__main__":
    
    time0 = datetime.datetime.now()
    
    # Make an operator-feed level file (this is published)    
    routes = create_routes_file_for_export(analysis_date)  
    
    # Export into GCS (outside export/)
    # create_routes is different than create_stops, which already has
    # a table created in gtfs_funnel that we can use to patch in previous dates
    # here, we have to create those for each date, then save a copy
    # the export/ folder contains the patched versions of the routes
    utils.geoparquet_gcs_export(
        routes,
        TRAFFIC_OPS_GCS,
        f"ca_transit_routes_{analysis_date}"
    )
    
    published_routes = patch_previous_dates(
        routes, 
        analysis_date,
    ).pipe(add_shn_information).pipe(finalize_export_df)
        
    utils.geoparquet_gcs_export(
        published_routes, 
        TRAFFIC_OPS_GCS, 
        "ca_transit_routes"
    )
    
    time1 = datetime.datetime.now()
    print(f"Execution time for routes script: {time1-time0}")
