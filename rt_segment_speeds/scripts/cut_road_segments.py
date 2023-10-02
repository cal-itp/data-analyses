"""
Cut road segments.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import gcsfs
import pandas as pd

from calitp_data_analysis.sql import to_snakecase
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (analysis_date, 
                                              SEGMENT_GCS, 
                                              GCS_FILE_PATH, 
                                              PROJECT_CRS
                                             )
from calitp_data_analysis import geography_utils, utils

SHARED_GCS = f"{GCS_FILE_PATH}shared_data/"

"""
TIGER
"""
def load_roads(road_type_wanted: list) -> gpd.GeoDataFrame:
    """
    Load roads based on what you filter.

    Args:
        road_type_wanted (list): the type of roads you want.
            S1100: primary roads
            S1200: secondary roads
            S1400: local roads                    
                            
        https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2019/TGRSHP2019_TechDoc.pdf

    Returns:
        gdf. As of 4/18/23, returns 953914 nunique linearid
    """
    df = gpd.read_parquet(
        f"{SHARED_GCS}all_roads_2020_state06.parquet",
        filters=[("MTFCC", "in", road_type_wanted)],
        columns=["LINEARID", "geometry", "FULLNAME"],
    ).to_crs(PROJECT_CRS)

    # If a road has mutliple rows but the same
    # linear ID, dissolve it so it becomes one row.
    df = (
        df.drop_duplicates()
        .dissolve(by=["LINEARID"])
        .reset_index()
        .drop_duplicates()
        .reset_index(drop=True)
    )

    df = to_snakecase(df)

    return df

"""
GTFS
"""
def gtfs_stops_operators(date: str) -> gpd.GeoDataFrame:
    """
    Load stops with operator and feed key information.

    Args:
        date: analysis date
    """
    stops = (
        helpers.import_scheduled_stops(
            date, 
            columns = ["feed_key", "stop_id", "stop_key", "geometry"],
            get_pandas = True,
            crs = PROJECT_CRS
        )
        .drop_duplicates()
    )

    # Buffer each stop by 50 feet
    stops = stops.assign(buffered_geometry=stops.geometry.buffer(50))

    # Set geometry
    stops = stops.set_geometry("buffered_geometry")

    # Merge for operator information
    trips = (
        helpers.import_scheduled_trips(analysis_date, (), ["name", "feed_key"])
        .compute()
        .drop_duplicates()
    )

    m1 = pd.merge(stops, trips, on=["feed_key"], how="left")

    # Fill in na
    m1.name = m1.name.fillna("None")

    return m1

def gtfs_routes_operators(date:str) -> gpd.GeoDataFrame:
    """
    Load routes with operator and feed key information.

    Args:
        date: analysis date
    """
    gtfs_shapes = helpers.import_scheduled_shapes(date).compute().drop_duplicates()

    gtfs_shapes = gtfs_shapes.set_crs(geography_utils.CA_NAD83Albers)

    trips = (
        helpers.import_scheduled_trips(date, (), ["name", "shape_array_key"])
        .compute()
        .drop_duplicates()
    )

    m1 = pd.merge(gtfs_shapes, trips, how="left", on="shape_array_key")

    return m1

def order_operators(date:str) -> list:
    """
    Reorder a list of operators in which the largest
    ones will be at the top of the list.

    Args:
        date: analysis date
    """
    operator_list = (
        helpers.import_scheduled_trips(date, (), ["name"]).compute().sort_values("name")
    )
    operator_list = operator_list.name.unique().tolist()

    # Reorder list so the biggest operators are at the beginning
    # based on NTD services data
    big_operators = [
        "LA DOT Schedule",
        "LA Metro Bus Schedule",
        "LA Metro Rail Schedule",
        "Bay Area 511 Muni Schedule",
        "Bay Area 511 AC Transit Schedule",
        "Bay Area 511 Santa Clara Transit Schedule",
        "Bay Area 511 BART Schedule",
        "San Diego Schedule",
        "OCTA Schedule",
        "Sacramento Schedule",
        "Bay Area 511 Sonoma-Marin Area Rail Transit Schedule",
        "Bay Area 511 SFO AirTrain Schedule",
        "Bay Area 511 South San Francisco Shuttle Schedule",
        "Bay Area 511 Marin Schedule",
        "Bay Area 511 County Connection Schedule",
        "Bay Area 511 MVGO Schedule",
        "Bay Area 511 Commute.org Schedule",
        "Bay Area 511 Union City Transit Schedule",
        "Bay Area 511 BART Schedule",
        "Bay Area 511 Caltrain Schedule",
        "Bay Area 511 Fairfield and Suisun Transit Schedule",
        "Bay Area 511 Dumbarton Express Schedule",
        "Bay Area 511 SamTrans Schedule",
        "Bay Area 511 Vine Transit Schedule",
        "Bay Area 511 Tri-Valley Wheels Schedule",
        "Bay Area 511 Sonoma County Transit Schedule",
        "Bay Area 511 Santa Rosa CityBus Schedule",
        "Bay Area 511 Golden Gate Transit Schedule",
        "Bay Area 511 Golden Gate Ferry Schedule",
        "Bay Area 511 San Francisco Bay Ferry Schedule",
        "Bay Area 511 SolTrans Schedule",
        "Bay Area 511 ACE Schedule",
        "Bay Area 511 Emery Go-Round Schedule",
        "Bay Area 511 Tri Delta Schedule",
        "Bay Area 511 Petaluma Schedule",
        "Bay Area 511 Capitol Corridor Schedule",
    ]

    # Delete off the big operators
    operator_list = list(set(operator_list) - set(big_operators))

    # Add back in the operators
    final_list = big_operators + operator_list

    return final_list

"""
Local Roads
"""
def loop_sjoin(date:str, local_roads_gdf:gpd.GeoDataFrame, gdf_routes_stops:gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    By operator, sjoin either routes or stops to the tiger gdf. 
    Delete off any linear ids that have already been joined.

    Args:
        local_roads_gdf: local roads gdf, use the buffered version of Tiger 
        gdf_routes_stops: stops or routes gdf
        date: analysis date
    """
    # Empty dataframe
    sjoin_full_results = pd.DataFrame()

    # Find all unique operators, ordered by largest operators first
    operators_list = order_operators(date)

    # Loop through and sjoin by operator
    for operator in operators_list:
        shapes_filtered = gdf_routes_stops.loc[
            gdf_routes_stops.name == operator
        ].reset_index(drop=True)

        # Delete any local road linear ids that have already been found by an operator
        try:
            # List of linear IDS
            linearid_to_delete = sjoin_full_results.linearid.unique().tolist()

            # Filter out the linear IDS in buffered local roads
            local_roads_gdf = local_roads_gdf[
                ~local_roads_gdf.linearid.isin(linearid_to_delete)
            ].reset_index(drop=True)
        except:
            pass

        # Do a sjoin but  keep the linearid as the only column
        sjoin1 = (
            gpd.sjoin(
                local_roads_gdf,
                shapes_filtered,
                how="inner",
                predicate="intersects",
            )[["linearid"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        sjoin_full_results = pd.concat([sjoin_full_results, sjoin1], axis=0)

    sjoin_full_results = sjoin_full_results.drop_duplicates()

    return sjoin_full_results

def sjoin_stops(buffered_roads:gpd.GeoDataFrame, original_roads:gpd.GeoDataFrame, date:str) -> gpd.GeoDataFrame:
    """
    Sjoin stops to local roads.
    
    Args:
        buffered_roads: local TIGER roads gdf that are buffered
        original_roads: original local TIGER roads gdf
        date: analysis date

    Returns:
        A list of linear IDs that have already
        been found and a GDF
    """
    start = datetime.datetime.now()

    # Load stops
    gtfs_stops = gtfs_stops_operators(date)

    # Loop through and sjoin by operator
    stops_sjoin = loop_sjoin(date, buffered_roads, gtfs_stops)

    # Merge back to original local roads gdf, so we have the
    # non buffered geometry.
    m1 = pd.merge(original_roads, stops_sjoin, on="linearid", how="inner")

    # Fill in null values for fullname
    m1.fullname = m1.fullname.fillna("None")

    # Find linear ids to delete
    linearid_to_delete = m1.linearid.unique().tolist()

    # Save
    m1.to_parquet(f"{SHARED_GCS}local_roads_stops_sjoin.parquet")

    end = datetime.datetime.now()
    print(f"Done with sjoin with stops with local roads. Time lapsed: {end-start}")

    return m1, linearid_to_delete

def sjoin_routes(buffered_roads: gpd.GeoDataFrame, original_roads: gpd.GeoDataFrame, date:str, linearid_to_delete: list) -> gpd.GeoDataFrame:
    """
    Sjoin routes to local roads.
    
    Args:
        buffered_roads: local TIGER roads that are buffered
        original_roads: original local Tiger roads
        date: analysis date
        linearid_to_delete: linear ids to delete that have already been found 
        while applying a sjoin to stops.
    """
    start = datetime.datetime.now()

    # Load stops
    gtfs_routes = gtfs_routes_operators(date)

    # Delete out linear ids that have already been found
    local_roads_buffered = buffered_roads[~buffered_roads.linearid.isin(linearid_to_delete)].reset_index(drop=True)
    local_roads_og = original_roads[~original_roads.linearid.isin(linearid_to_delete)].reset_index(drop=True)
    
    # Sjoin
    routes_sjoin = loop_sjoin(date, local_roads_buffered, gtfs_routes)

    # Merge back to original local roads, so we have the
    # non buffered geometry.
    m1 = pd.merge(local_roads_og, routes_sjoin, on="linearid", how="inner")

    # Fill in null values for fullname
    m1.fullname = m1.fullname.fillna("None")

    # Save
    m1.to_parquet(f"{SHARED_GCS}local_roads_routes_sjoin.parquet")

    end = datetime.datetime.now()
    print(f"Done with sjoin with routes and local roads. Time lapsed: {end-start}")

    return m1

def sjoin_local_roads(date:str) -> gpd.GeoDataFrame:
    """
    Sjoin local roads with stops first then routes.
    
    Args:
        date: analysis date
    """
    start = datetime.datetime.now()
    print(f"Begin sjoin")
    
    # Load local roads - not buffered
    local_roads_og = load_roads(["S1400"])
    
    # Load local roads - buffered
    local_roads_buffered = local_roads_og.assign(geometry=local_roads_og.geometry.buffer(200))
    local_roads_buffered = local_roads_buffered.set_geometry('geometry')
    
    print(f"Done buffering")
    
    # Deal with stops first
    stops_sjoin, linear_id_stops = sjoin_stops(
        local_roads_buffered, local_roads_og, date
    )

    # Move onto routes
    routes_sjoin = sjoin_routes(
        local_roads_buffered, local_roads_og, date, linear_id_stops
    )

    # Stack
    all_local_roads = pd.concat([stops_sjoin, routes_sjoin], axis=0)
    
    file_date = date.replace('-','_')
    all_local_roads.to_parquet(f"{SHARED_GCS}local_roads_all_routes_stops_sjoin_{file_date}.parquet")
    
    end = datetime.datetime.now()

    print(f"Done with doing an sjoin for all local roads. Time lapsed: {end-start}")
    return all_local_roads

# Find all the parquets again
def find_files(phrase_to_find: str) -> list:
    """
    Grab a list of files that contain the
    phrase inputted. 
    """
    folder = f"{SHARED_GCS}partitioned_tiger"
    
    # Create a list of all the files in my folder
    all_files_in_folder = fs.ls(folder)

    # Grab only files with the string "Verizon_no_coverage_"
    my_files = [i for i in all_files_in_folder if phrase_to_find in i]

    # String to add to read the files
    my_string = "gs://"
    my_files = [my_string + i for i in my_files]
    
    # Extract digit of parquet 
    return my_files

def extract_number(phrase_to_find: str) -> list:
    """
    Extract the numeric portion of a file path.
    """
    files = find_files(phrase_to_find)
    all_file_numbers = []
    for file in files:
    # https://stackoverflow.com/questions/11339210/how-to-get-integer-values-from-a-string-in-python
        file_number = "".join(i for i in file if i.isdigit())
        all_file_numbers.append(file_number)
    return all_file_numbers 

def chunk_dask_df(gdf) -> list:
    """
    Break up dataframes by a certain
    number of rows, turn them into a dask
    dataframe

    Args:
        gdf: the local roads that intersect w/ stops and routes
        chunk_row_size(int): how many rows each dataframe should
        be after splitting it out.

    Returns:
        List of dask dataframes. Length of how many dask dataframes
        are returned after cutting.
    """
   # Turn sjoin local roads to dask
    ddf1 = dd.from_pandas(gdf, npartitions=1)

    # Partition the sjoin stuff automatically
    ddf1_partitioned = ddf1.repartition(partition_size="1MB")
    
    #Save out to GCS
    ddf1_partitioned.to_parquet(f"{SHARED_GCS}partitioned_tiger", overwrite = True)
    
    # Read back all the partitioned stuff - grab the file number
    #part0.parquet, part1.parquet
    file_names_dask = extract_number("part")
    
    # https://www.geeksforgeeks.org/read-multiple-csv-files-into-separate-dataframes-in-python/
    # create empty list
    dataframes_list = []
 
    # append datasets into the list
    for i in range(len(file_names_dask)):
        gcs_file_path = f"{SHARED_GCS}partitioned_tiger/part."
        temp_df = dg.read_parquet(f"{gcs_file_path}{file_names_dask[i]}.parquet")
        dataframes_list.append(temp_df)
        
    return dataframes_list

def cut_geometry_compute(dask_dataframe_list:list) -> gpd.GeoDataFrame:
    # Cut geometry
    print("Cut geometry")
    cut_results = []
    for ddf in dask_dataframe_list:
        cut_geometry = delayed(geography_utils.cut_segments)(ddf, ["linearid", "fullname"], 1_000)
        cut_results.append(cut_geometry)
        
    print(f"Begin computing")
    # Compute 
    cut_results = [compute(i)[0] for i in cut_results]
    cut_df = pd.concat(cut_results, axis=0).reset_index(drop=True)
    
    return cut_df

def cut_local_roads(gdf, date:str) -> gpd.GeoDataFrame:
    """
    Cut all the local roads.
    
    gdf: the local roads to cut
    """
    start = datetime.datetime.now()
    print(f"Begin cutting local roads")

    # Divide the gdf into equal sized chunks (roughly)
    # and turn them into dask gdfs
    print("Split into mulitple parquets")
    ddf_list = chunk_dask_df(gdf)
    
    cut_df = cut_geometry_compute(ddf_list)

    file_date = date.replace('-','_')
    cut_df.to_parquet(f"{SHARED_GCS}segmented_local_rds_{file_date}.parquet")
    
    end = datetime.datetime.now()
    print(f"Done cutting local roads in {end-start} minutes")
    return cut_df

def monthly_linearids(date:str, last_month_segmented_local_roads: str) -> gpd.GeoDataFrame:
    """
    Instead of re-cutting all local roads found from the last run,
    only cut the new local roads that are found. 
    Delete out any local roads that aren't found in 
    this month's routes. 
    
    Args:
        date: analysis_date
        last_month_segmented_local_roads: file name of last month's local roads that
        have been cut. Don't include .parquet.
    """
    start = datetime.datetime.now()
    print(f"Start: {start}")
    
    # Sjoin this month's data to tiger roads
    this_month_gdf = sjoin_local_roads(date)
    
    # Find this month's linearids
    this_month_linearid = set(this_month_gdf.linearid.unique().tolist())
    
    # Grab last month's results that have already been cut
    last_month_gdf = gpd.read_parquet(f"{SHARED_GCS}{last_month_segmented_local_roads}.parquet")
    last_month_linearid = set(last_month_gdf.linearid.unique().tolist())
    
    # Have to cut linear ids that appear in this month but not last month
    linearids_to_cut = list(this_month_linearid - last_month_linearid)
    print(f"There are {len(linearids_to_cut)} new linear ids found this month that didn't appear last month.")
    
    # Have to delete linear ids that appear in last month but not this month.
    linearids_to_delete = list(last_month_linearid - this_month_linearid)
    print(f"There are {len(linearids_to_delete)} that didn't appear this month that will be deleted.")
    
    # Filter out linear ids that are no longer relevant to this month
    cut_linearid_1= last_month_gdf.loc[~last_month_gdf.linearid.isin(linearids_to_delete)].reset_index(drop = True)
    
    # Cut the linearids that are only found in this month
    cut_linearid_2 = this_month_gdf.loc[this_month_gdf.linearid.isin(linearids_to_cut)].reset_index(drop = True)
    cut_linearid_2 = cut_local_roads(cut_linearid_2, date)
    
    # Compare lengths of last versus this month's local roads
    this_month_local_roads = pd.concat([cut_linearid_1, cut_linearid_2], axis = 0)
    this_month_len = this_month_local_roads.geometry.length.sum()
    last_month_len = last_month_gdf.geometry.length.sum()
    print(f"This month's local roads length: {this_month_len} meters. Last month: {last_month_len} meters. Diff: {last_month_len-this_month_len} meters")
    
    # Read in primary & secondary roads that have already been cut
    primary_secondary = gpd.read_parquet(f"{SHARED_GCS}segmented_primary_secondary_roads.parquet")
    
    # Concat everything
    this_month_segmented = pd.concat([cut_linearid_1, cut_linearid_2, primary_secondary],axis=0)
    
    # Save
    file_date = date.replace('-','_')
    this_month_segmented.to_parquet(f"{SHARED_GCS}segmented_all_roads_{file_date}.parquet")
    
    end = datetime.datetime.now()
    print(f"Done: {end-start}")
    return this_month_segmented

"""
Primary/Secondary Roads
"""
def cut_primary_secondary_roads() -> gpd.GeoDataFrame:
    start = datetime.datetime.now()
    print(f"Cutting primary/secondary roads")

    # Find all primary and secondary roads
    # regardless of intersection w/ GTFS shapes
    primary_secondary_mtfcc = ["S1100", "S1200"]
    primary_secondary_roads = load_roads(primary_secondary_mtfcc)

    segments = geography_utils.cut_segments(
        primary_secondary_roads, ["linearid", "fullname"], 1_000  # 1 km segments
    )

    segments.to_parquet(f"{SHARED_GCS}segmented_primary_secondary_roads.parquet")

    end = datetime.datetime.now()
    print(f"Done cutting primary & secondary roads: {end-start}")
    return segments


"""
Segment everything
"""
def cut_all_roads(date:str) -> gpd.GeoDataFrame:
    """
    Cut all roads: primary, secondary, and primary roads
    that overlap with bus routes.
    
    Takes about 1.5 hours.
    date (str): analysis date
    """
    start = datetime.datetime.now()
    print(f"Start cutting all roads: {start}")
    # Find local roads that intersect  with GTFS shapes, then
    # segment them
    local_roads_unsegmented = sjoin_local_roads(date)
    local_roads_gdf = cut_local_roads(local_roads_unsegmented, date)

    # Segment primary and secondary roads
    segmented_primary_secondary_rds = cut_primary_secondary_roads()

    # Concat
    file_date = date.replace('-','_')
    all_roads = pd.concat([segmented_primary_secondary_rds, local_roads_gdf], axis=0)
    all_roads.to_parquet(f"{SHARED_GCS}segmented_all_roads_{file_date}.parquet")

    end = datetime.datetime.now()
    print(f"Time lapsed for cutting all roads: {end-start}")
    
    
def cut_all_or_month(date:str, 
                     last_month_segmented_local_roads: str, 
                     run_monthly:bool = True):
    """
    Cut only new roads and delete
    out old roads that are found for a particular month 
    OR cut either all the roads again (primary, secondary, local).
    
    Args:
        date (str): analysis date
        
        last_month_segmented_local_roads (str): file path in GCS to the parquet
        containing last month's segmented local roads
        
        run_monthly (bool): determine whether to cut only most
        recent month or everything.
    """
    if run_monthly:
        gdf1 = monthly_linearids(date, last_month_segmented_local_roads)
    else:
        gdf2 = cut_all_roads(date)    

# CD to rt_segment_speeds -> pip install -r requirements.txt
if __name__ == '__main__': 
    DATE = '2023-04-12'
    LAST_MONTH_FILE = "segmented_all_roads_2023_03_15"
    run_monthly = True
    df = cut_all_or_month(DATE, LAST_MONTH_FILE, run_monthly)

    
"""
Tiffany's Stuff
def compare_roads_to_shapes(analysis_date: str) -> dd.DataFrame:
    roads = dg.read_parquet(
         f"{SHARED_GCS}all_roads_2020_state06.parquet", 
         filters = [[("MTFCC", "==", "S1400")]],
         columns = ["LINEARID", "geometry"]
    ).to_crs(geography_utils.CA_NAD83Albers)
    
    roads = roads.assign(
        geometry = roads.geometry.buffer(200)
    )
    
    shapes = helpers.import_scheduled_shapes(analysis_date).compute()
    
    roads_intersect_with_shapes = dg.sjoin(
        roads,
        shapes[["geometry"]],
        how = "inner",
        predicate = "intersects"
    )[["LINEARID"]].drop_duplicates().reset_index(drop=True)
        
    return roads_intersect_with_shapes


def chunk_df(df: gpd.GeoDataFrame, n: int) -> list:
 
    # Cut df into chunks

    # https://stackoverflow.com/questions/33367142/split-dataframe-into-relatively-even-chunks-according-to-length
    list_df = [delayed(df[i:i+n]) for i in range(0, df.shape[0], n)]

    return list_df 


if __name__ == "__main__":
    start = datetime.datetime.now()
    
    road_categories = [
        "S1100", # primary road
        "S1200", # secondary road
        #"S1400" # local neighborhood road
    ]
        
    primary_secondary = gpd.read_parquet(
        f"{SHARED_GCS}all_roads_2020_state06.parquet", 
        filters = [[("MTFCC", "in", road_categories)]]
    ).to_crs(geography_utils.CA_NAD83Albers)
    
    print(f"# rows in primary/secondary: {len(primary_secondary)}")
    
    local_roads_intersect_shapes = compare_roads_to_shapes(analysis_date)
    
    time1 = datetime.datetime.now()
    print(f"get local subset: {time1-start}")
    
    local = gpd.read_parquet(
        f"{SHARED_GCS}all_roads_2020_state06.parquet", 
    ).to_crs(geography_utils.CA_NAD83Albers)
    
    local = dd.merge(
        local_roads_intersect_shapes,
        local,
        on = "LINEARID",
        how = "inner"
    ).compute()
    
    print(f"# rows in local: {len(local)}")
    
    gdf = pd.concat([primary_secondary, local], axis=0)
    
    time2 = datetime.datetime.now()
    print(f"concat df for segmenting: {time2-time1}")
    
    segments = geography_utils.cut_segments(
        gdf,
        ["LINEARID", "FULLNAME"],
        1_000 # 1 km segments
    )
     
    segments.to_parquet("./census_road_segments.parquet")
    end = datetime.datetime.now()
    print(f"cut segments: {end-time2}")
    print(f"execution time: {end-start}")
"""