"""
"""
import geopandas as gpd
import pandas as pd

from shared_utils import portfolio_utils, rt_dates
from calitp_data_analysis import geography_utils
from segment_speed_utils.project_vars import COMPILED_CACHED_VIEWS

# LA Metro data is for Oct 2022, so let's use the date we already downloaded
analysis_date = rt_dates.DATES["oct2022"]
PROJECT_CRS = geography_utils.CA_NAD83Albers


def fill_missing_route_short_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    In the trips table, occasionally we're missing route_short_name,
    so we'll fill it in with route_id, 
    otherwise merges to LA Metro's table will not be successful.
    """
    df = df.assign(
        parsed_route_id = df.route_id.str.split('-', expand=True)[0]
    )
    
    df = df.assign(
        route_short_name = df.route_short_name.fillna(df.parsed_route_id)
    ).drop(columns = "parsed_route_id")

    return df


def import_trips() -> pd.DataFrame:
    trips = pd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}_v2.parquet",
        filters = [[("name", "==", "LA Metro Bus Schedule")]],
        columns = ["feed_key", "name", "trip_id", "shape_id", 
                   "shape_array_key", "route_id", 
                   "route_short_name", "route_long_name"]
    )
    
    trips2 = fill_missing_route_short_name(trips)

    return trips2
        

def longest_shape_for_route(
    analysis_date: str,
    trips: pd.DataFrame
) -> gpd.GeoDataFrame:
    """
    Pick 1 shape to represent that route and 
    join against CalEnviroScreen tracts.
    """
    shapes = gpd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}routelines_{analysis_date}_v2.parquet", 
        columns = ["shape_array_key", "geometry"]
    ).to_crs(PROJECT_CRS)

    shapes_with_route = pd.merge(
        shapes,
        trips,
        on = "shape_array_key",
        how = "inner"
    )
        
    shapes_with_route = shapes_with_route.assign(
        route_length = shapes_with_route.geometry.length
    )
    
    longest_shape = (shapes_with_route.sort_values(
            ["name", "route_id", "route_length"], 
            ascending=[True,True, False])
            .drop_duplicates(subset=["route_id"])
    )[["name", "route_id", "route_length", 
       "shape_array_key", "geometry"]].reset_index(drop=True)
    
    
    return longest_shape


def split_route_into_separate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    In GTFS, some routes are coded together, like `10/48`, 
    and the combined trips / shapes for that route.
    Let's just split the trips in half from GTFS, because we 
    care more about percent cash than what's in GTFS.
    """
    no_slash = df[~df.route_short_name.str.contains("/")].reset_index(drop=True)
    has_slash = df[df.route_short_name.str.contains("/")].reset_index(drop=True)
    
    duplicated = (pd.concat([has_slash]*2, ignore_index=True)
                      .sort_values("route_short_name")
                      .reset_index(drop=True)
                     )

    duplicated = duplicated.assign(
        obs = duplicated.groupby("route_short_name").cumcount() + 1
    )
    
    duplicated = duplicated.assign(
        route_short_name_new = duplicated.apply(
            lambda x: x.route_short_name.split('/')[0] if x.obs ==1 
            else x.route_short_name.split('/')[1], axis=1),  
    ).drop(columns = ["obs", "route_short_name"]).rename(
        columns = {"route_short_name_new": "route_short_name"})
    
    cleaned_df = pd.concat(
        [no_slash, 
         duplicated], axis=0
    )
    
    # drop this row that has an error
    # it has route_id == 224, but route_short_name == 690
    # it contains duplicate info as row with route_id == 224 and 
    # route_short_name == 224
    cleaned_df = cleaned_df[~(
        (cleaned_df.route_id.str.contains("224")) & 
        (cleaned_df.route_short_name == "690"))
    ].reset_index(drop=True)
    
    return cleaned_df


def recode_route_for_shape(df: pd.DataFrame) -> pd.DataFrame:
    """
    There are some route_ids that are present in LA Metro's 
    dataset. Rather than losing it from not finding a corresponding
    route_id or route_short_name, Google to see
    which other route shares its shape.
    We cannot fill in service_hours or num_trips, but that's ok.
    """
    DUP_ME_AND_RECODE = {
        "162": "163",
        "51": "52", #770 discontinued, replaced with 70
        "236": "235",
        "260": "762",
        "30": "330",
        "487": "489",
        #-- 134 (existing now, along PCH to downtown SM, can't find)
        #-- 728 (discontinued)
        #-- 201 (discontinued)
    }
    recode_me = df[df.route_short_name.isin(DUP_ME_AND_RECODE.keys())]
    correct = df[~df.route_short_name.isin(DUP_ME_AND_RECODE.keys())]

    duplicated = (pd.concat([recode_me]*2, ignore_index=True)
                  .sort_values("route_short_name")
                  .reset_index(drop=True)
                 )
    
    duplicated = duplicated.assign(
        obs = duplicated.groupby("route_short_name").cumcount() + 1
    )

    duplicated = duplicated.assign(
        route_short_name = duplicated.apply(
            lambda x: DUP_ME_AND_RECODE[x.route_short_name] if x.obs ==2
            else x.route_short_name, axis=1),    
    ).drop(columns = ["obs"])
    
    cleaned_df = pd.concat(
        [correct, 
         duplicated], axis=0
    ).reset_index(drop=True)
    
    return cleaned_df
    
    
def assemble_route_level_data():
    
    trips = import_trips()
    
    route_cols = ["route_id", "route_short_name", "route_long_name"]
    route = trips[[
        "name"] + route_cols
    ].drop_duplicates().reset_index(drop=True)

    route2 = split_route_into_separate_rows(route)
    route3 = recode_route_for_shape(route2)
    
    route_shapes = longest_shape_for_route(analysis_date, trips)
    
    routes_with_geom = pd.merge(
        route_shapes,
        route3,
        on = ["name", "route_id"],
        how = "outer",
        validate = "1:m", 
        #m on right because we duplicated some route_ids to match LA Metro's dataset
        indicator=True
    )
    
    # Merge in number of trips, but don't assign values to the rows we 
    # duplicated. We don't want n_trips to be overinflated 
    trips_by_route = portfolio_utils.aggregate_by_geography(
        trips,
        group_cols = ["name"] + route_cols,
        nunique_cols = ["trip_id"],
        rename_cols = True
    )
    
    routes_with_geom2 = pd.merge(
        routes_with_geom,
        trips_by_route,
        on = ["name"] + route_cols,
        how = "left",
    )
    
    return routes_with_geom2