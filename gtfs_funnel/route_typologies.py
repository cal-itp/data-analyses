"""
For the most common shape for each route-direction,
apply some definitions for NACTO route typologies
and service frequency to roads.

Do our best at assigning scores to road segments
across multiple operators. We'll take the aggregate
stop arrivals and calculate an overall frequency
for that segment.

https://nacto.org/publication/transit-street-design-guide/introduction/service-context/transit-route-types/

https://nacto.org/publication/transit-street-design-guide/introduction/service-context/transit-frequency-volume/.

Once the 2 mile road segments are categorized, find the intersection
of shapes to buffered roads.
For each shape, get a percent distribution for each combo
of service frequency and route typology.

~1-1.5 min per date
"""
import datetime
import geopandas as gpd
import pandas as pd

from dask import delayed, compute

from segment_speed_utils import gtfs_schedule_wrangling, helpers                       
from segment_speed_utils.project_vars import PROJECT_CRS   
from shared_utils import rt_dates
from shared_utils.gtfs_utils_v2 import RAIL_ROUTE_TYPES
from update_vars import SHARED_GCS, SCHED_GCS, GTFS_DATA_DICT

route_dir_cols = [
    "schedule_gtfs_dataset_key", 
    "route_id", "direction_id", 
    "common_shape_id", "route_name", "route_meters"
]

typology_cols = ["freq_category", "typology"]
road_cols = ["linearid", "mtfcc", "fullname"]
road_segment_cols = road_cols + ["segment_sequence"]

route_typologies = [
    "downtown_local", "local", "coverage",
    "rapid", "express", "rail", "ferry"
]

def categorize_routes_by_name(
    analysis_date: str,
    route_typology_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Look at how operator describes route (route_short_name,
    route_long_name) and tag express / rapid / local / rail routes.
    """
    # Aready have route_type so let's merge in the route name columns
    route_name_df = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", "name", 
                   "shape_id", 
                   "route_long_name", "route_short_name"],
        get_pandas = True
    ).rename(columns = {"shape_id": "common_shape_id"})
    
    df = pd.merge(
        route_typology_df,
        route_name_df,
        on = ["schedule_gtfs_dataset_key", "common_shape_id"],
        how = "inner"
    )
    
    # Fill in missing values
    df = df.assign(
        route_id = df.route_id.fillna(""),
        route_short_name = df.route_short_name.fillna(""),
        route_long_name = df.route_long_name.fillna(""),
    )

    df = df.assign(
        combined_name = df.route_short_name + "__" + df.route_long_name
    )
    
    typology_tags = df.apply(
        lambda x: tag_rapid_express_rail_ferry(
            x.combined_name, x.route_type), axis=1
    )
    
    df2 = pd.concat([df, typology_tags], axis=1)

    df2 = df2.assign(
        is_local = df2.apply(
            lambda x: 
            1 if (x.is_express==0) and (x.is_rapid==0) and 
            (x.is_rail==0) and (x.is_ferry==0)
            else 0, axis=1).astype(int)
    )
    
    return df2

def tag_rapid_express_rail_ferry(
    route_name: str, route_type: str
) -> pd.Series:
    """
    Use the combined route_name and see if we can 
    tag out words that indicate the route is
    express, rapid, local, and rail.
    
    Treat rail as own category.
    For local routes, we'll pass that through NACTO to see
    if we can better categorize as downtown_local, local, or coverage.
    """
    route_name_lower = route_name.lower()
    
    express = 0
    rapid = 0
    rail = 0
    ferry = 0
    
    if any(substring in route_name_lower for substring in 
           ["express", "limited"]):
        express = 1
    if "rapid" in route_name_lower:
        rapid = 1
    
    if route_type in RAIL_ROUTE_TYPES:
        rail = 1
    
    if route_type == "4":
        ferry = 1
    
    return pd.Series(
            [express, rapid, rail, ferry], 
            index=['is_express', 'is_rapid', 'is_rail', 'is_ferry']
        )


def nacto_peak_frequency_category(freq_value: float) -> str:
    """
    Assign peak frequencies into categories.
    Be more generous, if there are overlapping
    cutoffs for categories, we'll use the lower value
    so transit route / road can achieve a better score.
    
    Source: https://nacto.org/publication/transit-street-design-guide/introduction/service-context/transit-frequency-volume/
    """
    # Set the upper bounds here 
    low_cutoff = 4
    mod_cutoff = 10
    high_cutoff = 20
    
    if freq_value < low_cutoff:
        return "low"
    elif freq_value >= low_cutoff and freq_value < mod_cutoff:
        return "moderate"
    elif freq_value >= mod_cutoff and freq_value < high_cutoff:
        return "high"
    elif freq_value >= high_cutoff:
        return "very_high"

    
def nacto_stop_frequency(
    stop_freq: float, 
    service_freq: str
) -> str:
    """
    Assign NACTO route typologies.
    Be more generous, if there are overlapping
    cutoffs for categories, we'll use the lower value
    so transit route / road can achieve a better score.
    
    https://nacto.org/publication/transit-street-design-guide/introduction/service-context/transit-route-types/
    """
    cut1 = 3
    cut2 = 4
    mod_high = ["moderate", "high"]
    
    if stop_freq >= cut2:
        return "downtown_local"
    
    elif (stop_freq >= cut1 and 
          stop_freq < cut2 and 
          service_freq in mod_high
         ):
        return "local"
    
    elif (stop_freq >= 1 and stop_freq < cut1 and 
          service_freq in mod_high):
        return "rapid"
    
    elif service_freq == "low":
        #(stop_freq >= 2 and stop_freq < 8
        return "coverage"
    
    # last category is "express", which we'll have to tag on 
    # the route name side

    
def categorize_non_bus_typologies(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    For non-bus typologies, we'll use route_type and tag it.
    These do not need to be filtered against a threshold (since it's likely
    that rail tracks are not roads, so they'd get filtered out too aggressively).
    """
    route_dir_sort_cols = [
        "schedule_gtfs_dataset_key", 
        "route_id", "direction_id", 
        "route_name", "route_type"
    ]

    df2 = df.sort_values(
        route_dir_sort_cols + ["pct_typology"],
        ascending = [True for c in route_dir_sort_cols] + [False]
    ).drop_duplicates(
        subset = route_dir_sort_cols
    )
    
    typology_tags = df2.apply(
        lambda x: tag_rapid_express_rail_ferry(
            x.route_name, x.route_type), axis=1
    )
    
    df3 = pd.concat([df2, typology_tags], axis=1)  
    
    return df3

    
def prep_roads(dict_inputs: dict) -> gpd.GeoDataFrame:
    """
    Uses aggregated sjoin to count stop arrivals on roads
    from stop_arrivals_in_roads.py.
    Assigns service frequency to road segment (across operators).
    """
    road_stats = pd.read_parquet(
        f"{SCHED_GCS}arrivals_by_road_segment.parquet"
    )

    ROAD_SEGMENTS = dict_inputs.shared_data.road_segments_twomile
    
    roads = gpd.read_parquet(
        f"{SHARED_GCS}{ROAD_SEGMENTS}.parquet",
        columns = road_segment_cols + ["geometry"],
    ).to_crs(PROJECT_CRS)
    
    road_stats = road_stats.assign(
        freq_category = road_stats.apply(
            lambda x: nacto_peak_frequency_category(x.frequency), axis=1)
    )
    
    road_stats = road_stats.assign(
        typology = road_stats.apply(
            lambda x: nacto_stop_frequency(
            x.stops_per_mi, x.freq_category), axis=1)
    )
    
    df = pd.merge(
        roads,
        road_stats,
        on = road_segment_cols,
        how = "inner"
    )
    
    return df
    
def overlay_shapes_to_roads(
    roads: gpd.GeoDataFrame,
    analysis_date: str,
    buffer_meters: int
) -> gpd.GeoDataFrame:
    """
    Get overlay of shapes to roads so we can 
    attach road's typology back to transit (shape).
    For the most common shape for each route-direction,
    count the number of meters and percentage of shape's total meters
    that is assigned to each typology to use as plurality later.  
    """
    common_shape = gtfs_schedule_wrangling.most_common_shape_by_route_direction(
        analysis_date
    )
    
    common_shape = common_shape.assign(
        route_meters = common_shape.geometry.length,
    )
        
    # use sjoin first to find where we want to calculate overlay
    s1 = gpd.sjoin(
        roads,
        common_shape,
        how = "inner",
        predicate = "intersects"
    ).drop(columns = ["index_right"]).reset_index(drop=True)
    
    # merge shape geometry back in 
    gdf = pd.merge(
        s1,
        common_shape.rename(columns = {"geometry": "shape_geometry"}),
        on = route_dir_cols,
        how = "inner"
    )
    
    # buffer road segment geom and take overlay
    overlay_geom = gdf.shape_geometry.intersection(
        gdf.geometry.buffer(buffer_meters), align=True)
    
    gdf = gdf.assign(
        overlay_geom = overlay_geom,
        overlay_meters = overlay_geom.length
    )
    
    # Calculate the sum of overlay meters for each typology combo
    gdf2 = (gdf.groupby(route_dir_cols + typology_cols,
                        observed=True, group_keys=False)
       .agg({"overlay_meters": "sum"})
       .reset_index()
    )
    
    # Find the percent of that typology over the total route_meters
    # This can be >1, it's not often, but it can.
    gdf2 = gdf2.assign(
        pct_typology = gdf2.overlay_meters.divide(gdf2.route_meters).round(2)
    )
    
    gdf3 = (gdf2.groupby(route_dir_cols + typology_cols)
            .agg({"pct_typology": "sum"})
            .reset_index()
    )
    
    # Add back route_type
    route_types_by_shape = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", "shape_id", "route_type"],
        get_pandas = True
    ).rename(columns = {"shape_id": "common_shape_id"})
    
    gdf4 = pd.merge(
        gdf3,
        route_types_by_shape,
        on = ["schedule_gtfs_dataset_key", "common_shape_id"],
        how = "inner"
    )
   
    return gdf4  


def primary_secondary_typology(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Instead of leaving combinations with typology-freq_category,
    aggregate by typology and select the top 2.
    """ 
    group_cols = route_dir_cols + ["route_type"]
    
    df2 = (df.groupby(group_cols + ["typology"])
           .agg({"pct_typology": "sum"})
           .reset_index()
          )
    
    df2 = df2.assign(
        obs = (df2
           .sort_values(group_cols + ["pct_typology"], 
                        ascending=[True for i in group_cols] + [False])
           .groupby(group_cols)
           .cumcount() + 1
          )
    )
    
    # Keep primary and secondary typology
    df3 = df2.loc[df2.obs <=2].drop(
        columns = ["pct_typology", "obs"]
    ).reset_index(drop=True)
    
    # Turn the typology column into a dummy variables
    df3 = pd.get_dummies(df3, columns = ["typology"])
    
    # Flag both primary and secondary typology as 1
    # so a route can have multiple dummies turned on
    # allow this so we can just keep one route-dir as a row
    max_cols = [c for c in df3.columns if "typology_" in c]

    df4 = (df3.groupby(group_cols)
           .agg({**{c: "max" for c in max_cols}})
           .reset_index()
           .rename(columns = {c: c.replace('typology', 'is_nacto') for c in max_cols})
          )
    
    return df4


def reconcile_route_and_nacto_typologies(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Let's see if we can sort out local routes into more
    specific NACTO local route types.
    If it's ever flagged as downtown_local or coverage,
    we'll use that.
    """
    df = df.assign(
        is_rapid = df[["is_rapid", "is_nacto_rapid"]].max(axis=1),                              
    ).rename(columns = {
        "is_nacto_downtown_local": "is_downtown_local",
        "is_nacto_coverage": "is_coverage"
    })
    
    # Retain as local if coverage or downtown_local aren't true
    df = df.assign(
        is_local = df.apply(
            lambda x: 1 if ((x.is_coverage==0) and (x.is_downtown_local == 0))
            or (x.is_nacto_local==1)
            else 0, axis=1)
    )
    
    drop_cols = [c for c in df.columns if "is_nacto_" in c]
    
    df2 = df.drop(columns = drop_cols)
    
    integrify = [f"is_{c}" for c in route_typologies]
    df2[integrify] = df2[integrify].astype(int)
    
    return df2


if __name__ == "__main__":
    
    from update_vars import analysis_date_list
    
    EXPORT = GTFS_DATA_DICT.schedule_tables.route_typologies
    
    start = datetime.datetime.now()

    roads = delayed(prep_roads)(GTFS_DATA_DICT)
    ROAD_BUFFER_METERS = 20
    TYPOLOGY_THRESHOLD = 0.09
 
    for analysis_date in analysis_date_list:
        
        time0 = datetime.datetime.now()
             
        gdf = delayed(overlay_shapes_to_roads)(
            roads, analysis_date, ROAD_BUFFER_METERS
        )    
        gdf = compute(gdf)[0]
        
        gdf.to_parquet(
            f"{SCHED_GCS}{EXPORT}_long_{analysis_date}.parquet"
        )        
        
        # Only keep significant typologies, but leave as typology-freq_category, 
        # then aggregate to route-dir for the top 2 frequent typologies
        bus_route_typology_df = gdf.loc[
            (gdf.route_type == "3") & 
            (gdf.pct_typology >= TYPOLOGY_THRESHOLD)
        ].pipe(primary_secondary_typology)
        
        bus_route_typology_df2 = categorize_routes_by_name(
            analysis_date, 
            bus_route_typology_df
        ).pipe(reconcile_route_and_nacto_typologies)
        
        non_bus_route_typology_df = gdf.loc[
            gdf.route_type != "3"
        ].pipe(
            categorize_non_bus_typologies
        )
        
        # Combine bus and non-bus
        # Drop duplicates if there are multiple common_shape_id values
        combined_route_typology = pd.concat(
            [bus_route_typology_df2, non_bus_route_typology_df],
            axis=0, ignore_index=True
        )[route_dir_cols + [
            f"is_{t}" for t in route_typologies]
         ].sort_values(route_dir_cols).drop_duplicates(
            subset = ["schedule_gtfs_dataset_key", "route_id", "direction_id"]
        ).reset_index(drop=True)
        
        combined_route_typology.to_parquet(
            f"{SCHED_GCS}{EXPORT}_{analysis_date}.parquet")
        
        time1 = datetime.datetime.now()
        print(f"route typologies {analysis_date}: {time1 - time0}")
        
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")