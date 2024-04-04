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
from update_vars import SHARED_GCS, SCHED_GCS

route_dir_cols = [
    "schedule_gtfs_dataset_key", 
    "route_id", "direction_id", 
    "common_shape_id", "route_name", "route_meters"
]
typology_cols = ["freq_category", "typology"]
road_cols = ["linearid", "mtfcc", "fullname"]
road_segment_cols = road_cols + ["segment_sequence"]

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

    
def prep_roads(dict_inputs: dict) -> gpd.GeoDataFrame:
    road_stats = pd.read_parquet(
        f"{SCHED_GCS}arrivals_by_road_segment.parquet"
    )

    ROAD_SEGMENTS = dict_inputs.shared_data.road_segments_twomile
    
    roads = gpd.read_parquet(
        f"{SHARED_GCS}{ROAD_SEGMENTS}.parquet"
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
    common_shape = gtfs_schedule_wrangling.most_common_shape_by_route_direction(
        analysis_date
    ).pipe(helpers.remove_shapes_outside_ca)

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
    
    return gdf3  


def primary_secondary_typology(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Instead of leaving combinations with typology-freq_category,
    aggregate by typology and select the top 2.
    """     
    df2 = (df.groupby(route_dir_cols + ["typology"])
           .agg({"pct_typology": "sum"})
           .reset_index()
          )
    
    df2 = df2.assign(
        obs = (df2
           .sort_values(route_dir_cols + ["pct_typology"], 
                        ascending=[True for i in route_dir_cols] + [False])
           .groupby(route_dir_cols)
           .cumcount() + 1
          )
    )
    
    primary_typology = (
        df2.loc[df2.obs==1].rename(
            columns = {
                "typology": "primary_typology",
                "pct_typology": "primary_pct_typology"
            })
        .drop(columns = "obs")
    )
    
    secondary_typology = (
        df2.loc[df2.obs==2]
        .rename(
            columns = {
                "typology": "secondary_typology",
                "pct_typology": "secondary_pct_typology"
            })
        .drop(columns = "obs")
    )
    
    df3 = pd.merge(
        primary_typology,
        secondary_typology,
        on = route_dir_cols,
        how = "left"
    )
    
    return df3


if __name__ == "__main__":
    
    from update_vars import analysis_date_list
    
    EXPORT = GTFS_DATA_DICT.schedule_tables.route_typologies
    
    start = datetime.datetime.now()

    roads = delayed(prep_roads)(GTFS_DATA_DICT)
    ROAD_BUFFER_METERS = 20
    TYPOLOGY_THRESHOLD = 0.10
    
    for analysis_date in analysis_date_list:
        
        time0 = datetime.datetime.now()
        
        gdf = delayed(overlay_shapes_to_roads)(
            roads, analysis_date, ROAD_BUFFER_METERS
        )    
        gdf = compute(gdf)[0]
        
        # Only keep significant typologies, but leave as typology-freq_category
        route_typology_df = gdf.loc[gdf.pct_typology >= TYPOLOGY_THRESHOLD]
        
        route_typology_df.to_parquet(
            f"{SCHED_GCS}{EXPORT}_long_{analysis_date}.parquet")
        
        # Aggregate to route-dir-typology
        route_typology_df2 = primary_secondary_typology(route_typology_df)
        
        route_typology_df2.to_parquet(
            f"{SCHED_GCS}{EXPORT}_{analysis_date}.parquet")
        
        time1 = datetime.datetime.now()
        print(f"route typologies {analysis_date}: {time1 - time0}")
        
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")