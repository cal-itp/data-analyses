"""
Cut road segments.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd

#from dask import delayed, compute

from shared_utils import dask_utils, geography_utils, utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import analysis_date

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
SHARED_GCS = f"{GCS_FILE_PATH}shared_data/"


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
    """
    Cut df into chunks
    """
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