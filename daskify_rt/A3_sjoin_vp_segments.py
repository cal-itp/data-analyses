"""
Skip the linear reference until sjoin to segments is done.
If linear reference is to derive distance elapsed between
two timestamps, then can we reference it against the segment?

We only need the enter/exit timestamps, rather than between each
point within a segment.

But, what to do with a segment that only has 1 point? 
It would still need a calculation derived, as an average, across the 
previous segment? or the post segment? But at that point, it 
can take the average for speed_mph...may not necessarily need
a shape_meters calculation?

https://stackoverflow.com/questions/24415806/coordinates-of-the-closest-points-of-two-geometries-in-shapely

https://github.com/dask/dask/issues/8042
"""

import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import pandas as pd
import geopandas as gpd

from dask import delayed, compute

from shared_utils import utils, geography_utils

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
DASK_TEST = f"{GCS_FILE_PATH}dask_test/"
analysis_date = "2022-10-12"
  

def sjoin_vehicle_positions_to_segments(
    itp_id: int, analysis_date: str, buffer_size: int = 50) -> dd.DataFrame:
    """
    Spatial join vehicle positions for an operator
    to buffered route segments.
    
    Returns a dd.DataFrame. geometry seems to make the 
    compute extremely large. 
    Do more aggregation at segment-level before bringing 
    point geom back in for linear referencing.
    """
        
    vp = dg.read_parquet(
        f"{DASK_TEST}vp_{analysis_date}.parquet").to_crs(
        geography_utils.CA_NAD83Albers)
    vp = vp[vp.calitp_itp_id==itp_id]
    
    segments = dg.read_parquet(f"{DASK_TEST}longest_shape_segments.parquet")
    segments = segments[segments.calitp_itp_id==itp_id]
    
    segments_buff = segments.assign(
        geometry = segments.geometry.buffer(buffer_size)
    )
    
    segment_cols = ["route_dir_identifier", "segment_sequence"]    

    vp_to_seg = dg.sjoin(
        vp,
        segments_buff[
            segment_cols + ["geometry"]],
        how = "inner",
        predicate = "within"
    ).drop(columns = "index_right").drop_duplicates()
    
    # Drop geometry and return a df...eventually,
    # can attach point geom back on, after enter/exit points are kept
    # geometry seems to be a big issue in the compute
    vp_to_seg = vp_to_seg.assign(
        lon = vp_to_seg.geometry.x,
        lat = vp_to_seg.geometry.y,
    )
    
    drop_cols = ["geometry", "vehicle_id", "entity_id"]
    ddf = vp_to_seg.drop(columns = drop_cols)
    
    return ddf
        

def compute_and_export(df_delayed):
    df = compute(df_delayed)[0]
    
    itp_id = df.calitp_itp_id.unique().compute()[0]
    
    df.to_parquet(
        f"{DASK_TEST}vp_sjoin/vp_segment_{itp_id}_{analysis_date}.parquet")
    
    import_ddf = dd.read_parquet(
        f"{DASK_TEST}vp_sjoin/vp_segment_{itp_id}_{analysis_date}.parquet"
    )
    
    import_ddf.compute().to_parquet(
        f"{DASK_TEST}vp_sjoin/vp_segment_{itp_id}_{analysis_date}.parquet"
    )
    
    #TODO: need to figure out how to delete the directory version with 
    # part0, part1
    
    
if __name__ == "__main__":
    start = datetime.datetime.now()
    vp = dg.read_parquet(
        f"{DASK_TEST}vp_{analysis_date}.parquet")
    
    ITP_IDS = vp.calitp_itp_id.unique()
    
    results = []
    
    for itp_id in ITP_IDS:
        start_id = datetime.datetime.now()
        
        vp_to_segment = delayed(sjoin_vehicle_positions_to_segments)(
            itp_id, analysis_date, buffer_size=50)
        
        results.append(vp_to_segment)
        
        end_id = datetime.datetime.now()
        print(f"{itp_id}: {end_id-start_id}")
        
    time1 = datetime.datetime.now()

    # For each item in the list of delayed objects,
    # turn it into a ddf and export
    [compute_and_export(i) for i in results]

