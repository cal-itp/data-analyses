"""
Draw buffers around clipped areas
and find all the stops that fall within those bus intersections.

From combine_and_visualize.ipynb
"""
import dask.dataframe as dd
import dask_geopandas
import geopandas as gpd
import numpy as np
import shapely.geometry as sh

import B1_bus_corridors as bus_corridors
from shared_utils import utils, geography_utils
from utilities import catalog_filepath

# Input files
COMBINED_CLIPPED = catalog_filepath("combined_clipped_intersections")
ALL_BUS = catalog_filepath("all_bus")


def process_clipped_intersections():
    gdf = dask_geopandas.read_parquet(COMBINED_CLIPPED)
    
    # Draw a buffer around the intersections
    # to better catch stops that might fall within it
    gdf = gdf.assign(
        geometry = gdf.geometry.buffer(50)
    )
    
    # utils.geoparquet_gcs_export(gdf, 
    # f'{bus_corridors.TEST_GCS_FILE_PATH}', 'major_bus_stops_working')
    
    gdf2 = gdf.assign(
        # need to use tuple to assign a name to this new series (called geom here)
        # and the dtype, which is geometry
        geometry = gdf.geometry.apply(drop_big_areas, meta=("geom", 'geometry'))
    ).dropna(subset="geometry").reset_index(drop=True)
    
    return gdf 


## TODO: pick some examples to see this is working as expected
## depending on the ordering of the points, is it getting rid of too much?
## 3/4 of the rows are dropped
# Drop big areas in order to narrow down to orthogonal intersections
# Intersections where 2 bus routes are traveling down the same path don't count
def drop_big_areas(geometry):
    LENGTH = 1_000
    if isinstance(geometry, sh.multipolygon.MultiPolygon):
        filtered = [x for x in list(geometry.geoms) if x.length < LENGTH]
        if len(filtered) > 0:
            return sh.MultiPolygon(filtered)
    elif isinstance(geometry, sh.polygon.Polygon):
        if geometry.length < LENGTH:
            return geometry
    else:
        return np.nan
    
    
def merged_clipped_geom_to_highest_trip_stop(clipped_df):    
    # For hqta_segment level data, only 1 stop is attached to each segment
    # It's the stop with the highest trip count
    hqta_segment = dask_geopandas.read_parquet(ALL_BUS)
    
    keep_cols = ["calitp_itp_id", "hqta_segment_id", 
             "stop_id", "hq_transit_corr"]

    stops_in_bus_intersections = dd.merge(
        # Put clipped geometries on the left for merge
        # Keep the clipped geom, don't want the full hqta_segment geom
        clipped_df,
        hqta_segment[keep_cols],
        on = ["calitp_itp_id", "hqta_segment_id"],
        how = "inner"
    )
    
    # Add the hqta type for these stops
    # These stops have the highest trip count to 
    # designate this corridor as high quality
    stops_in_bus_intersections = stops_in_bus_intersections.assign(
        hqta_type = "major_transit_stop",
    )
    
    # Change to gdf because gdf.explode only works with geopandas, not dask gdf
    return stops_in_bus_intersections.compute()


## TODO: Need a way to identify from multipolygon to polygon, how to distinguish
# each row. Is intersection_segment descriptive enough?
def explode_geometries(gdf):
    # Explode so that the multipolygon becomes multiple rows of polygons
    # Each polygon contains one clipped area
    # Explode works with dask dataframe, but not dask gdf
    one_intersection_per_row = gdf.explode(ignore_index=True)
    
    one_intersection_per_row = one_intersection_per_row.assign(
        intersection_segment = one_intersection_per_row.groupby(
            ["calitp_itp_id", "hqta_segment_id"]).cumcount() + 1,
        hqta_type = "major_transit_stop",
    )

    return one_intersection_per_row
    
    
if __name__ == "__main__":
    
    # all_clipped only has hqta_segment_id
    # Drop the clipped segments that are not orthogonal 
    clipped = process_clipped_intersections()
    
    # all_clipped only has the hqta_segment_id
    # Merge it back into the hqta_segment data to re-attach the stop_id with the
    # highest trip count 
    stops_in_bus_intersections = merged_clipped_geom_to_highest_trip_stop(clipped)
    
    # Explode geometries so that the multipolygon that contains
    # the intersections each become their own row, holding polygons
    #one_intersection_per_row = explode_geometries(stops_in_bus_intersections)
    
    ## TODO: at what unit is hqta_type == major_transit_stop? what is associated geometry?
    
    #utils.geoparquet_gcs_export(one_intersection_per_row, 
    #                            f'{bus_corridors.TEST_GCS_FILE_PATH}', 'major_bus_stops')