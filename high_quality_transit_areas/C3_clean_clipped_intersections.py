"""
Draw buffers around clipped areas
and find all the stops that fall within those bus intersections.

From combine_and_visualize.ipynb
"""
import dask_geopandas as dg

from utilities import catalog_filepath

# Input files
COMBINED_CLIPPED = catalog_filepath("combined_clipped_intersections")


def process_clipped_intersections() -> dg.GeoDataFrame: 
    """
    Drop some of the big areas in the clipped results,
    which capture too much beyond an intersection of 2 orthogonal bus routes.
    """
    gdf = dg.read_parquet(COMBINED_CLIPPED)
    
    # Draw a buffer around the intersections
    # to better catch stops that might fall within it
    gdf = gdf.assign(
        geometry = gdf.geometry.buffer(50)
    )

    return gdf 