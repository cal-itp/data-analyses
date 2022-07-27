"""
Draw buffers around clipped areas
and find all the stops that fall within those bus intersections.

From combine_and_visualize.ipynb
"""
import dask_geopandas
import geopandas as gpd
import numpy as np
import shapely.geometry as sh

from B1_bus_corridors import TEST_GCS_FILE_PATH
from utilities import catalog_filepath

# Input files
COMBINED_CLIPPED = catalog_filepath("combined_clipped_intersections")


def process_clipped_intersections():
    gdf = dask_geopandas.read_parquet(COMBINED_CLIPPED)
    
    # Draw a buffer around the intersections
    # to better catch stops that might fall within it
    gdf = gdf.assign(
        geometry = gdf.geometry.buffer(50)
    )
    
    # utils.geoparquet_gcs_export(gdf, 
    # f'{TEST_GCS_FILE_PATH}', 'major_bus_stops_working')
    
    gdf2 = gdf.assign(
        # need to use tuple to assign a name to this new series (called geom here)
        # and the dtype, which is geometry
        geometry = gdf.geometry.apply(drop_big_areas, meta=("geom", 'geometry'))
    ).dropna(subset="geometry").reset_index(drop=True)
    
    return gdf 


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
   