"""
Take single operator HQTA and combine across operators.

Additional data cleaning to filter out small HQTA segments.
"""
import dask.dataframe as dd
import dask_geopandas
import datetime as dt
import geopandas as gpd

import B1_bus_corridors as bus_corridors
from shared_utils import utils


def clean_combined_operators(gdf):    
    gdf = dask_geopandas.from_geopandas(gdf, npartitions=1)
    
    # Drop segments that are large enough
    gdf = gdf.assign(
        area = gdf.geometry.area 
    )
    
    gdf2 = gdf[gdf.area > 50*400].compute()  ##50m width * 400m segment min
    
    dissolved = gdf2.dissolve(
        by=["calitp_itp_id", "route_id", "hq_transit_corr"]).reset_index()

    dissolved = dissolved.assign(
        area = dissolved.geometry.area
    )
    
    dissolved2 = (dissolved[dissolved.area > 50*3_000] ##50m width * 3000m shape min
                 .reset_index(drop=True)
                 )

    return dissolved2 

# Read first one in, to set the metadata for dask gdf
OPERATOR_PATH = f"{bus_corridors.TEST_GCS_FILE_PATH}bus_corridors/"
first_operator = bus_corridors.ITP_IDS_IN_GCS[0]

if __name__ == "__main__":
    start = dt.datetime.now()
    
    # Read in first operator to set the metadata for dask gdf
    gdf = dask_geopandas.read_parquet(
        f'{OPERATOR_PATH}{first_operator}_bus.parquet'
    )

    for itp_id in bus_corridors.ITP_IDS_IN_GCS[1:]:
        operator = dask_geopandas.read_parquet(
            f'{OPERATOR_PATH}{itp_id}_bus.parquet')

        gdf = dd.multi.concat([gdf, operator], axis=0)
    
    print("Concatenated all operators")
    
    # Compute to make it a gdf
    gdf2 = gdf.compute().reset_index(drop=True)
    
    utils.geoparquet_gcs_export(gdf2, 
                                f'{bus_corridors.TEST_GCS_FILE_PATH}intermediate/', 
                                'all_bus')
    
    time1 = dt.datetime.now()
    print(f"Exported all_bus to GCS: {time1-start}")
    
    gdf3 = clean_combined_operators(gdf2)
    
    utils.geoparquet_gcs_export(gdf3,
                                f'{bus_corridors.TEST_GCS_FILE_PATH}intermediate/',
                                'shape_dissolve'
                               )
    time2 = dt.datetime.now()
    print(f"Exported shape_dissolve to GCS: {time2-time1}")
    
    end = dt.datetime.now()
    print(f"Execution time: {end-start}")