"""
Take single operator HQTA and combine.
"""

import dask.dataframe as dd
import dask_geopandas
import datetime as dt
import geopandas as gpd

import B1_bus_corridors as bus_corridors
from shared_utils import utils


# Read first one in, to set the metadata for dask gdf
OPERATOR_PATH = f"{bus_corridors.TEST_GCS_FILE_PATH}bus_corridors/"
first_operator = ITP_IDS_IN_GCS[0]

if __name__ == "__main__":
    start = dt.datetime.now()
    
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
    
    print("Exported to GCS")
    end = dt.datetime.now()
    print(f"Execution time: {end-start}")