import geopandas as gpd
import pandas as pd

from pathlib import Path
from typing import Literal
from service_increase_vars import PUBLIC_GCS, DATA_PATH

def export_parquet_as_csv_or_geojson(
    filename: str,
    filetype: Literal["df", "gdf"],
):
    """
    For parquets, we want to export as csv.
    For geoparquets, we want to export as geojson.
    """
    if filetype=="df":
        df = pd.read_parquet(filename)
        df.to_csv(
            f"{PUBLIC_GCS}bus_service_increase/"
            f"{Path(filename).stem}.csv", index=False
        )
        
        
    elif filetype=="gdf":
        df = gpd.read_parquet(filename)
        utils.geojson_gcs_export(
            df,
            f"{PUBLIC_GCS}bus_service_increase/",
            Path(filename).stem,
            geojson_type = "geojson"
        )
        
    return

if __name__ == "__main__":
    
    export_parquet_as_csv_or_geojson("shapes_processed", "df")
    
    export_parquet_as_csv_or_geojson("shapes_categorized", "gdf")
