import geopandas as gpd
import pandas as pd

from pathlib import Path
from typing import Literal

from calitp_data_analysis import utils
from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import helpers
from service_increase_vars import PUBLIC_GCS, DATA_PATH, dates

def crosswalk_with_identifiers(
    date_list: list
) -> pd.DataFrame:
    """
    Create a file that can be used as a crosswalk for McKinsey
    consultants.
    Let's add more information about route_id, etc,
    since the previous script to expand df by hours 
    is quite large already.
    """
    natural_identifiers = pd.concat([
        helpers.import_scheduled_trips(
            d,
            columns = ["gtfs_dataset_key", "name", 
                       "shape_id", "shape_array_key", 
                       "route_key",
                       "route_id", "route_short_name",
                       "route_long_name", "route_desc"
                      ],
            get_pandas = True,
        ).assign(
            service_date = pd.to_datetime(d)
        ) for d in date_list], 
        axis=0, ignore_index=True
    ).drop_duplicates().reset_index(drop=True)
    
    return natural_identifiers
    

def export_parquet_as_csv_or_geojson(
    filename: str,
    filetype: Literal["df", "gdf"],
    zipped: bool = True
):
    """
    For parquets, we want to export as csv.
    For geoparquets, we want to export as geojson.
    """
    if filetype=="df":
        df = pd.read_parquet(filename)
        
        df.to_csv(
            f"{PUBLIC_GCS}bus_service_increase/"
            f"{Path(filename).stem}.csv",
            index=False
        )
        
        del df
        
        
    elif filetype=="gdf":
        df = gpd.read_parquet(filename).to_crs(WGS84)
        utils.geojson_gcs_export(
            df,
            f"{PUBLIC_GCS}bus_service_increase/",
            Path(filename).stem,
            geojson_type = "geojson"
        )
        
        del df
        
    return

if __name__ == "__main__":
    
    all_dates = list(dates.values())
    
    crosswalk = crosswalk_with_identifiers(all_dates)
    crosswalk.to_csv(
        f"{PUBLIC_GCS}bus_service_increase/"
        f"gtfs_crosswalk.csv", index=False
    )
    
    export_parquet_as_csv_or_geojson(
        f"{DATA_PATH}shapes_processed.parquet", "df")
    
    export_parquet_as_csv_or_geojson(
        f"{DATA_PATH}shapes_categorized.parquet", "gdf")
    

    
