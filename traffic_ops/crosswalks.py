import pandas as pd
import uuid

from utils import RAW_GCS, PROCESSED_GCS

station_id_cols = [
    'station_id',
    'freeway_id',
    'freeway_dir',
    'city_id',
    'county_id',
    'district_id',
    'station_type',
    'param_set', 
    #'length',
    #'abs_postmile', 
    #'physical_lanes'
]

def create_station_crosswalk(filename: str = "hov_portion") -> pd.DataFrame:
    """
    Put in a set of columns that identify the 
    station + freeway + postmile position.
    Create a uuid that we can use to get back all 
    the columns we may want later.
    """
    df = pd.read_parquet(
        f"{RAW_GCS}{filename}",
        columns = station_id_cols
    ).drop_duplicates().reset_index(drop=True)
      
    df["station_uuid"] = df.apply(
        lambda _: str(uuid.uuid4()), axis=1, 
    )
      
    df.to_parquet(
        f"{PROCESSED_GCS}station_crosswalk.parquet"
    )
    
    return


def create_station_detector_crosswalk(
    filename: str = "hov_portion_detector_status_time_window"
):
    df = pd.read_parquet(
        f"{RAW_GCS}{filename}",
        columns = ["station_id", "detector_id"]
    ).drop_duplicates().reset_index(drop=True)
    
    df.to_parquet(
        f"{PROCESSED_GCS}station_detector_crosswalk.parquet"
    )
    
    return

if __name__ == "__main__":
    create_station_crosswalk()
    create_station_detector_crosswalk()