import geopandas as gpd
import pandas as pd

from typing import Literal

from calitp_data_analysis import utils
from utils import PROCESSED_GCS
from shared_utils.shared_data import GCS_FILE_PATH as SHARED_GCS

def postmiles_to_pems_format(
    gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    """
    rename_dict = {
        "route": "freeway_id"
    }
    
    gdf = gdf.assign(
        # NB becomes N
        freeway_direction = gdf.direction.str.replace("B", ""),
        # pm gets us better merges than pmc
        # pmc is PM(combined) which includes prefix/suffix
        abs_pm = gdf.odometer.round(1)
    ).rename(columns = rename_dict)
    
    return gdf
 
    
def clean_postmiles(
    dataset_name: Literal["points", "segments"],
    crs: str = "EPSG:4326"
) -> gpd.GeoDataFrame:
    """
    Clean SHN postmiles dataset.
    Be explicit about columns we don't want to keep
    
    We'll favor the PEMS data source over this one (for county, district, etc)
    and prefer column names / value formatting that match with PEMS.
    
    Also, postmiles will contain information about where the postmile
    is located, offset, etc, which we probably don't need either
    """
    if dataset_name=="points":
        FILE = "state_highway_network_postmiles"
    elif dataset_name=="segments":
        FILE = "state_highway_network_postmile_segments"
    else:
        raise ValueError(f"dataset_name must be: 'points' or 'segments'")
    
    gdf = gpd.read_parquet(
        f"{SHARED_GCS}{FILE}.parquet"
    ).to_crs(crs).pipe(postmiles_to_pems_format)
    
    if dataset_name=="points":
    
        drop_cols = [
            "district", "county",
            "direction",
            #route+suffix, sometimes it's 5
            # sometimes it's 5S. either way, we have freeway_direction
            "routes", "rtesuffix",
            "pmrouteid", 
            "pm", "pmc",
            "pminterval",
            "pmprefix", "pmsuffix",
            "aligncode", "pmoffset",
        ]
    
        gdf = gdf.drop(columns = drop_cols)
    
    elif dataset_name=="segments":
        gdf = gdf.drop(columns = ["direction", "abs_pm"])
    
    return gdf


def clean_station_freeway_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean up PEMS station dataset, so we can 
    merge with SHN postmiles dataset.
    """
    rename_dict = {
        "freeway_dir": "freeway_direction"
    }
    # Stations have abs_postmile, numeric, and we'll round to 1 decimal
    # place to match what SHN postmiles (odometer) would be
    df = df.assign(
        abs_pm = df.abs_postmile.round(1),
    ).rename(columns = rename_dict)
    
    return df

def merge_stations_to_shn_postmiles(
    station_crosswalk: pd.DataFrame,
) -> gpd.GeoDataFrame:
    merge_cols = ["freeway_id", "freeway_direction", "abs_pm"]

    station2 = clean_station_freeway_info(station_crosswalk)
    postmiles2 = clean_postmiles(dataset_name="points")
    
    m1 = pd.merge(
        station2,
        postmiles2,
        on = merge_cols,
        how = "left",
        indicator = True
    )
    
    station_cols = station2.columns.tolist()
    
    ok_df = m1[m1._merge=="both"]
    fix_df = m1[m1._merge=="left_only"][station_cols]

    station_fix_me = {
        35.4: 35.3,
        36.4: 36.3
    }
    
    fix_df = fix_df.assign(
        abs_pm = fix_df.abs_pm.map(station_fix_me)
    )
    
    m2 = pd.merge(
        fix_df,
        postmiles2,
        on = merge_cols,
        how = "inner",
    )
    
    df = pd.concat(
        [m1, m2], 
        axis=0, ignore_index=True
    ).drop(columns = "_merge")
    
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs = "EPSG:4326")
    
    return gdf

if __name__ == "__main__":
    
    station_crosswalk = pd.read_parquet(
        f"{PROCESSED_GCS}station_crosswalk.parquet",
    ) 
    
    final = merge_stations_to_shn_postmiles(
        station_crosswalk, 
    )
    
    print(f"# stations in final gdf: {final.station_uuid.nunique()}")
    print(f"# stations in original df: {station_crosswalk.station_uuid.nunique()}")
    print(final.dtypes)
    
    utils.geoparquet_gcs_export(
        final,
        PROCESSED_GCS,
        "stations_postmiles_crosswalk"
    )