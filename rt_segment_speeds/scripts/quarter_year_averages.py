"""
Average segment speeds over longer time periods,
a quarter or a year.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd

from typing import Literal

from calitp_data_analysis import utils
from shared_utils import dask_utils, rt_dates, time_helpers
from segment_speed_utils import helpers
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT


def get_aggregation(df: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    """
    Aggregating across days, take the (mean)p20/p50/p80 speed
    and count number of trips across those days.
    """
    speed_cols = [c for c in df.columns if "_mph" in c]

    df2 = (df
           .groupby(group_cols, group_keys=False)
           .agg(
               {**{c: "mean" for c in speed_cols},
                "n_trips": "sum"})
           .reset_index()
          )
    
    return df2


def get_segment_speed_ddf(
    segment_type: str,
    analysis_date_list: list, 
    time_cols: list
) -> dd.DataFrame:
    """
    Import segment speeds (p20/p50/p80) speeds
    across a list of dates.
    Prep the df for aggregation by year-quarter or year.
    """
    speed_file = GTFS_DATA_DICT[segment_type]["route_dir_single_segment"]
    paths = [f"{SEGMENT_GCS}{speed_file}" for _ in analysis_date_list]

    SEGMENT_COLS = [*GTFS_DATA_DICT[segment_type]["segment_cols"]]
    SEGMENT_COLS = [i for i in SEGMENT_COLS if i != "geometry"]
    
    OPERATOR_COLS = [
        "name", 
        "caltrans_district", 
        # TODO: in future, moving how we choose organization
        # to another approach, so let's not even include it here
    ]
    
    group_cols = helpers.unique_list(OPERATOR_COLS + SEGMENT_COLS + ["stop_pair_name"])
    
    ddf = dask_utils.get_ddf(
        paths, 
        analysis_date_list, 
        data_type = "df",
        add_date = True,
        columns = group_cols + [
            "p20_mph", "p50_mph", "p80_mph", "n_trips",
            "time_period"
        ]
    )
    
    ddf = time_helpers.add_quarter(ddf).sort_values(
        "year_quarter", ascending=False)
    
    ddf2 = get_aggregation(
        ddf, 
        group_cols + time_cols
    )
    
    return ddf2


def get_segment_geom_gddf(
    segment_type: str,
    analysis_date_list: list, 
    time_cols: list
) -> dg.GeoDataFrame:
    """
    Import segment geometry gdf
    across a list of dates.
    Dedupe the df for aggregation by year-quarter or year.
    """
    segment_file = GTFS_DATA_DICT[segment_type]["segments_file"]
    segment_cols = [*GTFS_DATA_DICT[segment_type]["segment_cols"]]
    paths = [f"{SEGMENT_GCS}{segment_file}" for _ in analysis_date_list]

    # From the speed file, we want schedule_gtfs_dataset_key and name
    # so we can dedupe by name over a longer time period
    # name may not change, but key can change
    speed_file = GTFS_DATA_DICT[segment_type]["route_dir_single_segment"]
    operator_paths = [f"{SEGMENT_GCS}{speed_file}" for _ in analysis_date_list]

    operator_ddf = dask_utils.get_ddf(
        operator_paths, 
        analysis_date_list, 
        data_type = "df",
        add_date = True,
        columns = ["schedule_gtfs_dataset_key", "name"]
    )
    
    segment_gddf = dask_utils.get_ddf(
        paths, 
        analysis_date_list, 
        data_type = "gdf",
        add_date = True,
        columns = ["schedule_gtfs_dataset_key"] + segment_cols
    )
    
    gddf = dd.merge(
        segment_gddf,
        operator_ddf,
        on = ["schedule_gtfs_dataset_key", "service_date"],
        how = "inner"
    )
    
    gddf = time_helpers.add_quarter(gddf).sort_values(
        "year_quarter", ascending=False)
    
    gddf2 = gddf[
        ["name"] + segment_cols + time_cols
    ].drop_duplicates()
    
    return gddf2


def average_segment_speeds_by_time_grain(
    segment_type: str,
    analysis_date_list: list, 
    time_grain: Literal["quarter", "year"]
) -> gpd.GeoDataFrame:
    """
    Average segment speeds (tabular) by time grain
    and attach the geometry for segment that's been deduped at
    that time grain.
    """    
    if time_grain == "quarter":
        time_cols = ["year", "quarter"]
    elif time_grain == "year":
        time_cols = ["year"]
    
    SEGMENT_COLS = [*GTFS_DATA_DICT[segment_type]["segment_cols"]]
    SEGMENT_COLS = [c for c in SEGMENT_COLS if c != "geometry"]
    
    speeds = get_segment_speed_ddf(
        segment_type, analysis_date_list, time_cols
    ).compute()
    
    segment_geom = get_segment_geom_gddf(
        segment_type, analysis_date_list, time_cols
    ).compute()

    speed_gdf = pd.merge(
        segment_geom,
        speeds,
        on = ["name"] + SEGMENT_COLS + time_cols,
        how = "inner"
    )
    
    return speed_gdf


if __name__ == "__main__":
    from shared_utils import rt_dates

    start = datetime.datetime.now()
    
    segment_type = "stop_segments"
    QUARTER_EXPORT = GTFS_DATA_DICT[segment_type]["route_dir_quarter_segment"]
    YEAR_EXPORT = GTFS_DATA_DICT[segment_type]["route_dir_year_segment"]
    
    all_dates = rt_dates.y2024_dates + rt_dates.y2023_dates
    
    # quarter averages take 11 min
    speeds_by_quarter = average_segment_speeds_by_time_grain(
        segment_type,
        all_dates, 
        time_grain = "quarter"
    )

    utils.geoparquet_gcs_export(
        speeds_by_quarter,
        SEGMENT_GCS,
        QUARTER_EXPORT
    )
    del speeds_by_quarter
    
    time1 = datetime.datetime.now()
    print(f"quarter averages: {time1 - start}")
    
    # year averages take 10 min  
    speeds_by_year =  average_segment_speeds_by_time_grain(
        segment_type,  
        all_dates, 
        time_grain = "year"
    )

    utils.geoparquet_gcs_export(
        speeds_by_year,
        SEGMENT_GCS,
        YEAR_EXPORT
    )    
    
    end = datetime.datetime.now()
    print(f"year averages: {end - time1}")
    print(f"execution time: {end - start}")