"""
Attach stop times table to HQTA segments,
and flag which segments are HQ transit corridors.

Takes <1 min to run.
- down from 1 hr in v2 (was part of B1)
"""

import datetime
import sys

import geopandas as gpd
import lookback_wrappers
import numpy as np
import pandas as pd
import shapely
from _utils import append_analysis_name
from calitp_data_analysis import get_fs, utils
from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
from loguru import logger
from segment_speed_utils import helpers
from update_vars import (
    AM_PEAK,
    GCS_FILE_PATH,
    HQ_TRANSIT_THRESHOLD,
    MS_TRANSIT_THRESHOLD,
    PM_PEAK,
    PROJECT_CRS,
    SEGMENT_BUFFER_METERS,
    analysis_date,
)

am_peak_hrs = list(range(AM_PEAK[0].hour, AM_PEAK[1].hour))
pm_peak_hrs = list(range(PM_PEAK[0].hour, PM_PEAK[1].hour))
both_peaks_hrs = am_peak_hrs + pm_peak_hrs
peaks_dict = {key: "am_peak" for key in am_peak_hrs} | {key: "pm_peak" for key in pm_peak_hrs}

gcsgp = GCSGeoPandas()


def max_trips_by_group(df: pd.DataFrame, group_cols: list, max_col: str = "n_trips") -> pd.DataFrame:
    """
    Find the max trips, by stop_id or by hqta_segment_id.
    Put in a list of group_cols to find the max.
    Can also subset for AM or PM by df[df.departure_hour < 12]
    """
    df2 = df.groupby(group_cols).agg({max_col: "max"}).reset_index()

    return df2


def hqta_segment_to_stop(hqta_segments: gpd.GeoDataFrame, stops: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Spatially join hqta segments to stops.
    Which stops fall into which segments?
    """
    segment_cols = ["hqta_segment_id", "segment_sequence", "route_id"]

    segment_to_stop = (
        gpd.sjoin(stops[["stop_id", "geometry"]], hqta_segments, how="inner", predicate="intersects").drop(
            columns=["index_right"]
        )
    )[segment_cols + ["stop_id"]]

    # After sjoin, we don't want to keep stop's point geom
    # Merge on hqta_segment_id's polygon geom
    segment_to_stop2 = pd.merge(hqta_segments, segment_to_stop, on=segment_cols)

    return segment_to_stop2


def hqta_segment_keep_one_stop(hqta_segments: gpd.GeoDataFrame, stop_frequencies: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Since multiple stops can fall into the same segment,
    keep the stop with the highest trips (sum across AM and PM).

    Returns gdf where each segment only appears once.
    """
    stop_cols = ["schedule_gtfs_dataset_key", "stop_id"]

    segment_to_stop_frequencies = pd.merge(hqta_segments, stop_frequencies, on=stop_cols)
    # TODO check route_id here?
    # Can't sort by multiple columns in dask,
    # so, find the max, then inner merge
    max_trips_by_segment = max_trips_by_group(
        segment_to_stop_frequencies, group_cols=["hqta_segment_id"], max_col="n_trips"
    )

    # Merge in and keep max trips observation
    # Since there might be duplicates still, where multiple stops all
    # share 2 trips for that segment, do a drop duplicates at the end
    max_trip_cols = ["hqta_segment_id", "am_max_trips_hr", "pm_max_trips_hr"]

    segment_to_stop_unique = pd.merge(
        segment_to_stop_frequencies, max_trips_by_segment, on=["hqta_segment_id", "n_trips"], how="inner"
    ).drop_duplicates(subset=max_trip_cols)

    # In the case of same number of trips overall, do a sort
    # with descending order for AM, then PM trips
    segment_to_stop_gdf = (
        segment_to_stop_unique.sort_values(max_trip_cols, ascending=[True, False, False])
        .drop_duplicates(subset="hqta_segment_id")
        .reset_index(drop=True)
    )

    return segment_to_stop_gdf


def find_circuitous_segments(hqta_segments: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Where individual segments loop tightly, segment_direction becomes arbitrary.
    OK to keep in possible HQ corridors, but shouldn't be used for bus intersection major stops
    """
    circuitousness_ratio_threshold = 3

    hqta_segments["length"] = hqta_segments.geometry.apply(lambda x: x.length)
    hqta_segments["start"] = hqta_segments.geometry.apply(lambda x: shapely.Point(x.coords[0]))
    hqta_segments["end"] = hqta_segments.geometry.apply(lambda x: shapely.Point(x.coords[-1]))
    hqta_segments["st_end_dist"] = hqta_segments.apply(lambda x: shapely.distance(x.start, x.end), axis=1)
    hqta_segments["circuitousness_ratio"] = (
        (hqta_segments.length / hqta_segments.st_end_dist).replace(np.inf, 10).clip(upper=5)
    )
    hqta_segments = hqta_segments.assign(
        circuitous_segment=hqta_segments.apply(
            lambda x: x.circuitousness_ratio > circuitousness_ratio_threshold, axis=1
        )
    )
    calculation_cols = ["length", "start", "end", "st_end_dist", "circuitousness_ratio"]
    hqta_segments = hqta_segments.drop(columns=calculation_cols)
    return hqta_segments


def sjoin_stops_and_stop_frequencies_to_hqta_segments(
    hqta_segments: gpd.GeoDataFrame,
    stops: gpd.GeoDataFrame,
    stop_frequencies: pd.DataFrame,
    buffer_size: int,
    hq_transit_threshold: int = HQ_TRANSIT_THRESHOLD,
    ms_transit_threshold: int = MS_TRANSIT_THRESHOLD,
) -> gpd.GeoDataFrame:
    """
    Take HQTA segments, draw a buffer around the linestrings.
    Spatial join the stops (points) to segments (now polygons).
    If there are multiple stops in a segment, keep the stop
    with more trips.
    Tag the segment as hq_transit_corr and/or ms_precursor (boolean)
    Since frequency thresholds for hq corrs and major stops have diverged,
    need to track both categories
    """
    # Only keep segments for routes that have at least one stop meeting frequency threshold
    # About 50x smaller, so should both slash false positives and enhance speed
    st_copy = stop_frequencies.copy().drop_duplicates(subset=["schedule_gtfs_dataset_key", "route_id"])
    hqta_segments = hqta_segments.merge(
        st_copy[["schedule_gtfs_dataset_key", "route_id"]], on=["schedule_gtfs_dataset_key", "route_id"]
    )
    stop_frequencies = stop_frequencies.drop(
        columns=["route_id"]
    ).drop_duplicates()  # prefer route_id from segments in future steps
    # Identify ambiguous direction segments to exclude from intersection steps
    hqta_segments = find_circuitous_segments(hqta_segments)
    # Draw buffer to capture stops around hqta segments
    hqta_segments2 = hqta_segments.assign(geometry=hqta_segments.geometry.buffer(buffer_size))

    # Join hqta segment to stops
    segment_to_stop = hqta_segment_to_stop(hqta_segments2, stops)
    gcsgp.geo_data_frame_to_parquet(stops, f"{GCS_FILE_PATH}test_segment_to_stop.parquet")
    segment_to_stop_unique = hqta_segment_keep_one_stop(segment_to_stop, stop_frequencies)

    # Identify hq transit corridor or major stop precursor
    drop_cols = ["n_trips"]

    segment_hq_corr = segment_to_stop_unique.assign(
        hq_transit_corr=segment_to_stop_unique.apply(
            lambda x: (
                True
                if (x.am_max_trips_hr >= hq_transit_threshold and (x.pm_max_trips_hr >= hq_transit_threshold))
                else False
            ),
            axis=1,
        ),
        ms_precursor=segment_to_stop_unique.apply(
            lambda x: (
                True
                if (x.am_max_trips_hr >= ms_transit_threshold and (x.pm_max_trips_hr >= ms_transit_threshold))
                else False
            ),
            axis=1,
        ),
    ).drop(columns=drop_cols)

    return segment_hq_corr


if __name__ == "__main__":

    logger.add("./logs/hqta_processing.log", retention="3 months")
    logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")

    start = datetime.datetime.now()

    fs = get_fs()

    # (1) Spatial join stops and stop times to hqta segments
    # this takes < 2 min
    hqta_segments = gcsgp.read_parquet(f"{GCS_FILE_PATH}hqta_segments.parquet")
    stops_cols = ["feed_key", "stop_id", "stop_name", "geometry"]
    stops = helpers.import_scheduled_stops(analysis_date, get_pandas=True, crs=PROJECT_CRS, columns=stops_cols).assign(
        analysis_date=analysis_date
    )
    published_operators_dict = lookback_wrappers.read_published_operators(analysis_date)
    print(published_operators_dict)
    trips_cols = ["name", "feed_key", "gtfs_dataset_key"]
    trips = helpers.import_scheduled_trips(analysis_date, columns=trips_cols, get_pandas=True).assign(
        analysis_date=analysis_date
    )
    lookback_trips = lookback_wrappers.get_lookback_trips(published_operators_dict, trips_cols)
    lookback_trips_ix = lookback_wrappers.lookback_trips_ix(lookback_trips)
    lookback_stops = lookback_wrappers.get_lookback_stops(
        published_operators_dict, lookback_trips_ix, stops_cols, crs=PROJECT_CRS
    )
    stops = (
        pd.concat([stops, lookback_stops])
        .merge(
            pd.concat([trips, lookback_trips])[["feed_key", "schedule_gtfs_dataset_key"]].drop_duplicates(),
            on="feed_key",
        )
        .pipe(append_analysis_name)
    )

    gcsgp.geo_data_frame_to_parquet(stops, f"{GCS_FILE_PATH}stops_with_lookback.parquet")
    max_arrivals_by_stop = pd.read_parquet(f"{GCS_FILE_PATH}max_arrivals_by_stop.parquet")

    hqta_corr = sjoin_stops_and_stop_frequencies_to_hqta_segments(
        hqta_segments=hqta_segments,
        stops=stops,
        stop_frequencies=max_arrivals_by_stop,
        buffer_size=SEGMENT_BUFFER_METERS,  # 50meters
        hq_transit_threshold=HQ_TRANSIT_THRESHOLD,
        ms_transit_threshold=MS_TRANSIT_THRESHOLD,
    )
    utils.geoparquet_gcs_export(
        hqta_corr,
        GCS_FILE_PATH,
        "all_bus",
    )

    end = datetime.datetime.now()
    logger.info(f"B3_sjoin_stops_to_segments {analysis_date} " f"execution time: {end - start}")
