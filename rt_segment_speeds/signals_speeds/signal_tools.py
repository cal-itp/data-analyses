import pandas as pd
import geopandas as gpd
from calitp_data_analysis.geography_utils import WGS84, CA_NAD83Albers_m
from shared_utils.rt_utils import arrowize_by_frequency, try_parallel
from typing import Hashable
from constants import SIGNAL_OWNER_NAME

def sjoin_signals(
    signals_gdf: gpd.GeoDataFrame,
    segments_lines_gdf: gpd.GeoDataFrame,
    unique_identifier_signals: Hashable,
    signals_buffer_distance: int = 150,
):
    """
    Sjoin the signals and segments gdf within the buffer distance, keeping both geometries as follows
      - Segment geometry -> line_geom, main geometry
      - Signal geometry -> signal_pt_geom, kept as a GeoSeries within the GDF
    signals_gdf: GDF with signal points
    segments_lines_gdf: GDF with speedmaps segments as lines
    unique_identifier_signals: Name of a column in signals_gdf that uniquely identifies a traffic signal
    signals_buffer_distance: The distance around each signal to buffer, defaults to 150
    """

    # Get buffered signals with just join fields and geometry
    signals = signals_gdf[
        [unique_identifier_signals, signals_gdf.geometry.name]
    ].copy()
    signals_points = signals.to_crs(CA_NAD83Albers_m)
    signals_buffered = signals_points.copy()
    signals_buffered.geometry = signals_buffered.buffer(signals_buffer_distance)

    # Join segments to signals, keeping only join fields from both
    # Rename the merged geometry to "line_geom"
    joined = (
        segments_lines_gdf.to_crs(CA_NAD83Albers_m)
        .sjoin(signals_buffered, how="left")
        .drop("index_right", axis=1)
    ).rename_geometry("line_geom")

    # Merge signal point geometry back into the joined gdf
    points_for_join = signals_points.rename_geometry("signal_pt_geom")
    joined_signal_points = joined.merge(
        points_for_join,
        on=unique_identifier_signals,
        how="inner",
        validate="many_to_one",
    )
    return joined_signal_points


def filter_points_along_corridor(
    points_gdf: gpd.GeoDataFrame, corridor_gdf: gpd.GeoDataFrame, buffer_distance: int
) -> gpd.GeoDataFrame:
    """Filter points in points_gdf that are within buffer_distance of the corridors in corridor_gdf"""
    return points_gdf.clip(
        corridor_gdf.to_crs(CA_NAD83Albers_m).buffer(buffer_distance)
    )


def add_transit_metrics_to_signals(
    signals_gdf: gpd.GeoDataFrame,
    sjoined_signals_segments: gpd.GeoDataFrame,
    unique_identifier_signals: Hashable,
):
    """
    Return the signals_gdf with the following additional metrics:
    - trips_hr_sch: the total trips per hour associated with the signal
    - route_names_aggregated: the comma-separated short names of each route associated with the signal. Maps 1:1 with organization_names_aggregated
    - organization_names_aggregated: the comma-separated organization names of each route associated with the signal. Maps 1:1 with route_names_aggregated
    
    Parameters:
    - signals_gdf - a points gdf of traffic signals
    - sjoined_signals_segments - speedmaps segments. Must be as returned from sjoin_signals
    - unique_identifier_signals - a unique identifier for each signal. Must be the same as passed to sjoin_signals
    
    Returns:
    signals_gdf with the specified additional fields
    """
    # Get a gdf of segments with one entry per signal and shape_id
    # Get the distance from the segment to the associated signal
    sjoined_signals_segments["distance_to_signal"] = (
        sjoined_signals_segments["line_geom"]
        .to_crs(CA_NAD83Albers_m)
        .distance(sjoined_signals_segments["signal_pt_geom"].to_crs(CA_NAD83Albers_m))
    )
    # Make sure we only count the one shape per signal
    signals_segments_removed_duplicates = sjoined_signals_segments.sort_values(
        ["distance_to_signal"], ascending=True
    ).drop_duplicates(subset=["shape_id", unique_identifier_signals], keep="first")

    # Get metrics for each signal
    signals_gdf_with_metrics = signals_gdf.set_index(unique_identifier_signals)
    speedmaps_grouped_by_signal = signals_segments_removed_duplicates.groupby(
        unique_identifier_signals
    )
    # Get frequencies through a stop
    signals_gdf_with_metrics["trips_hr_sch"] = speedmaps_grouped_by_signal[
        "trips_hr_sch"
    ].sum()

    # Get all the routes and organizations that serve a stop
    def aggregate_names(names):
        return ", ".join(names.drop_duplicates().dropna())
    signals_gdf_with_metrics["route_names_aggregated"] = (
        speedmaps_grouped_by_signal["route_short_name"]
    ).agg(aggregate_names)
    signals_gdf_with_metrics["organization_names_aggregated"] = (
        speedmaps_grouped_by_signal["organization_name"]
    ).agg(aggregate_names)

    return signals_gdf_with_metrics


def ready_speedmap_segments_for_display(sjoined_segments_gdf):
    """Arrowize and format fields for display for sjoined_segments_gdf"""
    arrowized_gdf = (
        sjoined_segments_gdf.drop(["signal_pt_geom"], axis=1)
        .to_crs(CA_NAD83Albers_m)
        .rename_geometry("geometry")
    )
    arrowized_gdf.geometry = arrowized_gdf.geometry.apply(try_parallel)
    arrowized_gdf = arrowized_gdf.apply(
        arrowize_by_frequency, axis=1, frequency_col="trips_hr_sch"
    )
    arrowized_gdf["route_short_name"] = (
        arrowized_gdf["organization_name"] + " - " + arrowized_gdf["route_short_name"]
    )
    return arrowized_gdf


def ready_signals_for_display(signals_gdf, buffer_distance, ownership_map):
    """
    Buffer and format fields for display for a single signals_gdf

    Parameters:
    signals_gdf - a gdf of signals. Must contain 'signal_owner' as a field.
    buffer_distance - the distance around each signal to buffer (corresponds to the render size of each signal)
    ownership_map - maps signal_owner values to ints to be used for color mapping
    """
    buffered_signals = gpd.GeoDataFrame(
        data=signals_gdf.rename(columns={"trips_hr_sch": "Trips/Hour"}),
        geometry=signals_gdf.to_crs(CA_NAD83Albers_m).buffer(buffer_distance),
    ).reset_index()
    buffered_signals["color_key"] = buffered_signals[SIGNAL_OWNER_NAME].map(ownership_map)
    return buffered_signals
