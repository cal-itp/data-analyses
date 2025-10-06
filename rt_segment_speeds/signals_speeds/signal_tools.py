import pandas as pd
import geopandas as gpd
from calitp_data_analysis.geography_utils import WGS84, CA_NAD83Albers_m
from shared_utils.rt_utils import arrowize_by_frequency, try_parallel


def sjoin_signals(
    signals_gdf: gpd.GeoDataFrame,
    segments_gdf: gpd.GeoDataFrame,
    segments_lines_gdf: gpd.GeoDataFrame,
    signals_buffer_distance: int = 150,
):
    """
    signals_gdf: one-off format from traffic ops. primarily a spatial process,
    so exclude freeway ramp meters (only relevant to traffic joining fwy,
    which usually isn't transit)
    segments_gdf: geometry is polygons (buffered)
    segments_lines_gdf: geometry is linestrings (need for later approaching calc)
    """

    signals = signals_gdf.loc[
        signals_gdf["tms_unit_type"] != "Freeway Ramp Meters",
        ["imms_id", "objectid", "location", signals_gdf.geometry.name],
    ].copy()

    signals_points = signals.to_crs(CA_NAD83Albers_m)
    signals_buffered = signals_points.copy()
    signals_buffered.geometry = signals_buffered.buffer(150)

    joined = gpd.sjoin(segments_gdf, signals_buffered).drop("index_right", axis=1)

    points_for_join = signals_points.rename_geometry("signal_pt_geom")[
        ["signal_pt_geom", "imms_id", "objectid"]
    ]
    joined_signal_points = joined.merge(
        points_for_join, on="objectid", how="inner", validate="many_to_one"
    )

    # add line geometries from stop_segment_speed_view
    seg_lines = segments_lines_gdf.rename_geometry("line_geom")[
        ["line_geom", "shape_id", "segment_id", "organization_source_record_id"]
    ].drop_duplicates(keep="first")
    # ideally a more robust join in the future
    joined_seg_lines = joined_signal_points.merge(
        seg_lines,
        how="inner",
        on=["shape_id", "segment_id"],
    )
    return joined_seg_lines


def filter_points_along_corridor(
    points_gdf: gpd.GeoDataFrame, corridor_gdf: gpd.GeoDataFrame, buffer_distance: int
) -> gpd.GeoDataFrame:
    """Filter points that are along the corridors in corridor_gdf"""
    return points_gdf.clip(
        corridor_gdf.to_crs(CA_NAD83Albers_m).buffer(buffer_distance)
    )


def clean_sjoin_signals_segments(
    signals_gdf, speedmap_segments_gdf, buffer_distance
) -> gpd.GeoDataFrame:
    """
    This just calls sjoin_signals_segments but handles the weirdness with buffering, it should be integrated into that function instead
    """
    buffer_distance_segments = 5
    buffer_distance_signals = buffer_distance - buffer_distance_segments

    # Get one GDF with signals and their nearest segment
    buffered_speedmap_segments = gpd.GeoDataFrame(
        data=speedmap_segments_gdf.drop(speedmap_segments_gdf.geometry.name, axis=1),
        geometry=speedmap_segments_gdf.to_crs(CA_NAD83Albers_m).buffer(
            buffer_distance_segments
        ),
    )

    # Join segments to signals
    sjoined_signals_segments = (
        sjoin_signals(
            signals_gdf=signals_gdf.reset_index(),
            segments_gdf=buffered_speedmap_segments,
            segments_lines_gdf=speedmap_segments_gdf,
            signals_buffer_distance=buffer_distance_signals,
        )
        .drop("geometry", axis=1)
        .set_geometry("line_geom")
    )
    return sjoined_signals_segments


def add_transit_metrics_to_signals(signals_gdf, sjoined_signals_segments):
    # Get distances which points and segments should be buffered by
    # TODO: I still don't understand the point of this, would be good to rewrite sjoin_signals to avoid

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
    ).drop_duplicates(subset=["shape_id", "objectid"], keep="first")

    # Get metrics for each signal
    signals_gdf_with_metrics = signals_gdf.copy()
    speedmaps_grouped_by_signal = signals_segments_removed_duplicates.groupby(
        "objectid"
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


def ready_signals_for_display(signals_gdf, buffer_distance):
    buffered_signals = gpd.GeoDataFrame(
        data=signals_gdf.rename(columns={"trips_hr_sch": "Trips/Hour"}),
        geometry=signals_gdf.to_crs(CA_NAD83Albers_m).buffer(buffer_distance),
    ).reset_index()
    return buffered_signals
