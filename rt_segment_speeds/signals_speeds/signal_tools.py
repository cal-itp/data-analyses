import pandas as pd
import geopandas as gpd
from calitp_data_analysis.geography_utils import WGS84, CA_NAD83Albers_m
import shapely
import numpy as np

def sjoin_signals(signal_gdf: gpd.GeoDataFrame,
                  segments_gdf: gpd.GeoDataFrame,
                  segments_lines_gdf: gpd.GeoDataFrame):
    '''
    signal_gdf: one-off format from traffic ops. primarily a spatial process,
    so exclude freeway ramp meters (only relevant to traffic joining fwy,
    which usually isn't transit)
    segments_gdf: geometry is polygons (buffered)
    segments_lines_gdf: geometry is linestrings (need for later approaching calc)
    '''
    
    signals = signal_gdf.loc[
        signal_gdf["tms_unit_type"] != "Freeway Ramp Meters",
        ["imms_id", "objectid", "location", signal_gdf.geometry.name]
    ].copy()
    
    signals_points = signals.to_crs(CA_NAD83Albers_m)
    signals_buffered = signals_points.copy()
    signals_buffered.geometry = signals_buffered.buffer(150)

    joined = gpd.sjoin(segments_gdf, signals_buffered).drop("index_right", axis=1)

    points_for_join = signals_points.rename_geometry("signal_pt_geom")[["signal_pt_geom", "imms_id", "objectid"]]
    joined_signal_points = joined.merge(points_for_join, on="objectid", how="inner", validate="many_to_one")

    # add line geometries from stop_segment_speed_view
    seg_lines = (segments_lines_gdf
        .rename_geometry("line_geom")[
            ["line_geom", "shape_id", "segment_id", "organization_source_record_id"]
        ].drop_duplicates(keep="first")
    )
    # ideally a more robust join in the future
    joined_seg_lines = joined_signal_points.merge(
        seg_lines,
        how="inner",
        on=["shape_id", "segment_id"],
    )
    return joined_seg_lines

@np.vectorize
def vector_start(linestring):
    return shapely.ops.substring(linestring, 0, 0)

@np.vectorize
def vector_end(linestring):
    return shapely.ops.substring(linestring, linestring.length, linestring.length)

@np.vectorize
def vector_distance(point1, point2):
    return point1.distance(point2)

def determine_approaching(joined_seg_lines_gdf: gpd.GeoDataFrame) -> pd.Series:
    
    '''
    using vectorized shapely functions,
    determine if segment is approaching a signal or departing a signal.
    '''
    
    start_array = vector_start(joined_seg_lines_gdf.line_geom)
    end_array = vector_end(joined_seg_lines_gdf.line_geom)
    start_distances = vector_distance(start_array, joined_seg_lines_gdf.signal_pt_geom)
    end_distances = vector_distance(end_array, joined_seg_lines_gdf.signal_pt_geom)
    approaching = start_distances > end_distances
    assert len(approaching) == len(joined_seg_lines_gdf)
    return approaching
    joined_seg_lines_gdf['approaching'] = approaching # lets not mess with mutability
    #return pd.concat([joined_seg_lines_gdf, pd.Series(approaching, name="approaching")], axis=1)
    