"""
Wrapper function over
nearest_vp_to_stop.py, 
interpolate_stop_arrivals.py,
and calculate_speed_from_stop_arrivals.py
"""
from pathlib import Path
from typing import Literal, Optional

from nearest_vp_to_stop import nearest_neighbor_for_stop
from interpolate_stop_arrival import interpolate_stop_arrivals
from stop_arrivals_to_speed import calculate_speed_from_stop_arrivals

from segment_speed_utils.project_vars import CONFIG_PATH, SEGMENT_TYPES

def nearest_neigbor_to_speed(
    analysis_date,
    segment_type: Literal[SEGMENT_TYPES],
    config_path: Optional[Path] = CONFIG_PATH
):
    """
    Wrapper function calling nearest neighbor, 
    stop arrival interpolation and monotonicity,
    interpolation of stop arrival, deriving segment speeds 
    between stops.
    """
    nearest_neighbor_for_stop(
        analysis_date = analysis_date,
        segment_type = segment_type,
        config_path = config_path
    )    

    interpolate_stop_arrivals(
        analysis_date = analysis_date, 
        segment_type = segment_type, 
        config_path = config_path
    )

    calculate_speed_from_stop_arrivals(
        analysis_date = analysis_date, 
        segment_type = segment_type,
        config_path = config_path
    )

    return