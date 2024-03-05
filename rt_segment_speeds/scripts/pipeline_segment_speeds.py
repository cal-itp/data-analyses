"""
Run nearest_vp_to_stop.py, 
interpolate_stop_arrivals.py,
and calculate_speed_from_stop_arrivals.py for stop_segments.
"""
from pipe import nearest_neigbor_to_speed
segment_type = "stop_segments"

if __name__ == "__main__":
    
    from segment_speed_utils.project_vars import analysis_date_list
    
    print(f"segment_type: {segment_type}")
    
    for analysis_date in analysis_date_list:

        nearest_neigbor_to_speed(
            analysis_date = analysis_date,
            segment_type = segment_type
        )    

       
