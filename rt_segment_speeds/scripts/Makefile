# Cut segments
segmentize:
	python prep_stop_segments.py
	python cut_normal_stop_segments.py
	python cut_special_stop_segments.py
	python concatenate_stop_segments.py
      
get_speeds_by_segment:
	python A0_preprocessing.py
	python A1_sjoin_vp_segments.py
	python A2_valid_vehicle_positions.py
	python A3_loop_inlining.py
	python A4_concatenate_vp_pared.py
	python B1_speeds_by_segment_trip.py
	python B2_avg_speeds_by_segment.py
	#python C1_rt_trip_diagnostics.py
	#TODO: where should calculate_stop_delay and stop_level_metrics be?

    
download_roads:
	#pip install esridump
	#esri2geojson https://geo.dot.gov/server/rest/services/Hosted/California_2018_PR/FeatureServer/0 ca_roads.geojson
	python download_all_roads.py
