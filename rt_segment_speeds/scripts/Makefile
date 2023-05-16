# Cut segments
segmentize:
	python cut_route_segments.py
	python prep_stop_segments.py
	python cut_normal_stop_segments.py
	python cut_special_stop_segments.py
	python concatenate_stop_segments.py
      
get_speeds_by_segment:
	python A0_preprocessing.py
	python A1_sjoin_vp_segments.py
	python A2_valid_vehicle_positions.py
	#python A3_speeds_by_segment_trip.py
	#python B1_rt_trip_diagnostics.py
	#python C1_calculate_stop_delay.py
    
download_roads:
	#pip install esridump
	#esri2geojson https://geo.dot.gov/server/rest/services/Hosted/California_2018_PR/FeatureServer/0 ca_roads.geojson
	python download_all_roads.py
