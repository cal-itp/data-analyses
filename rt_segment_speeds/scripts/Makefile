# Cut segments
segmentize:
	python prep_stop_segments.py
	python cut_normal_stop_segments.py
	python cut_special_stop_segments.py
	python concatenate_stop_segments.py
	#python cut_road_segments.py #cut once creates files needed to run in concat_road_segments
	python concatenate_road_segments.py
    
   
speeds_pipeline:
	python A0_preprocessing.py
	python find_vp_direction.py
	python A1_sjoin_vp_segments.py
	python A2_sjoin_postprocessing.py
	python A3_valid_vehicle_positions.py
	python B1_speeds_by_segment_trip.py
	python B2_avg_speeds_by_segment.py
	python B3_export.py
	python C2_triangulate_vp.py      
	python C3_trip_route_speed.py  

sjoin_prep:
	python A0_preprocessing.py
	python find_vp_direction.py
    
download_roads:
	#pip install esridump
	#esri2geojson https://geo.dot.gov/server/rest/services/Hosted/California_2018_PR/FeatureServer/0 ca_roads.geojson
	python download_all_roads.py
