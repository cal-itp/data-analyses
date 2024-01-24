# Cut segments
segmentize:
	#pip install -r requirements.txt
	python cut_stop_segments.py
	python select_stop_segments.py

prep_roads:
# run cut_road_segments and concatenate_road_segments once
	#python cut_road_segments.py #cut once creates files needed to run in concat_road_segments
	#python concatenate_road_segments.py 
	python shapes_roads_crosswalk.py     
	#python road_cutpoints.py 
	python sjoin_vp_to_roads.py 
    
speeds_pipeline:
	make segmentize
	python nearest_vp_to_stop.py
	python interpolate_stop_arrival.py
	python stop_arrivals_to_speed.py
	make export

export: 
	python handle_common_errors.py
	python average_speeds.py  
    
download_roads:
	#pip install esridump
	#esri2geojson https://geo.dot.gov/server/rest/services/Hosted/California_2018_PR/FeatureServer/0 ca_roads.geojson
	python download_all_roads.py
