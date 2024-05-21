# Cut segments
segmentize:
	#pip install -r requirements.txt
	python cut_stop_segments.py
	python select_stop_segments.py
	python cut_speedmap_segments.py    

prep_roads:
# run cut_road_segments and concatenate_road_segments once
	#python cut_road_segments.py #cut once creates files needed to run in concat_road_segments
	#python concatenate_road_segments.py 
	python shapes_roads_crosswalk.py     
	#python road_cutpoints.py 
	python sjoin_vp_to_roads.py 
    
speeds_pipeline:
	make segmentize
	python pipeline_segment_speeds.py
	python average_segment_speeds.py  

rt_stop_times_pipeline:
	python pipeline_rt_stop_times.py
	python average_summary_speeds.py  

speedmaps_pipeline:
	python pipeline_speedmap.py
	python average_speedmap_segment_speeds.py  
    
all_speeds_pipeline:
	make speeds_pipeline
	make rt_stop_times_pipeline
	make speedmaps_pipeline
    
download_roads:
	#pip install esridump
	#esri2geojson https://geo.dot.gov/server/rest/services/Hosted/California_2018_PR/FeatureServer/0 ca_roads.geojson
	python download_all_roads.py
