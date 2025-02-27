# Cut segments
segmentize:
	#pip install -r requirements.txt
	python cut_stop_segments.py
	#python select_stop_segments.py
	python cut_speedmap_segments.py    

prep_roads:
# run cut_road_segments once
	#python cut_road_segments.py #cut once creates files needed to run in concat_road_segments
	python shapes_roads_crosswalk.py     
	python proxy_road_stop_times.py    
    
# do not support in 2025 -- can refactor to be similar to roads
# where we have custom points along shape / corridor and check all trips against those
#speeds_pipeline: # (refactor): subsume by rt_stop_times / add more outputs there
	#python pipeline_segment_speeds.py

rt_stop_times_pipeline:
	python pipeline_rt_stop_times.py
	python average_summary_speeds.py  
	#python average_segment_speeds.py # will do yearly averages 

speedmaps_pipeline:
	python pipeline_speedmap.py
  
all_speeds_pipeline:
	make segmentize    
	make rt_stop_times_pipeline
	make speedmaps_pipeline
	python publish_open_data.py  
	#python publish_public_gcs.py #rarely used

download_roads:
	#pip install esridump
	#esri2geojson https://geo.dot.gov/server/rest/services/Hosted/California_2018_PR/FeatureServer/0 ca_roads.geojson
	python download_all_roads.py
