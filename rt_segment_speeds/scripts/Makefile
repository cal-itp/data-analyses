# Cut segments
segmentize:
	#pip install -r requirements.txt
	python cut_stop_segments.py
	python cut_speedmap_segments.py      

rt_stop_times_pipeline:
	make segmentize
	python pipeline_rt_stop_times.py
	python average_summary_speeds.py  
	#python average_segment_speeds.py  # Now multi-day averages

speedmaps_pipeline:
	python pipeline_speedmap.py
  
all_speeds_pipeline:
	make rt_stop_times_pipeline
	make speedmaps_pipeline
	python publish_open_data.py  
	#python publish_public_gcs.py #rarely used

download_roads:
	#pip install esridump
	#esri2geojson https://geo.dot.gov/server/rest/services/Hosted/California_2018_PR/FeatureServer/0 ca_roads.geojson
	python download_all_roads.py

# Prefer Census TIGER roads because road identifiers are easier to use (linearid)
census_roads_preprocessing:
	python ingest_ca_roads.py
	python cut_road_segments.py
	python condense_shn.py

modeled_vp_preprocessing:
	python vp_condenser.py
	python resample.py 
	#python modeled_speeds_general.py 
# dependency is a more minimal version of stop_times_direction
	#python cut_stop_segments.py  

modeled_rt_stop_times:
	python stop_times_prep.py
	python modeled_rt_stop_times_speeds.py
	python aggregate_modeled_speeds.py
    
modeled_road_segment_speeds:
	python crosswalk_vp_roads.py    
	python project_road_segments.py
	python model_speeds_for_roads.py
	python aggregate_road_speeds.py
    