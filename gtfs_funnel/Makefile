download_gtfs_data:
# make sure to update gtfs_funnel/update_vars.py for dates to download
	python download_trips.py
	python download_stops.py
	python download_shapes.py
	python download_stop_times.py
	python download_vehicle_positions.py
	python concatenate_vehicle_positions.py      

preprocess_schedule_vp_dependency:
	python stop_times_with_direction.py
	python crosswalk_gtfs_dataset_key_to_organization.py    
    
preprocess_vp:
	python vp_keep_usable.py
	python merge_in_vp_direction.py
	python cleanup.py
	python vp_dwell_time.py
	python merge_vp_dwell_usable.py
	python vp_condenser.py

preprocess_schedule_only:
	#make route_typologies_data # run quarterly
	python schedule_stats_by_route_direction.py # this no longer has route typology
	python operator_scheduled_stats.py # adjust this to use yearly route typology
	python schedule_stats_by_stop.py 
	python track_publish_dates.py
    
route_typologies_data:
	python stop_arrivals_in_roads.py # quarterly cadence (Jan/Apr/Jul/Oct)
	python route_typologies.py

# Start compiling scripts needed when we start concatenating several days
# Clean route names for displaying across time
timeseries_preprocessing: 
	python clean_route_naming.py
    
# monthly scheduled service, download after the end of each month
monthly_scheduled_data:
	python download_monthly_service.py
	python concatenate_monthly_scheduled_service.py
    
funnel_gtfs_single_day: 
	make download_gtfs_data 
	make preprocess_schedule_vp_dependency 
	make preprocess_vp
	make preprocess_schedule_only 
	make timeseries_preprocessing
	#make monthly_scheduled_data    

all:
#	make funnel_gtfs_single_day
    # update open_data/update_vars.py        
	cd ../open_data/ && make create_gtfs_schedule_geospatial_open_data -f Makefile
    # update high_quality_transit_areas/update_vars.py    
#	cd ../high_quality_transit_areas/ && make hqta_data -f Makefile    
    # update rt_segment_speeds/segment_speed_utils/project_vars.py
	cd ../rt_segment_speeds/scripts/ && make all_speeds_pipeline -f Makefile && cd ../../
    # update rt_scheduled_v_ran/scripts/update_vars.py
	cd ../rt_scheduled_v_ran/scripts/ && make full_pipeline -f Makefile
