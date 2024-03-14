download_gtfs_data:
# make sure to update update_vars.py for dates to download
	python download_trips.py
	python download_stops.py
	python download_shapes.py
	python download_stop_times.py
	python download_vehicle_positions.py
	python concatenate_vehicle_positions.py      

    
preprocess:
	python stop_times_with_direction.py
	python route_typologies.py
	python crosswalk_gtfs_dataset_key_to_organization.py    
	python vp_keep_usable.py
	python vp_direction.py
	python cleanup.py
	python vp_condenser.py
    
# Start compiling scripts needed when we start concatenating several days
# Clean route names for displaying across time
timeseries_preprocessing: 
	python clean_route_naming.py
    
    
funnel_gtfs_data: 
	make download_gtfs_data && make preprocess && make timeseries_preprocessing