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
	python vp_keep_usable.py
	python vp_direction.py
	python cleanup.py
	python route_typologies.py
    
    
funnel_gtfs_data: 
	make download_gtfs_data && make preprocess