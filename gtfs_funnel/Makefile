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

funnel_gtfs_single_day:
	make download_gtfs_data
	make preprocess_schedule_vp_dependency
	make preprocess_vp

all:
	make funnel_gtfs_single_day
    # update open_data/update_vars.py
	cd ../open_data/ && make create_gtfs_schedule_geospatial_open_data -f Makefile
    # update high_quality_transit_areas/update_vars.py
	cd ../high_quality_transit_areas/ && make hqta_data -f Makefile
    # update rt_segment_speeds/segment_speed_utils/project_vars.py
	cd ../rt_segment_speeds/scripts/ && make all_speeds_pipeline -f Makefile && cd ../../
