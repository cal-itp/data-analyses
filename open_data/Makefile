download_schedule_data_one_day:
# make sure to update update_vars.py
	python download_trips.py
	python download_stops.py
	python download_shapes.py
	python download_stop_times.py

download_vehicle_positions_one_day:
	python download_vehicle_positions.py
	python concatenate_vehicle_positions.py
    
create_gtfs_schedule_geospatial_open_data:
	python create_routes_data.py
	python create_stops_data.py
	python link_operator_to_county_district.py
    

compile_open_data_portal:
	python gcs_to_esri.py
	#python arcgis_script_pro.py #(in ESRI!)
	#update metadata_hqta.py or metadata_traffic_ops.py  # need to change XML field names...arcpy didn't
	python open_data.py
	python cleanup.py    