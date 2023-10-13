create_gtfs_schedule_geospatial_open_data:
# make sure to update update_vars.py
	python create_routes_data.py
	python create_stops_data.py
	#python link_operator_to_county_district.py
    

compile_open_data_portal:
	python gcs_to_esri.py
	#update metadata.yml (add new datasets here)
	python supplement_meta.py # run if any changes are made to yml
	#python arcgis_script_pro.py #(in ESRI!)
	python update_data_dict.py # check if columns are missing in data_dictionary yml 
	python update_fields_fgdc.py # populate fields with data dictionary yml values 
	python open_data.py # go back into ESRI and update xml
	python cleanup.py  # run after ESRI work done 