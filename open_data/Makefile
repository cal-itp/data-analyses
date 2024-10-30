create_gtfs_schedule_geospatial_open_data:
# make sure to update update_vars.py
	python create_routes_data.py
	python create_stops_data.py
    

compile_open_data_portal:
	python gcs_to_esri.py
	#update metadata.yml (add new datasets here)
	python supplement_meta.py # run if any changes are made to yml
	python update_data_dict.py # check if columns are missing in data_dictionary yml 
	python update_fields_fgdc.py # populate fields with data dictionary yml values, run if update_data_dict had changes to incorporate 
	# Download the zipped shapefiles and metadata.yml and move to local ESRI directory    
	#python arcgis_script_pro.py #(in ESRI!)
	# Bring the ESRI rewritten XML files into Hub and drop into xml/ and allow overwrite(s)
	python metadata_update_pro.py # (in Hub)
	# Download the overwritten XML files in xml/run_in_esri/ and move to local ESRI directory.   
	#python arcgis_script_pro.py #(in ESRI!)
	python cleanup.py  # run after ESRI work done 