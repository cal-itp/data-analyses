# Roughly, data cleaning steps are:
stop_times_poc_cleaned_data:
	cd ../rt_segment_speed_utils/ && pip install -r requirements.txt && cd ..
	#query_materialized_tables.py --save outputs in GCS
	python more_cleaning.py
	#python schedule_stop_times_with_geom.py -- do this for easier mapping 