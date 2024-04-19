rt_sched_pipeline:
	# cd rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env && cd ../
	python rt_v_scheduled_trip.py
	python rt_v_scheduled_routes.py
    

# this can be run after rt_segment_speeds make rt_stop_times_pipeline is run
schedule_rt_stop_times_table:
	python rt_stop_times.py
