rt_sched_pipeline:
	# cd rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env && cd ../
	python rt_v_scheduled_trip.py
	python rt_v_scheduled_routes.py
	#python rt_v_scheduled_operator.py  Amanda commenting this out for now  

# this can be run after rt_segment_speeds make rt_stop_times_pipeline is run
schedule_rt_stop_times_table:
	python rt_stop_times.py

full_pipeline:
	make rt_sched_pipeline && make schedule_rt_stop_times_table    