generate_speedmaps_slowly:
	python build_speedmaps_index.py
	python check_stage_intermediate.py
	python check_test_mapping.py
#	python stage_run_portfolio.py

generate_d4_data:
	python build_speedmaps_index.py
	python index_filter_d4.py
	python check_stage_intermediate.py
	python check_test_mapping.py

clean_speedmap_progress:
	rm _rt_progress*
