setup_rt_analysis:
	pip install -r requirements.txt

generate_speedmaps_slowly:
	python build_speedmaps_index.py
	python check_stage_intermediate.py
	python check_test_mapping.py
	python stage_run_portfolio.py

clean_speedmap_progress:
	rm _rt_progress*
