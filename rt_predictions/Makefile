# Roughly, data cleaning steps are:
stop_times_poc_cleaned_data:
	cd ../rt_segment_speed_utils/ && pip install -r requirements.txt && cd ..
	#query_materialized_tables.py --save outputs in GCS
	python more_cleaning.py
	#python schedule_stop_times_with_geom.py -- do this for easier mapping 
    
    
rt_msa_stop_report:
	python download_warehouse_tables.py
    # things that should be done before report is run, but probably can't make it into dbt model
    # save it out as parquet
	python prep_data.py
	python deploy_portfolio_yaml.py rt_msa_stops
	cd ../portfolio/ && pip install -r requirements.txt && cd ../ 
	python portfolio/portfolio.py build rt_trip_updates_stop_metrics
	gcloud auth login --login-config=iac/login.json && gcloud config set project cal-itp-data-infra # will need to authenticate this step
	python portfolio/portfolio.py build rt_trip_updates_stop_metrics --no-execute-papermill --deploy
	python portfolio/portfolio.py index --deploy