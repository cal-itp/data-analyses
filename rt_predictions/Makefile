# Roughly, data cleaning steps are:
stop_times_poc_cleaned_data:
    cd ../rt_segment_speed_utils/ && pip install -r requirements.txt && cd ..
    #query_materialized_tables.py --save outputs in GCS
    python more_cleaning.py
    #python schedule_stop_times_with_geom.py -- do this for easier mapping

stop_report:
    python download_warehouse_tables.py
    # things that should be done before report is run, but probably can't make it into dbt model
    # save it out as parquet
    python prep_data_for_viz.py
    python deploy_portfolio_yaml.py rt_msa_stops --month 2026-01-01
    cd ../portfolio/ && pip install -r requirements.txt && cd ../
    gcloud auth login --login-config=iac/login.json && gcloud config set project cal-itp-data-infra # will need to authenticate this step
    python portfolio/portfolio.py build rt_trip_updates_stop_metrics --hide-title-block --deploy --target staging
    python portfolio/portfolio.py index --deploy --no-prod

operator_report:
    #python download_warehouse_tables.py
    # things that should be done before report is run, but probably can't make it into dbt model
    # save it out as parquet
    #python prep_data_for_viz.py
    #python deploy_portfolio_yaml.py rt_msa_operators --month 2026-01-01
    #cd ../portfolio/ && pip install -r requirements.txt && cd ../
    cd ../ && python portfolio/portfolio.py clean test_tu
    gcloud auth login --login-config=iac/login.json && gcloud config set project cal-itp-data-infra # will need to authenticate this step
    #python portfolio/portfolio.py build test_tu
    # still set H1, hide title instead
    python portfolio/portfolio.py build test_tu --hide-title-block --deploy --target staging
    python portfolio/portfolio.py index --deploy --no-prod

test_stop_report:
    cd ../ && python portfolio/portfolio.py clean test_tu_stops
    python portfolio/portfolio.py build test_tu_stops --hide-title-block --deploy --target staging
    python portfolio/portfolio.py index --deploy --no-prod

test_operator_report:
    @echo "something"
    #cd ../ && python portfolio/portfolio.py clean test_tu
    #python portfolio/portfolio.py build test_tu --hide-title-block --deploy --target staging
    #python portfolio/portfolio.py index --deploy --no-prod
