digest_report:
	#python something_download_script.py
	make prep_for_viz
	make portfolio_deploy

prep_for_viz:
	python _operator_grain_route_dir_data_prep.py
	python _operator_grain_data_prep.py # filters only to recent date for each operator

portfolio_deploy:
	python _yml_gtfs_digest_orgs.py # run this after the files are created in merge_*
	python deploy_portfolio_yaml.py
	python publish_public_data.py
