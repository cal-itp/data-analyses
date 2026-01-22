digest_report:
	#python something_download_script.py
	make gtfs_digest_data
	make portfolio_deploy

portfolio_deploy:
	#python _yml_gtfs_digest_orgs.py # run this after the files are created in merge_*
	python deploy_portfolio_yaml.py

gtfs_digest_data:
	python _prep_crosswalk_ntd.py
	python _load_gtfs_data.py
	python _prep_gtfs_data.py
	python _publish_public_data.py
    