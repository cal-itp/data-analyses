digest_report:
	#python something_download_script.py
	make gtfs_digest_data
	make portfolio_deploy

portfolio_deploy:
	#python _yml_gtfs_digest_orgs.py # run this after the files are created in merge_*
	uv run python deploy_portfolio_yaml.py

gtfs_digest_data:
	uv run python _prep_crosswalk_ntd.py
	uv run python _load_gtfs_data.py
	uv run python _prep_gtfs_data.py
	uv run python _publish_public_data.py
    