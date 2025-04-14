assemble_data:
	python merge_data.py
	#python merge_operator_data.py
	python merge_operator_service.py    
	python publish_public_data.py
	python quarterly_rollup.py
	python _yml_gtfs_digest_orgs.py
	python deploy_portfolio_yaml.py
    
new_stuff:
	# produce the yaml first before deploying, keep running list of errors
	#python quarterly_rollup.py
	#python visualization_wrangling.py   
	# Add a deploy step for portfolios
	#python _yml_gtfs_digest_orgs
