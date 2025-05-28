assemble_data:
	make asssemble route_direction_data
	make assemble_operator_data 
	make portfolio_deploy
    
assemble_route_direction_data:
	python merge_data.py
	python viz_data_prep.py # save out the output that's aggregated   
	python quarterly_rollup.py # for now, quarterly only affects route-direction, since operator filters to recent date

assemble_operator_data:
	python merge_operator_data.py
	python aggregate_filter_operator_data.py # filters only to recent date for each operator
	python merge_operator_service.py # is this filtered to recent date too?    

portfolio_deploy:
	python deploy_portfolio_yaml.py
	python _yml_gtfs_digest_orgs.py
	python publish_public_data.py
