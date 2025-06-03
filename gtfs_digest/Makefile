assemble_data:
	make asssemble route_direction_data
	make assemble_operator_data 
	make portfolio_deploy
    
assemble_route_direction_data:
	python merge_data.py
	python _route_dir_data_prep.py  
	python quarterly_rollup.py # for now, quarterly only affects route-direction, since operator filters to recent date

assemble_operator_data:
	python merge_operator_data.py
	python _operator_data_prep.py # filters only to recent date for each operator
	python merge_operator_service.py   
    
portfolio_deploy:
	python deploy_portfolio_yaml.py
	python _yml_gtfs_digest_orgs.py
	python publish_public_data.py
