# Run this in data-analyses and specify Makefile: make build_parallel_corridors -f portfolio_Makefile
build_parallel_corridors:
	pip install -r portfolio/requirements.txt
	git rm portfolio/parallel_corridors/ -rf
	rm portfolio/parallel_corridors/ -rf
	python bus_service_increase/deploy_portfolio_yaml.py    
	python portfolio/portfolio.py build parallel_corridors --deploy 
	git add portfolio/parallel_corridors/district_*/ portfolio/parallel_corridors/*.yml portfolio/parallel_corridors/*.md 
	git add portfolio/sites/ 
    #--config=./portfolio/test-analyses.yml
    
    
build_highway_corridors:
	pip install -r portfolio/requirements.txt
	#git rm portfolio/highway_corridors/ -rf
	rm portfolio/highway_corridors/ -rf
	python portfolio/portfolio.py build highway_corridors --deploy 
	git add portfolio/highway_corridors/state_ca/ portfolio/highway_corridors/*.yml portfolio/highway_corridors/*.md 
	git add portfolio/sites/ 
    
    
build_rt_parallel:
	pip install -r portfolio/requirements.txt
	#git rm portfolio/rt_parallel/ -rf
	rm portfolio/rt_parallel/ -rf
	python portfolio/portfolio.py build rt_parallel --deploy 
	git add portfolio/rt_parallel/ portfolio/rt_parallel/*.yml portfolio/rt_parallel/*.md 
	git add -- portfolio/rt_parallel/itp_id_*/ portfolio/rt_parallel/*.yml portfolio/rt_parallel/*.md #:!portfolio/rt_parallel/_build
	git add portfolio/sites/ 
