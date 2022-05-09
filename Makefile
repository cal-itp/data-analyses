# Run this in data-analyses and specify Makefile: make build_parallel_corridors -f portfolio_Makefile
build_parallel_corridors:
	#pip install -r portfolio/requirements.txt
	#git rm portfolio/parallel_corridors/ -rf
	rm portfolio/parallel_corridors/ -rf
	python bus_service_increase/deploy_portfolio_yaml.py    
	python portfolio/portfolio.py build parallel_corridors --deploy 
	git add portfolio/parallel_corridors/district_*/ portfolio/parallel_corridors/*.yml portfolio/parallel_corridors/*.md 
	git add portfolio/analyses.yml
    #--config=./portfolio/test-analyses.yml

build_highway_corridors:
	#pip install -r portfolio/requirements.txt
	#git rm portfolio/highway_corridors/ -rf
	#rm portfolio/highway_corridors/ -rf
	python portfolio/portfolio.py build highway_corridors --config=./portfolio/test-analyses.yml --deploy
	git add portfolio/highway_corridors/*.yml portfolio/highway_corridors/*.md 
	git add portfolio/analyses.yml