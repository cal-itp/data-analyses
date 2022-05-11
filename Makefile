# Run this in data-analyses and specify Makefile: make build_parallel_corridors -f portfolio_Makefile
build_parallel_corridors:
	#pip install -r portfolio/requirements.txt
	#git rm portfolio/parallel_corridors/ -rf
	rm portfolio/parallel_corridors/ -rf
	#python bus_service_increase/deploy_portfolio_yaml.py    
	python portfolio/portfolio.py build parallel_corridors --deploy 
	git add portfolio/parallel_corridors/district_*/ portfolio/parallel_corridors/*.yml portfolio/parallel_corridors/*.md 
	git add portfolio/sites/ 
    #--config=./portfolio/test-analyses.yml