# Run this in data-analyses
# To specify different Makefile: make build_parallel_corridors -f Makefile
build_parallel_corridors:
	pip install -r portfolio/requirements.txt
	#git rm portfolio/parallel_corridors/ -rf
	python portfolio/portfolio.py clean parallel_corridors
	python bus_service_increase/deploy_portfolio_yaml.py   
	python portfolio/portfolio.py build parallel_corridors --deploy 
	git add portfolio/parallel_corridors/district_*/ portfolio/parallel_corridors/*.yml portfolio/parallel_corridors/*.md 
	git add portfolio/sites/ 
    #--config=./portfolio/test-analyses.yml


build_dla_reports:
	pip install -r portfolio/requirements.txt
	git rm portfolio/dla/ -rf
	python portfolio/portfolio.py build dla --deploy 
	git add portfolio/dla/district_*/ portfolio/dla/*.yml portfolio/dla/*.md 
	git add portfolio/sites/dla.yml


build_hqta:
	pip install -r portfolio/requirements.txt
	git rm portfolio/hqta/ -rf
	python portfolio/portfolio.py clean hqta
	python portfolio/portfolio.py build hqta --deploy 
	git add portfolio/hqta/*.yml portfolio/hqta/*.md portfolio/hqta/*.ipynb
	git add portfolio/sites/ 