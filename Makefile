# Run this in data-analyses
# To specify different Makefile: make build_parallel_corridors -f Makefile
build_competitive_corridors:
	#cd bus_service_increase/ && make setup_bus_service_utils && cd ..
	git rm portfolio/competitive_corridors/ -rf
	#need git rm because otherwise, just local removal, but git change is untracked
	python portfolio/portfolio.py clean competitive_corridors
	python bus_service_increase/deploy_portfolio_yaml.py   
	python portfolio/portfolio.py build competitive_corridors --deploy 
	git add portfolio/competitive_corridors/district_*/ portfolio/competitive_corridors/*.yml portfolio/competitive_corridors/*.md 
	git add portfolio/sites/competitive_corridors.yml 
	netlify deploy --site=cal-itp-data-analyses --dir=portfolio/competitive_corridors/_build/html/ --alias=competitive_corridors    
    #--config=./portfolio/test-analyses.yml

build_dla_reports:
	cd dla/ && pip install -r requirements.txt && cd ..
	git rm portfolio/dla/ -rf
	python portfolio/portfolio.py build dla --deploy 
	git add portfolio/dla/district_*/ portfolio/dla/*.yml portfolio/dla/*.md 
	git add portfolio/sites/dla.yml
	netlify deploy --site=cal-itp-data-analyses --dir=portfolio/dla/_build/html/ --alias=dla
    
build_quarterly_performance_metrics:
	#cd bus_service_increase/ && make setup_bus_service_utils && cd ..
	#git rm portfolio/quarterly_performance_metrics/ -rf
	#python portfolio/portfolio.py clean quarterly_performance_metrics
	python portfolio/portfolio.py build quarterly_performance_metrics --deploy 
	git add portfolio/quarterly_performance_metrics/*.ipynb portfolio/quarterly_performance_metrics/*.yml portfolio/quarterly_performance_metrics/*.md 
	netlify deploy --site=cal-itp-data-analyses --dir=portfolio/quarterly_performance_metrics/_build/html/ --alias=quarterly_performance_metrics    
	git add portfolio/sites/quarterly_performance_metrics.yml 

add_precommit:
	pip install pre-commit
	pre-commit install 
	#pre-commit run --all-files 


# Add to _.bash_profile outside of data-analyses
#alias go='cd ~/data-analyses/portfolio && pip install -r requirements.txt && cd #../_shared_utils && make setup_env && cd ..'

install_env:
	cd ~/data-analyses/_shared_utils && make setup_env && cd ..
	#cd bus_service_increase/ && make setup_bus_service_utils && cd ..
	#cd rt_delay/ && make setup_rt_analysis && cd ..    
	cd rt_segment_speeds && pip install -r requirements.txt && cd ..

# Create .egg to upload to dask cloud cluster
egg_modules:
	cd ~/data-analyses/rt_segment_speeds && python setup.py bdist_egg && cd ..
