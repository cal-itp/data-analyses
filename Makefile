# Run this in data-analyses
# To specify different Makefile: make build_parallel_corridors -f Makefile


build_portfolio_site:
	cd portfolio/ && pip install -r requirements.txt && cd ../
	#need git rm because otherwise, just local removal, but git change is untracked
	git rm portfolio/$(site)/ -rf
	python portfolio/portfolio.py clean $(site)
	python portfolio/portfolio.py build $(site) --deploy 
	git add portfolio/$(site)/*.yml portfolio/$(site)/*.md  
	git add portfolio/$(site)/*.ipynb 
	git add portfolio/sites/$(site).yml 
	#make production_portfolio


build_competitive_corridors:
	$(eval export site = competitive_corridors)
	cd bus_service_increase/ && make setup_bus_service_utils && cd ..
	python bus_service_increase/deploy_portfolio_yaml.py   
	make build_portfolio_site
    #--config=./portfolio/test-analyses.yml

build_dla_reports:
	$(eval export site = dla)
	cd dla/ && pip install -r requirements.txt && cd ..
	make build_portfolio_site
	git add portfolio/dla/district_*/ 
    
build_quarterly_performance_metrics:
	$(eval export site = quarterly_performance_metrics)
	cd bus_service_increase/ && make setup_bus_service_utils && cd ..
	make build_portfolio_site
    
build_ntd_report:
	$(eval export site = ntd_monthly_ridership)
	cd bus_service_increase/ && make setup_bus_service_utils && cd ..
	cd ntd/ && python deploy_portfolio_yaml.py && cd ..   
	make build_portfolio_site

build_route_speeds:
	$(eval override site = route_speeds)
	cd rt_segment_speeds / && make pip install -r requirements.txt && cd ..
	cd rt_segment_speeds/ && python deploy_portfolio_yaml.py && cd ..   
	make build_portfolio_site

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

production_portfolio:
	python portfolio/portfolio.py index --deploy --prod

# Create .egg to upload to dask cloud cluster
egg_modules:
	cd ~/data-analyses/rt_segment_speeds && python setup.py bdist_egg && cd ..
