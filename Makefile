# Run this in data-analyses
# To specify different Makefile: make build_parallel_corridors -f Makefile

build_portfolio_site:
	cd portfolio/ && pip install -r requirements.txt && cd ../
	#need git rm because otherwise, just local removal, but git change is untracked
	#git rm portfolio/$(site)/ -rf
	python portfolio/portfolio.py clean $(site)
	python portfolio/portfolio.py build $(site) --deploy 
	git add portfolio/sites/$(site).yml     
	#make production_portfolio

git_check_sections:
	git add portfolio/$(site)/*.ipynb # this one is most common, where operators nested under district

git_check_no_sections:
	git add portfolio/$(site)/district_*/*.ipynb # this one less common, but it's district pages only

remove_portfolio_site:
	python portfolio/portfolio.py clean $(site)
	git rm portfolio/sites/$(site).yml
	git rm portfolio/$(site)/ -rf


remove_competitive_corridors:
	$(eval export site = competitive_corridors) 
	make remove_portfolio_site

    
build_ntd_report:
	$(eval export site = ntd_monthly_ridership)
	cd bus_service_increase/ && make setup_bus_service_utils && cd ..
	cd ntd/ && python deploy_portfolio_yaml.py && cd ..   
	make build_portfolio_site

build_gtfs_digest:
	$(eval export site = gtfs_digest)
	#cd rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env && cd ..
	#cd gtfs_digest/ && python deploy_portfolio_yaml.py && make assemble_data && cd ..   
	cd gtfs_digest/ && python deploy_portfolio_yaml.py && cd ..       
	make build_portfolio_site
	make git_check_sections

build_district_digest:
	$(eval export site = district_digest)
	#cd rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env && cd ..
	cd gtfs_digest/ && python deploy_district_yaml.py district && cd .. 
	make build_portfolio_site 
	make git_check_no_sections
    
build_legislative_district_digest:
	$(eval export site = legislative_district_digest)
	#cd rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env && cd ..
	cd gtfs_digest/ && python deploy_district_yaml.py legislative_district && cd .. 
	make build_portfolio_site 
	make git_check_no_sections
    
    
build_starterkit_ha:
	$(eval export site = ha_starterkit_district)
	pip install -r portfolio/requirements.txt
	make build_portfolio_site 
	make git_check_no_sections
    
build_starterkit_LASTNAME:
	$(eval export site = YOUR_SITE_NAME)
	pip install -r portfolio/requirements.txt
	make build_portfolio_site 
	make git_check_no_sections

build_fund_split:
	$(eval export site = sb125_fund_split_analysis)
	pip install -r portfolio/requirements.txt
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
