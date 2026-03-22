# Run this in data-analyses
# To specify different Makefile: make build_parallel_corridors -f Makefile

add_precommit:
	pip install pre-commit
	pre-commit install
	#pre-commit run --all-files

install_env:
	cd ~/data-analyses/_shared_utils && make setup_env && cd ..
	#cd bus_service_increase/ && make setup_bus_service_utils && cd ..
	#cd rt_delay/ && make setup_rt_analysis && cd ..
	cd rt_segment_speeds && pip install -r requirements.txt && cd ..
	make add_precommit

# install portfolio dependencies with:
install_portfolio:
	cd ~/data-analyses/_shared_utils && make setup_env && cd ..
	cd portfolio/ && pip install -r requirements.txt && cd ../
	make add_precommit

# build site with:
# make build_portfolio_site site='MY_SITE_IDENTIFIER'
build_portfolio_site:
	cd portfolio/ && pip install -r requirements.txt && cd ../
	python portfolio/portfolio.py clean $(site)
	python portfolio/portfolio.py build $(site)
	gcloud auth login --login-config=iac/login.json && gcloud config set project cal-itp-data-infra
	python portfolio/portfolio.py build $(site) --no-execute-papermill --deploy
	git add portfolio/sites/$(site).yml
	#make deploy_production_portfolio_index

build_staging_portfolio_site:
	python portfolio/portfolio.py clean $(site)
	python portfolio/portfolio.py build $(site)
	gcloud auth login --login-config=iac/login.json && gcloud config set project cal-itp-data-infra-staging
	python portfolio/portfolio.py build $(site) --no-execute-papermill --deploy

# clean site locally with:
# make clean_portfolio_site site='MY_SITE_IDENTIFIER'
clean_portfolio_site:
	python portfolio/portfolio.py clean $(site)

# build site with:
# make build_portfolio_site site='MY_SITE_IDENTIFIER'
build_portfolio_site:
	python portfolio/portfolio.py build $(site)

remove_portfolio_site:
	python portfolio/portfolio.py clean $(site)
	git rm portfolio/sites/$(site).yml

build_ntd_report:
	$(eval export site = ntd_monthly_ridership)
	cd ntd/monthly_ridership_report/ && python deploy_portfolio_yaml.py && cd ..
	make build_portfolio_site

build_ntd_annual_report:
	$(eval export site = ntd_annual_ridership_report)
	cd ntd/annual_ridership_report/ && python deploy_portfolio_yaml.py && cd ..
	make build_portfolio_site

build_new_transit_metrics_report:
	$(eval export site = new_transit_metrics)
	cd ntd/new_transit_metrics/ && python deploy_portfolio_yaml.py && cd ..
	make build_portfolio_site

build_gtfs_digest:
	$(eval export site = gtfs_digest)
	cd rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env && cd ..
	cd gtfs_digest/ && make digest_report && cd ..
	make build_portfolio_site

build_district_digest:
	$(eval export site = district_digest)
	cd rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env && cd ..
	make install_portfolio
	cd gtfs_digest/ && python deploy_district_yaml.py district && cd ..
	make build_portfolio_site

build_legislative_district_digest:
	$(eval export site = legislative_district_digest)
	cd rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env && cd ..
	cd gtfs_digest/ && python deploy_district_yaml.py legislative_district && cd ..
	make build_portfolio_site

build_fund_split:
	$(eval export site = sb125_fund_split_analysis)
	pip install -r portfolio/requirements.txt
	make build_portfolio_site

# Add to _.bash_profile outside of data-analyses
#alias go='cd ~/data-analyses/portfolio && pip install -r requirements.txt && cd #../_shared_utils && make setup_env && cd ..'

deploy_staging_portfolio_index:
	gcloud auth login --login-config=iac/login.json
	python portfolio/portfolio.py deploy-index --target staging

deploy_production_portfolio_index:
	python portfolio/portfolio.py index --deploy

# deploy site to Staging with:
# make deploy_staging_portfolio site='MY_SITE_IDENTIFIER'
deploy_staging_portfolio:
	gcloud auth login --login-config=iac/login.json
	#make deploy_staging_portfolio_index
	python portfolio/portfolio.py deploy-site $(site) --target staging

# Create .egg to upload to dask cloud cluster
egg_modules:
	cd ~/data-analyses/rt_segment_speeds && python setup.py bdist_egg && cd ..
