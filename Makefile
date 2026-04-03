# Run this in data-analyses
# To specify different Makefile: make some_command -f Makefile2

# Build and Deploy Production Portfolio Site with:
# make build_production_portfolio_site site='MY_SITE_IDENTIFIER'
build_production_portfolio_site:
	cd portfolio/ && pip install -r requirements.txt && cd ../
	python portfolio/portfolio.py clean $(site)
	python portfolio/portfolio.py build $(site)
	gcloud auth login --login-config=iac/login.json && gcloud config set project cal-itp-data-infra
	python portfolio/portfolio.py build $(site) --no-execute-papermill --deploy --target production
	git add portfolio/sites/$(site).yml
	#make build_production_portfolio_index

# Build and Deploy Staging Portfolio Site with:
# make build_staging_portfolio_site site='MY_SITE_IDENTIFIER'
build_staging_portfolio_site:
	cd portfolio/ && pip install -r requirements.txt && cd ../
	python portfolio/portfolio.py clean $(site)
	gcloud auth login --login-config=iac/login.json
	python portfolio/portfolio.py build $(site)
	python portfolio/portfolio.py build $(site) --no-execute-papermill --deploy --target staging
	#make build_staging_portfolio_index

# Build and Deploy Production Portfolio Index page with:
build_production_portfolio_index:
	python portfolio/portfolio.py index --deploy --prod

# Build and Deploy Staging Portfolio Index page (including test portfolios) with:
build_staging_portfolio_index:
	python portfolio/portfolio.py index --deploy --no-prod

# Build locally the Portfolio Site without deploying with:
# make test_build_portfolio_site site='MY_SITE_IDENTIFIER'
test_build_portfolio_site:
	python portfolio/portfolio.py clean $(site)
	#gcloud auth login --login-config=iac/login.json
	python portfolio/portfolio.py build $(site)

# Delete Local Site folder with:
# make clean_portfolio_site site='MY_SITE_IDENTIFIER'
clean_portfolio_site:
	python portfolio/portfolio.py clean $(site)

# Delete Local Site folder and Remove Portfolio site with:
# make remove_portfolio_site site='MY_SITE_IDENTIFIER'
remove_portfolio_site:
	python portfolio/portfolio.py clean $(site)
	git rm portfolio/sites/$(site).yml

build_ntd_report:
	$(eval export site = ntd_monthly_ridership)
	cd ntd/monthly_ridership_report/ && python deploy_portfolio_yaml.py && cd ..
	make build_production_portfolio_site

build_ntd_annual_report:
	$(eval export site = ntd_annual_ridership_report)
	cd ntd/annual_ridership_report/ && python deploy_portfolio_yaml.py && cd ..
	make build_production_portfolio_site

build_new_transit_metrics_report:
	$(eval export site = new_transit_metrics)
	cd ntd/new_transit_metrics/ && python deploy_portfolio_yaml.py && cd ..
	make build_production_portfolio_site

build_gtfs_digest:
	$(eval export site = gtfs_digest)
	cd rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env && cd ..
	cd gtfs_digest/ && make digest_report && cd ..
	make build_production_portfolio_site

build_district_digest:
	$(eval export site = district_digest)
	cd rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env && cd ..
	cd portfolio/ && pip install -r requirements.txt && cd ../
	cd gtfs_digest/ && python deploy_district_yaml.py district && cd ..
	make build_production_portfolio_site

build_legislative_district_digest:
	$(eval export site = legislative_district_digest)
	cd rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env && cd ..
	cd gtfs_digest/ && python deploy_district_yaml.py legislative_district && cd ..
	make build_production_portfolio_site

build_fund_split:
	$(eval export site = sb125_fund_split_analysis)
	pip install -r portfolio/requirements.txt
	make build_production_portfolio_site

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
	make add_precommit

install_portfolio:
	cd portfolio/ && pip install -r requirements.txt && cd ../
