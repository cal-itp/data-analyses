# Run this in data-analyses
# To specify different Makefile: make some_command -f Makefile2

# IMPORTANT!
# If you are using `Prototype Image - 2026.3.18, Python 3.11` in JupyterHub, you need to add `uv run ` to the beginning of the commands in order to work.
# For example: Instead of `python portfolio/portfolio.py clean $(site)`, run `uv run python portfolio/portfolio.py clean $(site)`.

# Build and Deploy Production Portfolio Site with:
# make build_production_portfolio_site site='MY_SITE_IDENTIFIER'
build_production_portfolio_site:
	uv sync --group portfolio
	uv run python portfolio/portfolio.py clean $(site)
	uv run python portfolio/portfolio.py build $(site)
	gcloud auth login --login-config=iac/login.json && gcloud config set project cal-itp-data-infra
	uv run python portfolio/portfolio.py build $(site) --no-execute-papermill --deploy --target production
	git add portfolio/sites/$(site).yml
	#make build_production_portfolio_index

# Build and Deploy Staging Portfolio Site with:
# make build_staging_portfolio_site site='MY_SITE_IDENTIFIER'
build_staging_portfolio_site:
	uv sync --group portfolio
	uv run python portfolio/portfolio.py clean $(site)
	gcloud auth login --login-config=iac/login.json
	uv run python portfolio/portfolio.py build $(site)
	uv run python portfolio/portfolio.py build $(site) --no-execute-papermill --deploy --target staging
	#make build_staging_portfolio_index

# Build and Deploy Production Portfolio Index page with:
build_production_portfolio_index:
	uv run python portfolio/portfolio.py index --deploy --prod

# Build and Deploy Staging Portfolio Index page (including test portfolios) with:
build_staging_portfolio_index:
	uv run python portfolio/portfolio.py index --deploy --no-prod

# Build locally the Portfolio Site without deploying with:
# make test_build_portfolio_site site='MY_SITE_IDENTIFIER'
test_build_portfolio_site:
	uv run python portfolio/portfolio.py clean $(site)
	#gcloud auth login --login-config=iac/login.json
	uv run python portfolio/portfolio.py build $(site)

# Delete Local Site folder with:
# make clean_portfolio_site site='MY_SITE_IDENTIFIER'
clean_portfolio_site:
	uv run python portfolio/portfolio.py clean $(site)

# Delete Local Site folder and Remove Portfolio site with:
# make remove_portfolio_site site='MY_SITE_IDENTIFIER'
remove_portfolio_site:
	uv run python portfolio/portfolio.py clean $(site)
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
	cd gtfs_digest/ && make digest_report && cd ..
	make build_production_portfolio_site

build_district_digest:
	$(eval export site = district_digest)
	cd gtfs_digest/ && python deploy_district_yaml.py district && cd ..
	make build_production_portfolio_site

build_legislative_district_digest:
	$(eval export site = legislative_district_digest)
	cd gtfs_digest/ && python deploy_district_yaml.py legislative_district && cd ..
	make build_production_portfolio_site

build_fund_split:
	$(eval export site = sb125_fund_split_analysis)
	make build_production_portfolio_site

add_precommit:
	uv run pre-commit install
	#pre-commit run --all-files

# Add to _.bash_profile outside of data-analyses
#alias go='cd ~/data-analyses && uv sync'

install_env:
	#cd bus_service_increase/ && make setup_bus_service_utils && cd ..
	#cd rt_delay/ && make setup_rt_analysis && cd ..
	uv sync
	make add_precommit

install_portfolio:
	uv sync --group portfolio

# Create .egg to upload to dask cloud cluster
egg_modules:
	cd ~/data-analyses/rt_segment_speeds && python setup.py bdist_egg && cd ..
