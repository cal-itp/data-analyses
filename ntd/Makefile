# Make sure update_vars.py and ntd_dates.py are updated

staging_monthly_export:
	# make report data
	cd monthly_ridership_report && python monthly_ridership_by_rtpa.py
	# make site yaml
	$(eval export site = ntd_monthly_ridership)
	cd monthly_ridership_report && python deploy_portfolio_yaml.py
	# build report html
	cd ../portfolio/ && pip install -r requirements.txt && cd ../
	python ../portfolio/portfolio.py clean $(site)
	python ../portfolio/portfolio.py build $(site)
	gcloud auth login --login-config=iac/login.json && gcloud config set project cal-itp-data-infra
	# deploy to staging site
	python ../portfolio/portfolio.py deploy-site $(site) --target staging
	git add portfolio/sites/$(site).yml

ntd_monthly_export:
	cd monthly_ridership_report && python monthly_ridership_by_rtpa.py
	cd ../ && make build_ntd_report -f Makefile

annual_ntd_report:
	cd annual_ridership_report && python annual_ridership_script.py
	cd ../ && make build_ntd_annual_report -f Makefile
