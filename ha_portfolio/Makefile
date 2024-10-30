build_test_portfolio:
	cd ../ && pip install -r requirements.txt
	cd ../ && python portfolio/portfolio.py clean ha_starterkit_district 
	cd ../ && python portfolio/portfolio.py build ha_starterkit_district --deploy
	cd ../ && python portfolio/portfolio.py build ha_starterkit_district --deploy
	git add portfolio/ha_starterkit_district/district_*/ portfolio/ha_starterkit_district/*.yml portfolio/ha_starterkit_district/*.md
	git add portfolio/sites/ha_starterkit_district.yml