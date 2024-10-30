build_test_portfolio:
	cd ../ && pip install -r portfolio/requirements.txt
	python portfolio/portfolio.py clean REPLACE_YML_NAME
	python portfolio/portfolio.py build REPLACE_YML_NAME --deploy
	git add portfolio/REPLACE_YML_NAME/district_*/ portfolio/REPLACE_YML_NAME/*.yml portfolio/REPLACE_YML_NAME/*.md
	git add portfolio/sites/REPLACE_YML_NAME.yml