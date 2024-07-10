# runs all scripts for bus procurement cost
all_bus_scripts:
	python _01_fta_data_cleaner.py
	python _02_tircp_data_cleaner.py
	python _03_dgs_data_cleaner.py
	python _04_cost_per_bus_cleaner.py

	#execute NB
	jupyter nbconvert --to notebook --execute --inplace _05_cost_per_bus_analysis.ipynb
    
	#convert NB to HTML then to PDF
	jupyter nbconvert --to html --no-input --no-prompt _05_cost_per_bus_analysis.ipynb
	pip install WeasyPrint
	weasyprint _05_cost_per_bus_analysis.html cost_per_bus_analysis_weasyprint.pdf
    
