# runs all scripts for bus procurement cost
all_bus_scripts:
	python 01_fta_data_cleaner.py
	python 02_tircp_data_cleaner.py
	python 03_dgs_data_cleaner.py
	python 04_cost_per_bus_cleaner.py

	#execute NB
	jupyter nbconvert --to notebook --execute --inplace 05_cost_per_bus_analysis.ipynb
    
	#convert NB to HTML then to PDF
	jupyter nbconvert --to html --no-input --no-prompt 05_cost_per_bus_analysis.ipynb
	weasyprint 05_cost_per_bus_analysis.html cost_per_bus_analysis_weasyprint.pdf
    
