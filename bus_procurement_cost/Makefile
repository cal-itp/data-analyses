# runs all scripts for bus procurement cost
all_bus_scripts:
	python fta_data_cleaner.py
	python tircp_data_cleaner.py
	python dgs_data_cleaner.py
	python cost_per_bus_cleaner.py

	#execute NB
	jupyter nbconvert --to notebook --execute --inplace cost_per_bus_analysis.ipynb
    
	#convert NB to HTML then to PDF
	jupyter nbconvert --to html --no-input --no-prompt cost_per_bus_analysis.ipynb
	weasyprint cost_per_bus_analysis.html cost_per_bus_analysis_weasyprint.pdf
    
