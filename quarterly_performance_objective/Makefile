quarterly_performance_report:
	#cd ../rt_segment_speeds/ && make && pip install -r requirements.txt && cd .. 
	python clean_data.py
	python compile_time_series.py
	#cd ../ && make build_quarterly_performance_metrics -f Makefile
    
quarto_report:
    # this renders as html
	#quarto render report.ipynb --execute 
    # to convert ipynb to qmd
	quarto convert report.ipynb 
    # to convert qmd to ipynb
	quarto convert report.qmd     
    #https://quarto.org/docs/computations/parameters.html#jupyter couldn't get this to work
	#quarto render report.qmd --execute-params params.yml
	quarto publish report.qmd