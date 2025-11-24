# Make sure update_vars.py and ntd_dates.py are updated
ntd_monthly_export:
	cd monthly_ridership_report && python monthly_ridership_by_rtpa.py
	cd ../ && make build_ntd_report -f Makefile

annual_ntd_report:
	cd annual_ridership_report && python annual_ridership_script.py
	cd ../ && make build_ntd_annual_report -f Makefile
