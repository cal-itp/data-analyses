quarterly_performance_report:
	#cd ../bus_service_increase && $(MAKE) -C make setup_bus_service_utils
	#cd ../rt_delay && $(MAKE) -C make setup_rt_analysis
	python A1_generate_routes_on_shn_data.py
	python A2_categorize_routes.py
	python A3_generate_endpoint_delay.py
	python A4_route_service_hours_delay.py
    