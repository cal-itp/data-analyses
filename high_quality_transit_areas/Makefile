download_hqta_data:
	python operators_for_hqta.py
	python A2_combine_stops.py
    
create_hqta_types:
	python B1_create_hqta_segments.py
	python B2_sjoin_stops_to_segments.py
	python C1_prep_pairwise_intersections.py
	python C2_get_intersections.py
	python C3_create_bus_hqta_types.py

export_data:
	python D1_assemble_hqta_points.py
	python D2_assemble_hqta_polygons.py 
    
hqta_data:
	make download_hqta_data && make create_hqta_types && make export_data 