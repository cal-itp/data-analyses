download_hqta_data:
	python download_trips.py
	python download_stops.py
	python download_shapes.py
	python download_stop_times.py
	python operators_for_hqta.py
	python A2_combine_stops.py
    
grab_corridors_and_find_intersections:
	python B1_create_hqta_segments.py
	python B2_sjoin_stops_to_segments.py
	python C1_prep_pairwise_intersections.py
	python C2_get_intersections.py
	python C3_create_bus_hqta_types.py

export_data:
	python D1_assemble_hqta_points.py
	python D2_assemble_hqta_polygons.py 