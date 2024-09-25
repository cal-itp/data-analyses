hqta_data:
	python rail_ferry_brt_stops.py
	python create_hqta_segments.py
	python sjoin_stops_to_segments.py
	python prep_pairwise_intersections.py
	python get_intersections.py
	python create_bus_hqta_types.py
	python assemble_hqta_points.py
	python assemble_hqta_polygons.py 

# Only need this is operator input changes
# For now, Muni sent over a date-versioned list of stops
# they want included as BRT
add_operator_input:
	python operator_input.py
