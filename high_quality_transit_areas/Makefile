download_hqta_data:
	python download_data.py
	python operators_for_hqta.py
	python compile_operators_data.py    
	python A2_combine_stops.py
    
grab_corridors_and_clip:
	python B1_create_hqta_segments.py
	python B2_sjoin_stops_to_segments.py
	python C1_prep_for_clipping.py
	python C2_clip_bus_intersections.py
	python C4_create_bus_hqta_types.py

export_data:
	python D1_assemble_hqta_points.py
	python D2_assemble_hqta_polygons.py 