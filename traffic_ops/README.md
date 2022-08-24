# README

Traffic Ops had a request for all transit routes and transit stops to be published in the open data portal. 

## Scripts
1. [prep_data](./prep_data.py): prep warehouse queries for GTFS schedule tables
1. [create_routes_data](./create_routes_data.py): functions to assemble routes that appear in `shapes` and routes that don't appear in `shapes` (use `trips` and `stops` to string together bus stop locations to create the route path)
1. [create_stops_data](./create_stops_data.py): functions to assemble stop data
1. **[make_routes_stops_shapefiles](./make_routes_stops_shapefiles.py)**: run this script once a month to create the `ca_transit_routes` (line geometry) and `ca_transit_stops` (point geometry) datasets in the open data portal.

## Open Data
* [Metadata](../open_data/traffic_ops.py) associated with `ca_transit_routes` and `ca_transit_stops` are in the `open_data` directory.
