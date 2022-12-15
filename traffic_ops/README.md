# README

Traffic Ops had a request for all transit routes and transit stops to be published in the open data portal. 

## Scripts
1. [prep_data](./prep_data.py): prep warehouse queries for GTFS schedule tables
1. [create_routes_data](./create_routes_data.py): functions to assemble routes that appear in `shapes`
1. [create_stops_data](./create_stops_data.py): functions to assemble stop data

In terminal, use `Makefile` to execute workflow: `make gtfs_schedule_geospatial_open_data`

## Open Data
* [Metadata](../open_data/traffic_ops.py) associated with `ca_transit_routes` and `ca_transit_stops` are in the `open_data` directory.
