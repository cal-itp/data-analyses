# Archives
## Parallel Corridors

The scripts here are archived. It uses v1 warehouse and the analysis has not been moved over to the v2 warehouse.

### Data Assembly

1. [setup_parallel_trips_with_stops](./D1_setup_parallel_trips_with_stops.py): set up parallel routes, select 1 representative trip per route (25th percentile trip), to compare against car 
1. [setup_gmaps](./D2_setup_gmaps.py): set up df to be used in Google Directions API with origin, destination, waypoint, and departure times
1. [make_gmaps_requests](./D3_make_gmaps_requests.py): make Google Directions API requests and cache results in GCS
1. [make_gmaps_results](./D4_make_gmaps_results.py): grab cached JSON results and assemble into df
1. [make_stripplot_data](./D5_make_stripplot_data.py): merge competitive routes info from Google Directions API back to trip-level data and wrangle data for making stripplots showing trip variability.
1. [definitions-competitive-viable](../D6_definitions-competitive-viable.ipynb): descriptives to justify cut-offs in competitive-parallel-routes report

## 100 Routes to Better Buses

The scripts here are archived. It uses v1 warehouse and the analysis has not been moved over to the v2 warehouse.

### Data Assembly
1. [Set variables](./E0_bus_oppor_vars.py): set variables needed for rest of analysis.
1. [Get bus routes on SHN](./E1_get_buses_on_shn.py): identify the routes that travel on the state highway network.
1. [Aggregated route stats](./E2_aggregated_route_stats.py): aggregate trip-level stats to the route-level.
1. [Calculate speeds for all operators](./E3_calculate_speeds_all_operators.py): Use outputs from speeds pipeline and get the first / last stop to get trip-level speeds.
1. [Highway segment stats](./E4_highway_segments_stats.py): cut highways into 5 mile segments and attach transit route stats.
1. [100 routes - highways](./E5_highway_processed_data_for_reports.py): identify transit deserts (where transit is not) and where transit is on the SHN.
