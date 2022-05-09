# README

Refer here for [visualizations index](./visualizations_index.md)

## Bus Service Increase

Scripts associated with data creation and visualization / output generation. The bus service increase analysis is done for a selected Thursday, Saturday, and Sunday in October 2021, and the processed dataset is fairly static. 

### Data Creation
1. [warehouse_queries](./warehouse_queries.py): for route-level and tract-level analysis
1. [create_calenviroscreen_lehd_data](./create_calenviroscreen_lehd_data.py): import CalEnviroScreen and LEHD data at tract-level
1. [create_analysis_data](./create_analysis_data.py): create analysis, processed dataset


### Visualization / Outputs
1. [setup_service_increase](./setup_service_increase_data.py): calculate additional trips, service hours, annual service hours, and capital expenditures needed by operator
1. [setup_viz_data](./setup_viz_data.py): generate metrics (service density, or service per 1000 jobs and pop) for tract-level visualizations
1. [setup_tract_charts](./setup_tract_charts.py): functions for altair charts, seaborn heatmaps
1. [setup_tract_maps](./setup_tract_maps.py): functions for folium maps
1. [make_tract_viz](./make_tract_viz.py): combine charts and maps and produce all visualizations needed


## Parallel Corridors

### Data Creation

1. [create_parallel_corridors](./create_parallel_corridors.py): find transit routes that are considered parallel to State Highway Network
1. [setup_corridors_stats](./setup_corridors_stats.py): aggregate summary stats by operator or highway route
1. Planning and Modal Advisory Committee (PMAC) analysis: [script](current_routes.py) and [analysis notebook](./pmac.ipynb) -- these were run with latest dates prior to `dim_shapes` table creation
1. [setup_parallel_trips_with_stops](./setup_parallel_trips_with_stops.py): set up parallel routes, select 1 representative trip per route (fastest trip), to compare against car 

### Visualization / Outputs

1. [setup_gmaps](./setup_gmaps.py): set up df to be used in Google Directions API with origin, destination, waypoint, and departure times
1. [make_gmaps_requests](./make_gmaps_requests.py): make Google Directions API requests and cache results in GCS
1. [make_gmaps_results](./make_gmaps_results.py): grab cached JSON results and assemble into df
1. [make_stripplot_data](./make_stripplot_data.py): merge competitive routes info from Google Directions API back to trip-level data and wrangle data for making stripplots showing trip variability.
