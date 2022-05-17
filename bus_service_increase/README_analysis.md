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
1. [setup_tract_charts](./setup_tract_charts.py): functions for altair charts, seaborn heatmaps
1. [make_tract_viz](./make_tract_viz.py): combine charts and maps and produce all visualizations needed


## Parallel Corridors

### Data Creation

1. [create_parallel_corridors](./create_parallel_corridors.py): find transit routes that are considered parallel to State Highway Network
1. [setup_corridors_stats](./setup_corridors_stats.py): aggregate summary stats by operator or highway route

### Planning and Modal Advisory Committee (PMAC)

1. [script](./D1_current_routes.py) and
1. [analysis notebook](./D2_pmac.ipynb)
Note: these were run with latest dates prior to `dim_shapes` table creation. 

### Data Assembly

1. [setup_parallel_trips_with_stops](./E1_setup_parallel_trips_with_stops.py): set up parallel routes, select 1 representative trip per route (fastest trip), to compare against car 
1. [setup_gmaps](./E2_setup_gmaps.py): set up df to be used in Google Directions API with origin, destination, waypoint, and departure times
1. [make_gmaps_requests](./E3_make_gmaps_requests.py): make Google Directions API requests and cache results in GCS
1. [make_gmaps_results](./E4_make_gmaps_results.py): grab cached JSON results and assemble into df
1. [make_stripplot_data](./E5_make_stripplot_data.py): merge competitive routes info from Google Directions API back to trip-level data and wrangle data for making stripplots showing trip variability.
1. [definitions-competitive-viable](./E6_definitions-competitive-viable.ipynb): descriptives to justify cut-offs in competitive-parallel-routes report


### Reports
1. [parallel_corridors_utils](./parallel_corridors_utils.py): utility functions used in reports.  
1. [deploy_portfolio_yaml](./deploy_portfolio_yaml.py): programmatically set up jupyterbook yml file in [portfolio/sites/](../portfolio/sites/parallel_corridors.yml)
1. [competitive-parallel-routes](./competitive-parallel-routes.ipynb): parameterized report at operator-level showing which parallel routes are viable competitive routes to prioritize for service improvements
1. [publish_single_report](./publish_single_report.py): nbconvert notebook () into html, then upload it to GitHub and host as GH pages
1. [highways-no-parallel-routes-gh](./highways-no-parallel-routes-gh.ipynb): unparameterized report at state-level showing highway corridors by district with no parallel routes (GH page deploy)
1. [highways-no-parallel-routes-jb](./highways-no-parallel-routes-jb.ipynb): unparameterized report at state-level showing highway corridors by district with no parallel routes (Jupyterbook deploy)
1. [highways-low-competitive-routes.ipynb](./highways-low-competitive-routes.ipynb): unparameterized report at state-level showing highway corridors by district with high parallel routes, but low competitive routes (GH page deploy)
