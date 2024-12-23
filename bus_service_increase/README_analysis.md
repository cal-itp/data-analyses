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
1. [service_increase_estimator](./A3_service_increase_estimator.ipynb): estimate service hours increase, capital expenditures to bring transit up to desired 15, 30, 60 min frequencies by urban/suburban/rural.
1. [bus_arrivals_by_tract](./B2_chart_bus_arrivals_by_tract.ipynb): charts and maps produced for tract-level population / service density against equity scores.


## Parallel Corridors

### Data Creation

1. [create_parallel_corridors](./create_parallel_corridors.py): find transit routes that are considered parallel to State Highway Network
1. [setup_corridors_stats](./setup_corridors_stats.py): aggregate summary stats by operator or highway route


### Reports
1. [parallel_corridors_utils](./parallel_corridors_utils.py): utility functions used in reports.  
1. [deploy_portfolio_yaml](./deploy_portfolio_yaml.py): programmatically set up jupyterbook yml file in [portfolio/sites/](../portfolio/sites/parallel_corridors.yml)
1. [competitive-parallel-routes](./competitive-parallel-routes.ipynb): parameterized report at operator-level showing which parallel routes are viable competitive routes to prioritize for service improvements
1. [publish_single_report](./publish_single_report.py): nbconvert notebook into html, then upload it to GitHub and host as GH pages
1. [highways-no-parallel-routes-gh](./highways-no-parallel-routes-gh.ipynb): unparameterized report at state-level showing highway corridors by district with no parallel routes. [Report here.](https://docs.calitp.org/data-analyses/bus_service_increase/img/highways-no-parallel-routes.html)