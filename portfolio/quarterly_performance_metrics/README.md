# Mass Transit Performance Objectives

## MT.PO.01: increase total amount of service on the SHN and reliability of that service by 2024

Transit routes along the SHN can be categorized into 3 groups:

1. **On SHN** - where at least 20% of the transit route runs the SHN (within 50 ft) 
1. **Intersects SHN** - where at least 35% of the transit route runs within 0.5 mile of the SHN.
1. **Other** - all other transit routes.


Initially presented for the Planning and Modal Advisory Committee (PMAC).

## Workflow
### Data Generation

1. [Generate processed data for categories and service hours](https://github.com/cal-itp/data-analyses/blob/main/quarterly_performance_objective/A1_generate_routes_on_shn_data.py) with GTFS schedule data
1. [Categorize routes into 3 groups](https://github.com/cal-itp/data-analyses/blob/main/quarterly_performance_objective/A2_categorize_routes.py)
1. [Generate endpoint data processed data](https://github.com/cal-itp/data-analyses/blob/main/quarterly_performance_objective/A3_generate_endpoint_delay.py) with GTFS real-time data
1. [Merge service hours and endpoint delay](https://github.com/cal-itp/data-analyses/blob/main/quarterly_performance_objective/A4_route_service_hours_delay.py)

### Helper Scripts for Reports
1. [data prep functions](https://github.com/cal-itp/data-analyses/blob/main/quarterly_performance_objective/B1_report_metrics.py)
1. [chart functions](https://github.com/cal-itp/data-analyses/blob/main/quarterly_performance_objective/B2_report_charts.py)

### Reports

Create a report of current quarter's snapshot as well as a historical comparison of quarterly metrics report.

1. [current quarter's snapshot](https://github.com/cal-itp/data-analyses/blob/main/quarterly_performance_objective/current_quarter_report.ipynb)
1. [historical comparison](https://github.com/cal-itp/data-analyses/blob/main/quarterly_performance_objective/historical_report.ipynb)