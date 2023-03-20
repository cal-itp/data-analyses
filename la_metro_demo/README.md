# README

We want to figure out which LA Metro division to demo our contactless payments technology. We have route, route-division, and division level data provided by LA Metro on the percent of transactions that were cash.

We also want to factor in how much of that bus route travels through an high-equity-concern community (scoring top 1/3 on CalEnviroScreen). One shape is selected for each route to spatially join against the CalEnviroScreen tract data.

Build a table that is route-level: 
| route | pct_cash | pct_equity | transactions | division |
|-------|----------|------------|--------------|----------|
|       |          |            |              |          |
|       |          |            |              |          |

We can categorize routes into scoring high / low (above median or below median) on the 2 metrics. The division with the most routes that scored high on both will be selected.

## Workflow
1. Import LA Metro's datasets: `python A1_import_data.py`
2. Visualizations: `02-select-division.ipynb` (functions are referenced in other scripts).