# README

## 5311

### Data Creation and Utils
1. [data_prep](./_data_prep.py): functions for cleaning our 3 data sources, merging, and aggregating them together. 
2. [utils](./_utils.py): functions for charts and aggregating the merged data frames. 
3. [agency crosswalk](./_agency_crosswalk.py): a crosswalk created to match agency names against the data sources, when the Cal ITP/NTD ID were not available. 
4. [add_inflation](./5311/add_inflation.ipynb)
5. [add_rtpa.ipynb](./5311/add_rtpa.ipynb)

### Analyses
1. [5311.ipynb](./5311/5311.ipynb): 5311 organizations first pass
2. [door_analysis](./door_analysis.ipynb): notebook that explores the district information, number of doors, and fleet size of each 5311 organization. 
3. [gtfs_monetary_analysis](./gtfs_monetary_analysis.ipynb): notebook that explores the gtfs status, reporter type, organizations in district, and fleet size & age of the organizations. Also charts the total sum of 5311 funds received by each agency from 2011-2021.
4. [district_analyses](./district_analyses/): folder with notebooks looking at a subset of the data in various districts
5. [count_doors_fleetage.ipynb](./5311/count_doors_fleetage.ipynb)

#### Visualizations
1. [chart_outputs](/5311/chart_outputs): Charts for all outputs
