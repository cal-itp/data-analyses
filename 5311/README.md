# README

## 5311 Analysis

Below are our scripts and notebooks we used for cleaning the data and creating the metrics & flags to address the research ask about agencies recorded in Caltrans' Black Cat grant management database that received 5311 & 5311(f) funds. 

### Data Creation
1. [data_prep](./data_prep.py): functions for cleaning our 3 data sources and merging them together. 

### Visualization / Outputs
1. [utils](./_utils.py): functions for charts and aggregating the merged data frames. 
1. [door_analysis](./door_analysis.ipynb): notebook that explores the district information, number of doors, and fleet size of each 5311 organization. 
1. [gtfs_monetary_analysis](./gtfs_monetary_analysis.ipynb): notebook that explores the gtfs status, reporter type, organizations in district, and fleet size & age of the organizations. Also charts the total sum of 5311 funds received by each agency from 2011-2021.
