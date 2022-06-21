# README

## E-76

### Data Creation/Cleaning

1. [Cleaning Script](./dla_utils/dla_utils/clean_data.py): Pull E_-76 data from Caltrans page, clean and produce parquet file 
1. [cleaning_notebooks](./e76_obligated_funds/cleaning_notebooks): folder with cleaning notebooks work 
1. [project_categories](./e76_obligated_funds/project_categories) folder with identifying project categories (included in cleaning script)
1. [update_parquet.ipynb](./e76_obligated_funds/data_exploration_analyses/update_parquet.ipynb): notebook to pull and read E-76 Data


### Crosswalks

1. [agency_ntd_crosswalk.ipynb](./crosswalks/agency_ntd_crosswalk.ipynb): notebook creating crosswalk between NTD organizations and Airtable organizations
1. [dla_ntd_itp_crosswalk.ipynb](.//crosswalks/dla_ntd_itp_crosswalk.ipynb): notebook connecting crosswalk 1 (NTD and Airtable) with DLA Locodes


### Data Assembly

1. [function_work.ipynb](./e76_obligated_funds/data_exploration_analyses/function_work.ipynb): notebook with work for developing functions for _dla_utils


### Analyses

1. [project_categories_analysis.ipynb](./e76_obligated_funds/project_categories/project_categories_analysis.ipynb): Project Categories analysis
1. [district_analyses](./e76_obligated_funds/district_analyses): notebooks for district level analyses (D3, D4, D7, D10, D11)
1. [data_inconsistencies.ipynb](./e76_obligated_funds/data_exploration_analyses/data_inconsistencies.ipynb): identify inconsistencies within e-76 data
1. [humboldt_county_analysis.ipynb](./e76_obligated_funds/data_exploration_analyses/humboldt_county_analysis.ipynb): analyses for Humboldt County
1. [new-metrics.ipynb](./e76_obligated_funds/data_exploration_analyses/new-metrics.ipynb): explore new metrics
1. [percentiles.ipynb](./e76_obligated_funds/data_exploration_analyses/percentiles.ipynb): 
1. [prefix_profiles_3.ipynb](./e76_obligated_funds/data_exploration_analyses/prefix_profiles_3.ipynb): identify repeat customers
1. [preliminary_analysis_2.ipynb](./e76_obligated_funds/data_exploration_analyses/preliminary_analysis_2.ipynb): preliminary analysis 
1. [project_groupings.ipynb](./e76_obligated_funds/data_exploration_analyses/project_groupings.ipynb): identify groupings in data



### Reports
1. [_dla_utils_reports.py](./e76_obligated_funds/reports/_dla_utils_reports.py): utility functions used in reports
1. [dla_district_report.ipynb](./e76_obligated_funds/reports/dla_district_report.ipynb): parameterized notebook for reports
1. [gen_report.py](./e76_obligated_funds/reports/gen_report.py): script for dla report
1. [reports_work/](./e76_obligated_funds/reports/reports_work): folder for working notebooks 



