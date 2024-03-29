# Bus Procurement Analysis

## Datasets
* FTA Bus and Low- and No-Emission Grant Awards
* TIRCP Project List
* DGS Usage Reports (via Rebel)
* (upcoming )Washington and/or Georgia Contract list (via Rebel)

## GH issue
Research Request - Bus Procurement Costs & Awards #897
  
## Research Question
Identify federal awards to fund bus purchases and how much agencies pay for them.

## Methodology
- Examine each dataset to:
    * understand table structure. Which dataset need to be aggregated or disaggregated?
    * determine if any columns need cleaning/validation
    * determine which columns are unnecessary
    * understand project types (bus procurement, facility procurement, both)
<br></br>
- Identify projects that include bus purchases. Of which, note the following
    * Total cost of project
    * count of buses
    * propulsion type of buses (zero/non-zero emission, BEB, FCEB, CNG etc)
    * bus type (standard, cutaway, articulated etc)
<br> </br>
- Combine datasets together, aggregate up by different categories, calculate a "cost_per_bus" (cpb) column.
    * merge datasets
    * cpb calculated by dividing total cost by total bus count.
<br></br>
- Aggregate cpb by:
    * transit agency
    * propulsion type
    * bus size type
<br></br> 
 - Visualize aggregations on charts
     * Frequency Distribution Charts
     * Bar charts
     * top level totals
<br></br> 
 - Calculate summary stats on cpb
     * calculate mean, standard deviation
     * calculate z-score. remove outliers
     * plot distribution
<br></br>
 - Other
     * review external reports/resources regarding bus procurement

## Script Explanation

Executing `make all_bus_scripts` will run the following scripts
<br></br>
- **fta_data_cleaner.py:**
    * Reads in and cleans FTA data
    * outputs 2 files: 
        * cleaned, all projects: `fta_all_projects_clean.parquet`
        * cleaned, bus only projects:`fta_bus_cost_clean.parquet`
<br></br>        
- **tircp_data_cleaner.py**
    * Reads in and cleans tircp data
    * outputs 2 files: 
        * cleaned, all projects: `clean_tircp_project.parquet`
        * cleaned, bus only projects:`clean_tircp_project_bus_only.parquet`
<br></br>
- **dgs_data_cleaner.py**
    * Reads in and cleans DGS data
    * outputs 2 files: 
        * cleaned, bus only projects: `dgs_agg_clean.parquet`
        * cleaned, bus only projects with options:`dgs_agg_w_options_clean.parquet`
<br></br>
- **cost_per_bus_cleaner.py**
    * Reads in and merges all the bus only datasets
    * updates columns names 
<br></br>
- **cost_per_bus_utils.py**
    * stores variables for summary section (total projects, total buses, etc)
    * stores chart functions to be used in notebook
    * stores the summary and conclusion text.