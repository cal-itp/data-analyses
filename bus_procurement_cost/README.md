# Bus Procurement Analysis

## Datasets
* FTA Bus and Low- and No-Emission Grant Awards
* TIRCP Project List
* DGS Usage Reports (via Rebel)
* (upcoming )Washington and/or Georgia Contract list (via Rebel)

## GH issue
* [Research Request - Bus Procurement Costs & Awards #897](https://github.com/cal-itp/data-analyses/issues/897)
* [Research Task - Refactor: Bus Procurement Cost #1142](https://github.com/cal-itp/data-analyses/issues/1142)
  
## Research Question
Analyze bus procurement projects to see how much transit agencies pay for them.

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

- **bus_cost_utils.py**
    * contains all the shared functions and variable used throughout the cleaner scripts
<br></br>   

Executing `make all_bus_scripts` will run the following scripts
<br></br>

- **_01_fta_data_cleaner.py:**
    * Reads in and cleans FTA data
    * outputs 2 files: 
        * cleaned, all projects: `clean_fta_all_projects.parquet`
        * cleaned, bus only projects:`clean_fta_bus_only.parquet`
<br></br>        

- **_02_tircp_data_cleaner.py**
    * Reads in and cleans tircp data
    * outputs 2 files: 
        * cleaned, all projects: `clean_tircp_all_project.parquet`
        * cleaned, bus only projects:`clean_tircp_bus_only.parquet`
<br></br>

- **_03_dgs_data_cleaner.py**
    * Reads in and cleans DGS data
    * outputs 2 files: 
        * cleaned, bus only projects: `clean_dgs_all_projects.parquet`
        * cleaned, bus only projects with options:`clean_dgs_bus_only_w_options.parquet`
<br></br>

- **_04_cost_per_bus_cleaner.py**
    * Reads in and merges all the bus only datasets
    * updates columns names
    * calculates `cost_per_bus`, z-score and idetifies outliers.
    * outputs 2 files:
        * cleaned projects: `cleaned_cpb_analysis_data_merge.parquet`
        * cleaned, removed outliers: `cleaned_no_outliers_cpb_analysis_data_merge.parquet`
<br></br>

- **nbconvert --to notebook**
    * runs all cells in the `cost_per_bus_analysis.ipynb`
    * overwrites the nb in place
<br></br>

- **nbconvert --to html**
    * converts the nb to HTML
    * hides the code cells and prompts
<br></br>

- **weasyprint ...html ...pdf
    * convers the HTML files to PDF, perserving the same style fonts, tables and charts.

Output data files are saved to GCS at: `calitp-analytics-data/data-analyses/bus_procurement_cost`

Final deliverable: `cost_per_bus_analysis.pdf`
