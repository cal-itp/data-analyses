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
- Aggregate cpb by:
    * transit agency
    * propulsion type
    * bus size type
 <br></br> 
 - Visualize aggregations on charts
 - Calculate summary stats on cpb
     * calculate mean, standard deviation
     * calculate z-score. remove outliers
     * plot distribution
 <br></br>
 - Other
     * review external reports/resources regarding bus procurement
