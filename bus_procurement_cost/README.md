# Bus Procurement Analysis

## Datasets
* FTA Bus and Low- and No-Emission Grant Awards
* TIRCP Project List
* (upcoming) DGS Usage Reports (via Rebel)
* (upcoming )Washington and/or Georgia Contract list (via Rebel)

## GH issue
Research Request - Bus Procurement Costs & Awards #897
  
## Research Question
Identify federal awards to fund bus purchases and how much agencies pay for them.

## Methodology
- Examine each dataset to determine if any columns need cleaning/validation
- Identify projects that include bus purchases. Of which, note the following"
    * Total cost of project
    * count of buses
    * propulsion type of buses (zero/non-zero emission, BEB, FCEB, CNG etc)
    * bus type (standard, cutaway, articulated etc)
<br> </br>
- Combine datasets together, aggregate up by transit agency, calculate a "cost_per_bus" (cpb) column.
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