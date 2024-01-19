# Bus Procurement Analysis

## Datasets
    * FTA Bus and Low- and No-Emission Grant Awards
    * TIRCP Project List
    * DGS Usage Reports (via Rebel)
    * Washington and Georgia Contract list (via Rebel)

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
    - Combine datasets together, aggregate up by transit agency, calculate a "cost_per_bus" column.
    - Anylyze cost per bus for the different bus categories