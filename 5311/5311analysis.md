# 5311 Analysis

## Data Sources
1. Black Cat 5311 grant recipients
1. NTD agencies, vehicles
1. GTFS status (Airtable)

## Crosswalk
1. Flag which NTD agencies are 5311, subset to CA
    * `Reporter Type` == `Rural Reporter`?
1. Merge NTD + GTFS to get ITP ID and GTFS status
    * left join, how many NTD IDs do not have ITP IDs? Tracked in Airtable already
1. Merge NTD and grants
    * left join, how many observations have no info?
    * use `fuzzymatcher` to join, figure out threshold of matching to keep
    * for first `fuzzy_left_join`, NTD and grant entities should only appear once

Produce 1 additional crosswalk using script: grant recipient name - NTD ID 

## NTD 
* Keep relevant vehicle columns
* Is the df long or wide?
* Cleaning is easier while df is long, but by the end of cleaning, better for it to be wide
* Reshaping can be done through aggregating, counting, dropping duplicates, etc
* Reshape df to be wide, each row should represent an agency with all the vehicle info attached

## Grants
* Keep relevant columns
* Is the df long or wide?
* Reshape df to be wide, each row should represent an agency with grant info attached (similar process as described above)

## Functions

The best practice for collaborative and reproducible work is to use functions, functions...and data catalogs! These functions can move from the notebook to a script once they're ready. Notebooks should import these functions so that the cleaned data is the same starting point for everyone.

Also, remember to `git pull origin this-branch` to get each other's commits before you push your commits. Work in different notebooks to avoid merge conflicts. Conflicts in Python scripts and Markdown are very easy to resolve.

Use functions related to the following:

* Clean / subset each of the data sources
* Catalog the crosswalks
* Merging datasets together using the 2 crosswalks already available
    * Merge NTD with GTFS (left join) --> `m1_df`
    * Merge grants with m1_df (left join) --> `m2_df`
    * Deal with missing info: left joins mean grant recipients that do not have NTD IDs, and subsequently, no ITP IDs are kept. This is ok, but have a way to flag and remove these from visualizations related to GTFS status.
* Aggregation 
    * Decide on units of analysis...organizations can have receive 5311 funding sources
    * For each GTFS status, how many 5311 agencies?
    * For each funding source, how many 5311 agencies? 5311 agencies-status?
* Visualization
    * Before visualizing, do some observations need to be dropped? Ex: 5311 recipients that are not linked to NTD and ITP ID (no GTFS status, but this is expected). Understand what the "universe" or "pool" of observations going into the visualization is.
    * Whatever type, use parameters within the function to get the labeling for axes, titles, legend labels, chart PNG file-naming, etc 
    