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
* Cleaning is easier while df is long, but by the end of cleaning, maybe it needs to be reshaped to be wide. Depends on visualization! Think through this, and double check with 1:1 or m:1 merges explicitly in `pd.merge` to understand how one dataset should be related to the next one.
* Reshaping can be done through aggregating, counting, dropping duplicates, `pd.melt` or `pd.pivot`, etc.
* If the df needs to be wide, each row should represent one agency with all the vehicle info attached

## Grants

* Keep relevant columns
* Is the df long or wide?
* If the df needs to be wide, each row should represent one agency with all the grant info attached (similar process as above)

## Best Practices

The best practice for collaborative and reproducible work is to use functions, functions...and data catalogs! 

* **Functions**: These functions can move from the notebook to a script once they're ready. Notebooks should import these functions so that the cleaned data is the same starting point for everyone.
* **Data Catalog**: Raw and processed datasets should be included. Intermediate datasets can be checked in locally (not in GitHub), and shouldn't crowd the catalog. [Data catalog docs](https://docs.calitp.org/data-infra/analytics_tools/data_catalogs.html)
* **GitHub Workflow**: Remember to `git stash` and `git pull origin this-branch` and `git stash pop` to get each other's commits before you push your commits. Work in different notebooks to avoid merge conflicts. Conflicts in Python scripts and Markdown are very easy to resolve. [Refer often to helpful hints](https://docs.calitp.org/data-infra/analytics_tools/saving_code.html)
* **Sub-folders**: Create sub-folders for data and visualizations if those are being saved locally. This will make it easy to exclude checking in datasets to GitHub, and easy to add all visualizations produced. 

## Functions

Use functions related to the following:

1. Clean / subset each of the data sources. 
<br>Are there cases for aggregation / visualization where a long (not wide) df is needed? Add additional function to allow for this.

1. Catalog the crosswalks and use them to merge.
<br>Merging datasets together using the 2 crosswalks already available.
    * Merge NTD with GTFS (left join) --> `m1_df`
    * Merge grants with m1_df (left join) --> `m2_df`
    * Within `pd.merge`, explicitly define your `how=` to choose left or inner join, and set `indicator=True`. Print outputs to show the join results.
    * Deal with missing info: left joins mean grant recipients that do not have NTD IDs, and subsequently, no ITP IDs are kept. This is ok, but have a way to flag and remove these from visualizations related to GTFS status.

1. Aggregate.
<br>Decide on units of analysis...organizations may have received from multiple 5311 funding sources. Does it matter? Or is it just the 5311 category that counts?
    * For each GTFS status, how many 5311 agencies?
    * For each funding source, how many 5311 agencies? 5311 agencies-GTFS status?

1.  Visualize.
<br>Before visualizing, do some observations need to be dropped? 
<br>Ex: 5311 recipients that are not linked to NTD and ITP ID (no GTFS status, but this is expected). Understand what the "universe" or "pool" of observations going into the visualization is.
    * Decide on format for deliverable. [Decide on deliverable format and incorporate it in the code](https://docs.calitp.org/data-infra/analytics_publishing/how_to_publish.html)
    * Whatever type, use parameters within the function to get the labeling for axes, titles, legend labels, chart PNG file-naming, etc 
    * Save final visualizations and check them all into GitHub. `git add viz/` would add everything in the `viz` sub-folder.