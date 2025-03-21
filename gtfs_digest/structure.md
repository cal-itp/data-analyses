# Merge Data

## Single Day
* schedule + wrangling
* speeds + wrangling
* rt vs schedule + wrangling
* crosswalk + wrangling (caltrans district)
* combine all 3 -> suppress private datasets
* single day export (stage1 these are feeds)
* handle dupes for organization 
* aggregate or dedupe for single day to reflect organization (stage2 these are organizations)
* rename for single day viz

## Quarterly
* start from: single day export (stage1 these are feeds)
* aggregate
   * the columns should be not readable, space-y columns yet
   * so that the metrics in single day and quarterly reflect what's explicitly defined in the import
* handle dupes for organization
* aggregate for quarterly
* rename for quarterly viz

## Visualization Wrangling
* this should already be handled in renaming, but might be more than that


## Portfolio
* after `merge_data`, that dataset should be used to flag complete operators vs incomplete (ok vs raise errors)
* create yaml to deal with duplicates
* use this to clean up the single day and quarterly 
