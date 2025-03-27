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

### YAML for 3 categories
* return 1 df from `_operators_prep.operators_schd_vp_rt` and switch from `some_func()` to `some_func(url)`, or every time you're importing the script, it's running the function.
   * work on reading in parquets using the `columns = []` and dropping duplicates to get the df as close as possible to the grain you want
   
   ```
   Ex: read in 3 columns since I already know it's a route-direction-time_period grain,
   I will dedupe it immediately and pass it through to filter to just the date I want
   
   most_recent_feeds = pd.read_parquet(
    FILE,
    columns = [
        "schedule_gtfs_dataset_key",
        "name",
        "service_date"
    ]
    ).drop_duplicates().pipe(
        publish_utils.filter_to_recent_date
    )
   ```
   * by the end of the one df returned (not 3, as it is), the df should be sorted by cateogry, name, organization_name so the 1:m, m:m, m:1 relationships are clear.
   * use these relationships to set up the next yaml
   * you can use the same df with various functions to help you get at these varied relationships
* Use these hints to help return 1 df in the function. You can use the 1 df to see whether name appears multiple times, organization_name appears multiple times, etc. 
   * use `publish_utils.most_recent_date` to already filter against the most recent service-date/operator combination
      * caltrans_district would be handled within `merge_data` in the crosswalk concatenation
      * there is an extra deduping now, because caltrans_district values have not been standardized, but you can remove the deduping steps if you use `portfolio_utils` in an earlier script.
   * is it possible for an operator to have multiple `sched_rt_category` values? are these dealt with using `gtfs_schedule_wrangling.mode_by_group`?
   * set a list of excluded operators (`name`) as a variable. if there are more beyond `City of Alameda`, we want to have an easy way to add to this.

### YAML for deduping feeds to organizations
* Right now, I see these kinds of pairing. 
```
City of Menlo Park:
- Commute.org
Commute.org:
- City of Menlo Park
```

* Desired output is to understand what is included vs excluded on the portfolio, where distinct information is displayed without overcounting. Maybe it needs to be clear that  a Duarte case would provide duplicate information, so excluding them means they are already included in the Foothill Schedule completely. There can probably be a caption to list out which feeds this includes in the report itself, so make sure this is yaml is set up to help you get what you need!
```
1:m (1 schedule_gtfs_dataset_key-many organization)...primary one is the included
Foothill Transit:
- include: Foothill Schedule
- exclude: Duarte Schedule

m:1 (many schedule_gtfs_dataset_key-1 organization)
LA Metro:
- include: LA Metro Bus
- include: LA Metro Rail

City and County of San Francisco:
- include: Bay Area 511 Muni Schedule
- include: Bay Area 511 Golden Gate Park Shuttle Schedule
- include: Golden Gate Park Shuttle Schedule

m:m (this would be a really weird case...let's double check there are none of these, but if there are, we need to know)
```

### Quarterly Rollup
* `shared_utils.time_helpers` has an `add_quarter` function to ensure that the way we are constructing that is consistent for viz, where a format of 2024_Q1, 2024_Q2, etc would sort correctly on the x-axis.
* I see some similarities in the getting the metrics you want as in https://github.com/cal-itp/data-analyses/blob/main/rt_segment_speeds/segment_speed_utils/metrics.py, specifically the `derive_rt_vs_schedule_metrics`. Is there a way to bridge these 2 instances so that the function itself is adapted to handle the generic df without over-handling some other stuff? You can explore to see how the reference is used elsewhere and see if you can streamline both use cases so you can use the same function in 2 places.
    ```
    df = df.assign(
        vp_per_minute = df.total_vp / df.rt_service_minutes,
        pct_in_shape = df.vp_in_shape / df.total_vp,
        pct_rt_journey_atleast1_vp = df.minutes_atleast1_vp / df.rt_service_minutes,
        pct_rt_journey_atleast2_vp = df.minutes_atleast2_vp / df.rt_service_minutes,
        pct_sched_journey_atleast1_vp = (df.minutes_atleast1_vp / 
                                         df.scheduled_service_minutes),
        pct_sched_journey_atleast2_vp = (df.minutes_atleast2_vp / 
                                         df.scheduled_service_minutes),
    )
    ```