# GTFS Schedule Analytics Strategy

Our warehouse for GTFS schedule data is more mature than GTFS real-time. At present, it's going through a rewrite (GTFS schedule warehouse v2). 

**Summary**:
1. Current analyst use patterns for accessing GTFS schedule tables in the warehouse
    * [gtfs_utils](https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/gtfs_utils.py)
    * Addresses need for query templates, standardization in putting tables together, and cached parquets cuts down on time for pre-assembling data for analysts
    * Discussion: What is the best caching strategy then for single day, granular tables under warehouse v2?
    * Discussion: moving from single day to averages framework, what steps can be taken to prepare for future `mart` table?
1. Identify opportunities, under warehouse v2, where `mart` tables would be created
    * `gtfs_utils`
    * `rt_utils` - `fct_daily_vehicle_positions`
    * monthly averages
    
    
## Current Analyst Use Patterns

The `gtfs_utils` within `shared_utils` acts as a wrapper to standardize how analysts query the warehouse. It was written to lighten the cognitive load (always searching in past work), to gain a comprehensive view of how analysts were accessing and assembling their "raw" data, and to make it significantly easier to propagate changes downstream (switching to warehouse v2, upgrading `calitp-py` and `siuba`).

All that said, `gtfs_utils` may or may not live in its existing form in warehouse v2. It serves several purposes, but if a `mart` table view can create canonical "pre-assembled" tables for analysts, `gtfs_utils` can live on in a pared down form.  


### Templated Queries

Analysts were typically looking in past work to find a template of a constructed query to use in future work. It got more difficult to remember where the queries were saved in various notebooks and scripts within `data-analyses`. 

### Standardization 

Canonical datasets are important, but how those datasets are put together are also important. Different decisions early on might lead to different rows retained, leading to different counts, resulting in different aggregate metrics reported...eroding stakeholder trust. 

Do some of the pre-assembly for the analysts to streamline some of this decision-making, and also ensure we are comparing apples-to-apples across time, even if different people work on the analysis.

Across analysts, basically, 85% of the query used the components, just that analysts were filtering for different days, different operators, different modes, etc. Differences were present in how tables were joined, what merge keys were used, whether intermediate queries were saved as `LazyTbl`s or not, etc. 

* `stops` - always `views.gtfs_schedule_dim_stops` + `views.gtfs_schedule_fact_daily_feed_stops` + creating point geometry from lat/lon
* `trips` - always `views.gtfs_schedule_dim_trips` + `views.gtfs_schedule_fact_daily_trips` + patch in a Metrolink fix since `shape_id`s are known to be missing, but can be constructed based on `direction_id` and `route_id`.
    * Where's Metrolink? [issue](https://github.com/cal-itp/data-analyses/issues/289), [PR](https://github.com/cal-itp/data-analyses/pull/290)

### Caching Results

We typically use single-day queries in our analyses. This saves on our Big Query costs. [Query caching](https://cal-itp.slack.com/archives/C01FNDG1ZPA/p1660577985253819?thread_ts=1660059469.106639&cid=C01FNDG1ZPA) was discussed, but until we actually wrote duplicate queries, it wouldn't have actually be used in practice. Post `gtfs_utils`, maybe we got more use out of this.

After using `gtfs_utils`, I aligned the analytics workflows to get the most mileage out of the cached parquets. External facing work (HQTA, traffic ops, speedmaps, quarterly performance metrics, competitive bus corridors) used cached parquets. HQTA / traffic ops runs at monthly frequencies for all operators. 

Examples:
* HQTA and Traffic Ops are both open data portal datasets. It makes sense to publish the GTFS schedule on the same day. 
* Speedmaps uses 5 raw tables, 4 of which are generated through HQTA (`shapes`, `trips`, `stops`, `stop_times`) and 1 is vehicle positions. Having to create all 5 tables means the data generation stage takes nearly 20 hrs, but pulling the 4 cached tables and just creating the vehicle positions table cuts that down to maybe 10 hrs. 
* Analyses are not usually picky about a date, and analysts pick a random weekday. But, they have a date each month (through HQTA) to pick, and can have all the cached parquets ready.

Up until now, we needed  time to understand what tables are typically used, what granularity was the starting point, and identify any common patterns in the process. 

I think caching is still needed as long as we are under the single day framework. A `trips` table and a `shapes` table off by a month leads a significant portion of joins that are not `both` statewide. That's problematic, because it's not actually an error, it's just a matter of aligning on dates. Most analysts are not filtering / subsetting down too much, they want the granular tables to do spatial joins and other bespoke data wrangling. If they're always using granular tables (`trips`, `stops`, etc), then there's not much to do in an aggregated `mart` view, beyond the pre-assembly needed to create `trips`, `stops`. 

**Discussion: What is the best caching strategy then for single day, granular tables under warehouse v2?** Parquets in GCS or there's a way to optimize this? At the very least, we will have monthly parquets saved for HQTA. (If there's more to discuss, Hunter wants this group + Mjumbe for more options)

### Single Day to Averages

Freed from the single day framework, if we move toward averages (average weekday service, average weekend / holiday service), there's opportunity to move these aggregations as a `mart` table. Aggregated / average service hours can become the rule, and single day service hours can be the exception (used in special analytics cases).

**Current idea:** average service hours will be calculated by month-time_of-day-day_of_week-operator-shape_id.

* `shape_id` - needed to join back to line geometry, can easily find `route_id`
* `month` - can easily aggregate to business quarters and year, but more understandable than week 1, week 17, which may span different months across years.
* `day_of_week` - weekday, weekend / holiday 
* `time_of_day` - owl, early AM, AM peak, midday, pm peak, evening. add in all day, peak, non-peak. 

**Discussion**: moving from single day to averages framework, what steps can be taken to prepare for future `mart` table?
* I'll be at least working out a prototype of what I'd like to see, maybe exploring `ibis` to help with moving Python to SQL. 
* `dask-bigquery`
    * parse the timestamp to get `departure_hour` and aggregate to `time_of_day` bins. 
    * parse date to also attach weekday or weekend or holiday?
    * ideas for how to do the testing that doesn't require hitting BQ constantly?
    * will probably do it on all our cached parquets as fake data to test
* A `mart` table would probably run monthly to add a new monthly average?

### References
* [Weekday Aggregations Epic](https://github.com/cal-itp/data-analyses/issues/512)
