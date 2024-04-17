## Notes for my reference for testing the [portfolio](https://test-gtfs-exploratory--cal-itp-data-analyses.netlify.app/readme)

### 4/17/2024 Notes
* How is the stuff in `GTFS_DATA_DICT.digest_tables.operator_sched_rt` different than the findings in `digest_tables.route_schedule_vp`?
* Is Section #2 supposed to be a less granular version of Section #3? 
    * Section #3 splits the route by direction and time period, but section #2 will just give you the route without these distinctions? 
* For `operator_profiles` some organizations don't information that is most recently available. What do I do? 
### 4/11/2024 Run 
To-Do
* YML
    * How do I get the values from the `organization_name` column to show up on the left hand panel but I want to filter on  `name`?
* Modify the `makefile`
* Add in the different sections.
* <s>Move all `read_parquet` to reference stuff grabbed from `catalog_utils.get_catalog("gtfs_analytics_data")`.</s>
* <s>`Average Total Service Hours` chart </s>
    * Rename as `Total` because it is not the average.
    * Move the chart out of route-direction monthly overview
    * Concat the dataframes together so users can filter for months in both 2023 and 2024.
* Route-Direction Monthly Overview Section
    * <s>Great tables: dates are a little different (minor). Move the great table to the bottom OR add another markdown line right before all the charts to instruct users to filter by route or else the charts won't make any sense.</s>
    * Charts: 
        * Test if making a simple chart then adding more encoding and domain ranges will work? This is for later.
        * Make it a bit smaller.
        * Add a function that resizes the charts instead of having the properties configured for each one? 
        * <s>Move dropdown to the top</s>
        * I got the `this exceeds the max rows allowed for Altair` error for AC Transit...I turned off the warning but I am worried this will backfire in the future. Figure out a way to summarize all the info without making files unbearably large.
        * <s>Maybe update the color palette to something more meaningful? Like red to green to for `vehicle positions per minute` so the fewer the vps, the redder?</s>
        <s>* If there are no values for a certain chart,display a message instead of a blank chart.</s>
            * <b> Note 4/17</b>: Created a chart that returns "no chart available, not enough data."
        * Maybe all_day can be a pink/red color while the peak and offpeak can remain blue? 
        * <s>Add interactive() behind each chart.</s>
        * <s>Minor but make sure all the dates are slanted at 45 degrees for readability.</s>
        * What will happen once we add more dates?? 
        * How to display only one route before you filter down to the route you choose. For now all the routes show and this is seriously confusing.
            * <b> Note 4/17 </b> [The former method is no longer working.](https://github.com/cal-itp/data-analyses/blob/main/rt_segment_speeds/_threshold_utils.py)
    * <s>Frequency of Route chart: change from heatmap because this is confusing to people.</s>
        * <b> Note 4/16 </b>: switched to a faceted chart that repeats. 
    * <s>Speed chart: how to make `all_day` speed more obvious.</s>
            * <b> Note 4/16 </b>: switching over to a new color palette now makes `all_day` a red color so this is more obvious. 
    * For Vehicle Positions per Minute, % of RealTime Trips, % of Scheduled Trips, and Spatial Accuracy charts, figure out if there's a way to color the background so the bottom  red, middle is yellow, and the top is green to signify the different "zones/grades".
        * <b> Note 4/16 </b> Having a hard time getting this to work. Facet doesn't allow for layering and it seems like the background impacts the colors of the main chart, making everything hard to view.
    * <s>Route Statistics for Route for Dir 0/1: Quite often nothing shows up...Figure out why?? Stuff shows up on the charts. 
        * Example route: 652 Skyline High - Elmhurst Bay Area 511 AC Transit Schedule</s>
            * <b> Note 4/17 </b>: Fixed this. The `_section2_utils.route_stats()` function needs outer merges. I also edited the `create_text_table()` function to drop duplicate rows. Without dropping duplicate rows, the text table overlays all the rows and you can't read the text. 
