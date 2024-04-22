## Notes for my reference for testing the [portfolio](https://test-gtfs-exploratory--cal-itp-data-analyses.netlify.app/readme)

### 4/22/2024
* I updated the yaml because last week, I discovered there are multiple organization name-name combos and duplicates. This work is in `07_crosswalk.ipynb`. 
    * The new yaml includes only one row for one organization.
* Last week, I tried to run all the operators. However, the notebook would error out at AC Transit & SF Muni. 
    * To test, I commented out a lot of the charts in the `section2.filtered_route(sched_vp_df)` part. 
    * All the operators' github pages were successfully generated however this seems to hint that too many Altair charts causes things to go a bit wild. 
    * Cut down on charts? Not sure. 
* Comments on the new GitHub Pages
    * Operator Overview (section 1)
        * The Pie Chart with Total Routes by Typology doesn't match the number of unique routes. 
            * UC Davis runs 19 unique routes but there are 20 routes in the pie chart.
            * Double check this w/ Tiffany.
        * <s>Double check the longest and shortest route. Some routes are VERY long.
            * City of Eureka in D1 has a route that is almost 160 miles.
            * Yes this is correct</s>
        * For the "public transit provided in the following counties" style the dataframe instead of printing it. Maybe concat a bullet point to the county so it looks like a list?
        * For the `Total Service Hours` month, I mapped 1 to be Monday, 2 to be Tuesday, etc. Double check and make sure it's correct.
         <s>* Need to add back stop information. </s>
   * Detailed Route-Direction Overview (section 3)
       * <s>What's that box that prints "None"?? Why is it generated for every page??
           * Prints "None" because I wrapped the `great_table` with `display()`</s>
       * <s>`create_data_unavailable_chart()` doesn't work  when adding a dropdown menu to allow for route selection...only works with one route example  in `01_section3.ipynb` 
           * Test: Gold Route in City of Eureka 
           * Test: 6 wellness express  "Palo Verde Valley Transit Agency"/'Desert Roadrunner GMV Schedule' in d8
           * Tried returning a `print` statement but it didn't work. </s>
* Use the `readable.yml` 
    * To create the charts.
    * Rename dataframes.
    
### 4/17/2024 Notes
    * Example: just have one chart for speeds for both directions? 
* For `operator_profiles` some organizations don't have any information for March. 
    * in `04_gtfs_exploratory.ipynb` Blue Lake Rancheria's most recent information is from last September. 
    * Some organizations don't have information at all such as Peninsula Corridor Joint Powers. 
        * However this organization appears to have speed/trip data. 
    * We want only one organization to map with one name
        * Keep only the most recent name-row for the organization_name.
* AC Transit & SF Muni always throws an error:
    * "nbclient.exceptions.CellTimeoutError: A cell timed out while it was being executed, after 4 seconds.
    The message was: Timeout waiting for IOPub output.
    Here is a preview of the cell contents:
    -------------------
    section2.filtered_route(sched_vp_df)"
    
    * This didn't happen last time. 
    * Test what is going wrong one element at a time. 

* Readable.yml
    * Operators -> what does it mean when you have `organization_name:Organization`. 
        * Organization is a string. 
        * Other column names have "" to avoid any special symbols like () or + being read in weird. 

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
