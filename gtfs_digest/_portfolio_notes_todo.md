## Notes for my reference for testing the [portfolio](https://test-gtfs-exploratory--cal-itp-data-analyses.netlify.app/readme)
### 4/11/2024 Run 
To-Do
* YML
    * How do I get the values from the `organization_name` column to show up on the left hand panel but I want to filter on  `name`?
* Modify the `makefile`
* Modify the dataframes to read from the data catalog instead of my own f-strings.
* Add in the different sections.
* Move all `read_parquet` to reference stuff grabbed from `catalog_utils.get_catalog("gtfs_analytics_data")`.
* `Average Total Service Hours` chart 
    * Rename as `Total` because it is not the average.
    * Move the chart out of route-direction monthly overview
    * Concat the dataframes together so users can filter for months in both 2023 and 2024.
* Route-Direction Monthly Overview Section
    * Great tables: dates are a little different (minor). Move the great table to the bottom OR add another markdown line right before all the charts to instruct users to filter by route or else the charts won't make any sense.
    * Charts: 
        * Test if making a simple chart then adding more encoding and domain ranges will work? This is for later.
        * Make it a bit smaller.
        * Add a function that resizes the charts instead of having the properties configured for each one? 
        * Move dropdown to the top
        * I got the `this exceeds the max rows allowed for Altair` error for AC Transit...I turned off the warning but I am worried this will backfire in the future. Figure out a way to summarize all the info without making files unbearably large.
        * Maybe update the color palette to something more meaningful? Like red to green to for `vehicle positions per minute` so the fewer the vps, the redder? 
        * If there are no values for a certain chart,display a message instead of a blank chart.
        * Maybe all_day can be a pink/red color while the peak and offpeak can remain blue? 
        * Add interactive() behind each chart.
        * Minor but make sure all the dates are slanted at 45 degrees for readability.
        * What will happen once we add more dates?? 
        * How to display only one route before you filter down to the route you choose. For now all the routes show and this is seriously confusing.
    * Frequency of Route chart: change from heatmap because this is confusing to people. 
    * Speed chart: how to make `all_day` speed more obvious.
    * For Vehicle Positions per Minute, % of RealTime Trips, % of Scheduled Trips, and Spatial Accuracy charts, figure out if there's a way to color the background so the bottom  red, middle is yellow, and the top is green to signify the different "zones/grades".
    * Route Statistics for Route for Dir 0/1: Quite often nothing shows up...Figure out why?? Stuff shows up on the charts. 
        * Example route: 652 Skyline High - Elmhurst Bay Area 511 AC Transit Schedule
