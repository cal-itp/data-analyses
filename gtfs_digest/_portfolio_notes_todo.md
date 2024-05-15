## Notes for my reference for testing the [portfolio](https://test-gtfs-exploratory--cal-itp-data-analyses.netlify.app/readme)
* cd ../ && pip install -r portfolio/requirements.txt
* python portfolio/portfolio.py clean test_gtfs_exploratory && python portfolio/portfolio.py build test_gtfs_exploratory  --deploy 
* python portfolio/portfolio.py build gtfs_digest  --deploy 
* cd data-analyses/rt_segment_speeds && pip install altair_transform && pip install -r requirements.txt && cd ../_shared_utils && make setup_env
* RT Dates https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/rt_dates.py
### Running to-do list
* Portfolio:
    * Figure out Makefile situation.
* Charts 
    * Section 2
         * Vehicle Positions per Minute, % of RealTime Trips, % of Scheduled Trips, and Spatial Accuracy charts, figure out if there's a way to color the background so the bottom  red, middle is yellow, and the top is green to signify the different "zones/grades".
        * <b> Note 4/16 </b> Having a hard time getting this to work. Facet doesn't allow for layering and it seems like the background impacts the colors of the main chart, making everything hard to view.
        <s>* Create an Altair chart with just a title/subtitle to divide out metrics about the quality of the rider's experience on a route vs the quality of the data collected about the route.</s>
    * <s> Check YAML script
        * Make sure there aren't any operators that have stuff in `sched_vp` but not in `operator_profile`
        * 4/24: Done, using `07_crosswalk.ipynb`</s>

### 5/10/2024
* How to display only the first value before filtering for charts. Right now, the charts show stuff for all months/routes and this is confusing.
    * Still couldn't get this to work even with a very simple `cars` dataset from the Altair example. 
    * Submitted a question onto StackOverflow for help. 
* <s>Service Hour chart.</s>
    * Show chart to Eric and Katrina & double check the aggregation with Tiffany before making it into functions.
* Add back cardinal directions to `direction_id`. 

### 5/9/2024 To-Do
* <s>Frequency Chart: make the colors a set range, so all routes will always be dark green if a bus going that route and direction comes by every 10 minutes, dark red if every 60 minutes.</s>
* <s>Spatial Accuracy: Confirm with Tiffany about buffer and add the buffer into the subtitle.</s>
    * The buffer is 35m.

### 5/8/2024 To-Do 
* <s>% RT Journey Chart with VP: delete</s>
* <s>Total Monthly Scheduled Service Chart: delete</s>
* <s>Speed Chart: use different colors. Red, green, and yellow suggest bad, good, and ok.</s>

* <s>Route Typology Chart: remove the subtitle. Add in a blurb from the methodology.md about how routes are categorized. Add a hyperlink to NACTO Route guide.</s>
### 5/6/2024
<s>* Eric: the maps from .explore() look pretty overwhelming when an operator has many routes. 
    * Can we modify the reports site maps to be used here too? 
    * Check Total Service Hours charts.</s>
    * 5/8: Not worth the effort and people probably won't even interact with the map to look at a singular/a few routes.
* Streamline code, some of the code for my charts is redundant.
* NTD
    * Ask people for feedback on the NTD copy.
    * Try to connect the agencies by ntd_id instead of name? 
    * Tiffany:
        * Explain NTD_ID situation again? 
    * Change NTD copy so it doesn't run off the page
    * Alternatively, set the max width for each cell so nothing runs off?
* Update Makefile stuff.
    * https://github.com/cal-itp/data-analyses/blob/main/ntd/Makefile#L4
    * The goal is to write one line of code in the terminal and have the entire portfolio deploy each month.
* When should this be updated every month? The beginning?
* If I want to start on the bunching metric, which folder do I work out of? 

### 5/2/2024
* <s>Update README with some of the methodology FAQ.
* Officially deploy portfolio
    * https://github.com/cal-itp/data-analyses/blob/main/Makefile#L69</s>
### 5/1/2024
* <s>Add in NTD data.</s>
* Update Readme.MD
    * Cherry pick commit.
    * Link to Methodology
* <b>Consolidate the total service hours charts with Eric's findings.</b>
* <s>Address Tiffany's comments
    * can you change the thickness of the line chart, looking a bit thick, no?
    * also in the text in the beginning, it's now reading like Apr, 2024, but should read Apr 2024
    * semi-annoying that the legend label runs off the page, perhaps the chart width needs to shrink by a little. the alternative seems more cumbersome, which is to inject a list into the legend</s>
        * Amanda: Changed the name of legend values that were running off. 
    * <s>maybe work more headers in? there's just 2 main headers, but kind of a lot of sections, like "map of routes" (this one esp gets buried) probably can be its own, "route typology"</s>
    * spacing after the map is really big - why?
        * Amanda: Think this always happens with `.explore()` but I need to talk to Eric about using something else to display the maps because operators with a lot of routes gets very overwhelming. 
    * <s>in the caption, pick a consistent spacing: Early AM:time vs Early AM: time</s>
    * <s>monthly aggregation label Full Date actually refers to  Month</s>
    *<s> spatial accuracy - static route shape -> scheduled shape (path)
        * Amanda: Confirm with Tiffany what this means? </s>
    * <s>GTFS always spelled like that, not Gtfs (in your text box for route dropdown)</s>
    * <s><b>name of the portfolio should be GTFS Digest, not GTFS Exploratory</b></s>
    * <s>colors for the average scheduled minutes for route...yellow/red may not be the best choice here, it's not a good or bad thing to be direction 0/1. the way yellow/red are used elsewhere does seem to indicate good vs bad.</s>
    * <s>can you write the counties served as a sentence? LA Metro: Los Angeles, Ventura, Orange (alphabetized).</s>
        
### 4/30/2024
* <s>Update readme.
    * 5/1 Check with Tiffany that this is what she's looking for.</s>
* <s>Rebase off of main and update the URL for service_hours</s>
* <s>Add in divider charts to Section 2 that sort the charts thematically.</s>
### 4/29/2024
#### To-Do
*  <s>Routes Pie Chart
    * Confirm again that a route can fall into various categories.
    * D3 UCD only runs 19 unique routes but the piechart has like 30+ routes.
    * Same with D4 Central Contra Costa Transit Authority
    * Update with new cols: 'it's like downtown_local, local, coverage, rapid, express, rail'</s>
* <s>Section 1 Total Service Hours
    * Dates should be sorted in descending order, so the most recent date is at the top
    * Make a new daily chart, continue automating finding the number of each type of day in a year. 
    * Refine total hours across the month chart.</s> 
* Section 2
    * <s>Need to update `spatial_accuracy` subtitle.</s>
    * Continue troubleshooting the javascript message that has now appeared.
        * Figured out the error was due to `def create_data_unavailable_chart():

        chart = alt.LayerChart()
    
        return chart`
    
### 4/26/2024
#### To-do Today
* <s>Remove the mapping for the `Total Service Hours` with the weekday column that is in the dataset. </s>
     * Create a new chart  chart names to Eric's suggestions.
* <s>Reorder charts thematically. Add a little chart in between that explains the divide.</s>
* <s>Double check that all the column names when injecting into charts/display are correct. I caught a mistake: I plugged in RT trips when I wanted to display Scheduled trips in section 3.</s>

#### Notes 
* Tried making a map that is controlled with an `ipywidget` dropdown menu so users can view one map at a time. This only works in the notebook and not when uploaded onto GH pages.
    * Go back to using .explore()
    * I tried using altair yesterday but altair can only plot geomtype of points, not lines. 
* `Total Service Hours` doesn't match reports...still trying to debug.
* Section2
    * After updating the graphs for section 2, I now get this message `Javascript Error: Unrecognized signal name: "concat_1_width"
This usually means there's a typo in your chart specification. See the javascript console for the full traceback.`. 
    * I noticed I have to run the same cell twice in order to get the charts to generate `for _ in range(2):` but this is not ideal because the error is not resolved and is still displayed. 
    * Some operators aren't even running like Emeryville:
        `Javascript Error: Unrecognized signal name: "concat_1_width"
This usually means there's a typo in your chart specification. See the javascript console for the full traceback.
Javascript Error: Unrecognized signal name: "concat_3_width"
This usually means there's a typo in your chart specification. See the javascript console for the full traceback.`
    * All the charts generate fine on their own, error is coming from concating them? 
    
* Goals
    * <s>Finish copy on readable.yml</s>
    *<s> Convert all of the charts to read from the readable.yml. </s>
        <s>* For charts in section 1, add subtitles and title.</s>
    * <s>Change section3utils references back to section2.</s>
   
  
   * <s>Double check section3.add_categories() is accurate. 
       * Decided to take it out since I'm not using this in any of the charts.</s>
### 4/24/2024 
#### Questions for Tiffany
* Operator Overview (section 1)
    * The Pie Chart with Total Routes by Typology doesn't match the number of unique routes. 
            * UC Davis runs 19 unique routes but there are 20 routes in the pie chart.
            * Double check this w/ Tiffany.
    * For the `Total Service Hours` month, I mapped 1 to be Monday, 2 to be Tuesday, etc. Double check and make sure it's correct.
    * Confirm about total daily trips
        * This is now only displaying "all_day" columns but some still seem very high?
* Status on route classification?
* Status on road classification?
* Bunching metric game plan? 
* Incorporate agency classification into this portfolio? 

#### Comments from Tiffany
##### Done
* formatting for numbers, include the comma (like 1,000 instead of 1000)...like {,} (https://pbpython.com/styling-pandas.html)
    * Done, rounded up the individual numbers since I'm not sure if you can grab values off of a styled dataframe.
* First sentence: This section presents an overview of statistics for Los Angeles County Metropolitan Transportation Authority, using data from the most * recent date of 3/2024 ...replace the 3/2024 to display March 2024 as a more readable format
    * Done, now displays month in string
* add a map displaying all the routes, i think a gdf.explore() would work -- although let me know if this is what's breaking the map
* vp per minute chart probably shouldn't run up to 5. it really only hits 3 as a max...so either 3.5 or 4 for y-axis
    * 4/24: added it back in.
* Total Scheduled Service Hours ....not blue for time-of-day. go with categorical scale? also set the colorscale manually because no color should be repeated
    * I think I addressed this? Confirm with Tiffany.
* Fill out your readable.yml for better displaying column names and captions -- you're already working on this    

##### Having Issues
* yes, leave out the chart unavailable. i've tried this: when you're concatenating altair charts but you're missing one, you'd basically do like chart2= alt.LayerChart() so that when you do
 chart_list = [chart1, chart2, chart3]
 chart = alt.vconcat(*chart_list)
you're not missing any chart, but it's just an empty nothing, but there's no title, no df being read in</s>
    * Still doesn't work once I concat the charts, I think the route dropdown is impacting this? 
* have you tried this: https://stackoverflow.com/questions/70937066/make-dropdown-selection-responsive-for-y-axis-altair-python
    * Having trouble getting it to work with our charts but it works in the Stack Overflow example.
    `dropdown = alt.binding_select(
    options=['Miles_per_Gallon', 'Displacement', 'Weight_in_lbs', 'Acceleration'],
    name='X-axis column '
)
xcol_param = alt.param(
    value='Miles_per_Gallon',
    bind=dropdown
)`
#### Goals for today 
* <s>Update frequency</s>
* Incorporate comments from Tiffany.

### 4/23/2024
* <s>Check the # of daily trips again in section 3. Even after dropping duplicates, it still seems crazy high.</s>
    * Solution: filter on `time_period == all_day`.
* <s>"Operator Overview":
    * Add a sentence about the Total Scheduled Service Hours and what it means.</s>
* Reran all pages for all operators: this time it worked.
    * I think it's because I dropped duplicates  when reading in `sched_vp`.
    * Observations of wonky things.
    * Organization - City of Eureka, Route - AMTRS Red Route
        * Frequency of Trips Per Hour Chart: some of the charts look funky.
        * Section 3
        * <s>Still says "Scroll down to filter for a specific route" update it to something else since the dropdown menu is now on the right.</s>
        * `Frequency of Trips per Hour` is kidn of confusing, when it's a low number like 0.04. What does that mean? Should it be X every X hours instead? 
        * `Average Speed` charts' interactive legend doesn't work. I want people to be able to view one time period at a time because sometimes the lines overlap too closely to see anything.
        * `Vehicle Postions per Minute`: set anything above 2 as green. Right now, the color scale maps to lowest and highest value. Some routes that record 3 vps per minute now have bars that red when only 2 vps were recorded, even though 2 is good.
            * Ex: Mendocino Transity Authority 20 Willits/Ukiah 
            * Same thing with `Spatial Accuracy` Chart: anything above 95 should be green...
    * <s>Bring make the `explore` map maybe once all the other sections are done so we can visualize the reach of the transit agency?</s>
* Notes from 1:1
    * Who is the target audience?
        * Local agencies 
        * TTTF
        * Advocates
    * What is the transit background/knowledge of the audience?
        * General public?
        * Data enthusiasts? 
        * Transit enthusiasts?
    * The metrics presented cover both data quality metrics (spatial accuracy, density of GPS pings per minute) and the quality of the rider's experience (early/late/ontime trips and speed). How to incorporate these two themes seamlessly? 
        * Themes are interrelated.
        * Some audience members are interested more in one theme than the other. 
        * Delineate Section 3 into these two themes? Section 2 is a snapshot...focus on quality of the experience?
    * How to present this information?
        * Too much information and too many charts is overwhelming, what's the perfect amount? 
        * Perhaps to the general person, vehicle positions is not easily grasped. Explain what it is, why it's important to transit improvement, why it's relevant to the transit rider's experience.
        * Work on captions/chart names/metric names/metric categories to make them more easily understandable.
            * Use the readme.md as a "How to Understand" Guide? Glossary?
    * Bunching: mostly relevant to agencies with lots of riders/frequent routes. 
        * Do we run this metric for all agencies or only some? This metric wouldn't be meaningful to some agencies/routes...
    
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
         * Need to add back stop information. </s>
   * Detailed Route-Direction Overview (section 3)
       * <s>What's that box that prints "None"?? Why is it generated for every page??
           * Prints "None" because I wrapped the `great_table` with `display()`</s>
       * <s>`create_data_unavailable_chart()` doesn't work  when adding a dropdown menu to allow for route selection...only works with one route example  in `01_section3.ipynb` 
           * Test: Gold Route in City of Eureka 
           * Test: 6 wellness express  "Palo Verde Valley Transit Agency"/'Desert Roadrunner GMV Schedule' in d8
           * Tried returning a `print` statement but it didn't work. </s>
        
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

