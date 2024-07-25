## Notes for my reference for testing the [portfolio](https://test-gtfs-exploratory--cal-itp-data-analyses.netlify.app/readme)
* python portfolio/portfolio.py clean gtfs_digest_testing && python portfolio/portfolio.py build test_gtfs_exploratory  --deploy 
* cd data-analyses/rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env && cd ../gtfs_digest
* RT Dates https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/rt_dates.py
* `import sys sys.path.append("../gtfs_funnel") import crosswalk_gtfs_dataset_key_to_organization`
### Running to-do list
* Portfolio:
    * Figure out Makefile situation.

### 7/16/2024
* Figure out how to update the mermaid diagram/supporting docs. 
* If you can sketch out, on paper or somewhere, what grains went into the digest, and what exactly the datasets are supporting the digest notebook, your mermaid diagram is simply the mermaid code of bringing your sketch to life. So i'd start with the conceptual motivation of "if i stumbled upon the digest and looked at the charts, what do i need to understand the various grains for the datasets? did 1 dataset support it? multiple?"
Another way to think about this is, if you used words to say, here's chart 1, it uses a dataset that is X grain. Here's chart 2, it uses Y grain. Here's chart 3, it also uses Y grain. Here's chart 4, it uses Z grain. How does someone connect the dots from a report they see to the (many) slices of data you used?
    * Chart 1: Total Routes by Typology
        * Schedule 
        * `gtfs_funnel/operator_scheduled_stats.py`
        * Operator-Month
        * NACTO
        * Why is there no arrow connecting NACTO  to `operator_profiles` in the mermaid [diagram](https://mermaid.live/edit#pako:eNqFVG1vmzAQ_iuWpyidlEQhIU1KpUlNm2zqlnZao30YVMiBA7yCjWzTllb57zNQCqjNyofg-J7n7rkX7hl73Ads4eFw6DBFVQwW-rVF9xLdeBH4WQxoA0pQTzqsxPR6zw5DiDKqLFQeEeqrCBLoW6i_IxL6g_btbyIo2cUg-69wbUoFTYjIz3nMRcH7tJqtZ-uzmtogtvCoGtR4PH4LWXLhgzgEiimDQzYJHmd-V8d6PV8tWxgFQtEOJAiCfmXeFy_9s-_1HOawIOYPXkSEQttlBZDZLhQkjZB8KWbtFiEvJlJeQID8AAU0jq2XEryDkJ6gqapRpcAiXI07s3V_UnlrWZb2NRx-QY2teFZHNk9BEMWFW-vwXamIkqM0v_2seVWE04a3tGVEUmg7XbXM57ZUPD1kvSitrqLJQQdrO1SBdO8gd7kICaNPRFHOXE9wKR9IfNfhtRJalVc_7DoRVOSOkmpEX0mIhKGAkCh4px6bhix4psD1qQCvCN_1c1oXGZhfH18benV2vr2u-EjlqR6OkIJs1aBQIVOdFYnRX04ZUhztsiAAAb7mEV--I-2rXVhcCWECTB1u6bcju5LehO62suB0KZd2FfSBqqiluClZF96RXnJizkKQCpWDgfRAvWT_JtL3SlsnUt6ksjlF6H-VvYeIero3KZe0aIpsz_qVfZ-6mSwWysHiXGvMh2PRpRROfha0jwaiJrTk11d4gBMQCaG-XqflpnNwuQEdbOmjDwHJYuVgvS40lGSK3-TMw5YSGQywDhxG2ApILPW_LPW1yAtKdEmSGgI-1Z_wptrX5doe4JQwbD3jR2wNF5PR9GQ6OTGn88X0ZGKYA5xja3o8MszZwjw2TWM2XpjGfoCfONdOjdHE0OtwbMynU40252bp7k9pLHXs_wGgMdVb)? 
    * Chart 2: Longest and Shortest Route
        * Scheduled data.
        * Operator-route for the latest date.
    * Map 3: Same as above.
    * Charts 4-6:
        * Grain: Scheduled trips -> operator level -> summing up all  service hours for a week in either April or October -> differentiate by Sat/Sun/Weekday and by hour.
    * Rest of the charts
        * Combining scheduled with realtime data
        * Grain: operator-route-single service day 
### 7/15/2024
* <s>`Merge_operator_data.py`
    * Added `digest.tables/operator_profiles` and `scheduled_service_hours` here.</s>
    * Or should `scheduled_service_hours` go somewhere in `gtfs_funnel`? I am not sure since we discussed there isn't a usage for this dataset outside of the portfolio.

Slack messages
* so i guess i can ask you where the datasets published are living in gtfs_analytics_data.yml? 95% of datasets that have been processed / saved out should make its way there + the Mermaid diagrams + Makefile should reflect that. i notice a lot more datasets being published for digest, so what do the 2 operator profiles mean? when do i choose one over the other? is one outdated?
    * Deleted out the `operator_profile_portfolio_view.`
    * Now I only have `digest.tables/operator_profiles` which incorporates the NTD data. 
* the makefile's purpose would be, if i'm in this folder, what scripts do i have to run and in what order? to make it obvious to include the ones you added, you want to add it in, otherwise it'll get mistaken for random script, not relevant to the product
* the overarching principle for mermaid diagrams / how&what scripts are used would be the grain of the dataset.
* essentially, the scripts and the mermaid diagram and gtfs_analytics_data.yml catalog all should reflect each other, and you might find that setting up the diagrams mean you edit how scripts are constructed.
as is, catalog shows groupings of certain datasets, and digest is one of those. all the datasets under that header would be exactly what is saved to the GCS/gtfs_digest/ and published to the public GCS. if anything is in the catalog and not being updated in GCS, you can drop all those references in the code and delete those files in the bucket.
    * Double check I'm publishing `operator_profies` with NTD stuff and `scheduled_service_hours` to  gcs_paths.RT_SCHED_GCS/digest (which is [here](https://console.cloud.google.com/storage/browser/calitp-analytics-data/data-analyses/rt_vs_schedule/digest?pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22))&project=cal-itp-data-infra)) and the public GCS [here](https://console.cloud.google.com/storage/browser/calitp-publish-data-analysis/gtfs_digest?pageState=(%22StorageObjectListTable%22:(%22f%22:%22%255B%255D%22)))
* ^ following that principle, operator_profiles and operator_profile_portfolio_view basically differ only on yes NTD or no NTD columns. the grain is operator. a row is operator. columns are operator characteristics (regardless of NTD or GTFS) and should be in the same table. pick a name and delete the other reference in both GCS buckets and the catalog and only use that name in the scripts. you should feel free to delete or rename the datasets and scripts I put, and aim to reconcile everything / think about it as one digest pipeline, rather than appending side datasets to what i've created (causes more confusion)
expanding on digest related datasets, there should only be 1 final dataset per grain, and that 1 final dataset can be derived from multiple sources. if there's a new grain you're adding ([define your grain for stop_times scheduled service hours]), that is the part that you want to add onto the mermaid diagrams / situate it some place
    * My grain for `"digest/total_scheduled_service_hours"` is operator-one week all scheduled service hours across all routes. 
* my hint for you related to setting up mermaid diagram / catalog for success is: identify the number of datasets for digest and what grains they are. see if you can match what grains are produced in merge_data.py and what grains are produced in merge_operator_data.py , and see how your stuff fits into that. then consider where your new grain lives, and whether it can sit by itself in a merge_* script. (right now, _gtfs_digest_specific_datasets.py convolutes several grains, and if you can tease it apart, the diagram will be easier to develop, and the catalog will naturally reflect that)
    1. digest_tables.operator_routes_map
        * Grain: operator-route geography
    2. counties
        * Grain: get county geographies 
    3. digest_tables.operator_profiles
        * Grain: operator and its characteristics (also draws data from NTD)
    4. digest_tables.scheduled_service_hours
        * Grain: operator and all the scheduled service hours for one week across all routes.
    5. digest_tables.route_schedule_vp
        * Grain: operator-route for each service_date downloaded for each month.

### 7/9/2024
* Understand how `publish_public_data.py` works.
### 7/8/2024
* Rerun a subset of operator...Didn't work on 7/3.
* <s>Finish updating `README` with new template created as part of summer refactor 2024. </s>
    * <s>Update `methodology.md`</s>
    * <b>To-do: ask somebody to look it over.</b>
* Take a look at cardinal direction work again: I noticed one route has four different directions and this shouldn't be happening.
    * Went back to the original code using City of Fairfield's Cordelia Library Route as my test.
    * It's no mistake: the route does change direction in May 2023. 
    * Unsure how to proceed: 
        * It is confusing to the viewers to have a direction switch, however it's accurate. 
        * I could also replace the direction across time with the two most common combinations, but this wouldn't be accurate. 

### 7/5/2024
* <s>Fix `operator_profiles` so all the dates and operators are included.</s>
* <s>Upload datasets to the public GCS.
    * Add this to the make file.
    * Make sure datasets have a CSV and parquet version.</s>
* Rerun a subset of operator...Didn't work on 7/3.
* Update `README` with new template created as part of summer refactor 2024. 

### 7/3/2024 Goals
* <s>Switch color palette to colorblind friendly one.</s>
* <s>Switch NTD info to crosswalk. Read in crosswalk file when I load in `gtfs_digest/_section2_utils/operator_profiles`.</s>
    * Question: Do I need to upload this specific operator_profile view with all the NTD stuff to the public GCS?
* <s>Move Monthly Services data to its own file in `gtfs_digest`</s>
* <b>Rerun a subset of operators for the GTFS Digest test site. AH: this isn't working, posted on data_office_hours</b>

### 7/2/2024 Notes 
* <s>Cardinal Direction
    * There's no stipulation that nan values in `direction_id` need to be filled.
    * Some routes only run one way, that's why `direction_id` is only populated once. 
    * By filling in `direction_id` in some places and not others, this causes everything to be disjointed. When merging the `sched_vs_rt` and `cardinal_direction` dataframes together, this will impact the merges.
    * Action: Delete all lines that fill na for `direction_id`. Rerun `gtfs_funnel/Makefile/cardinal_dir_temp` first and then `gtfs_digest/merge_data`</s>
    * Left to do:
        * Upload the new dataset to the public GCS folder.
        * Once all the NTD/Monthly Service stuff is done: rerun the portfolio.
        * Create supporting diagrams/text.
* NTD
    * Actions:
        * Use `dim_annual_ntd_agency_service` instead of the table I currently use.
            * Amanda: `dim_annual_ntd_agency_service` doesn't contain the columns such as the UZA/reporter type/etc that I need for the portfolio.Clarified w/ Vivek: this table is ok to use. NTD is just behind.
        * Add this to `crosswalk_gtfs_dataset_key_to_organization` instead of `operator_profiles` because the `crosswalk` file already contains NTD ID. 
        * Delete out `ntd_annual_database_agency` stuff and just add it directly into the crosswalk file.
        * Read in crosswalk file when I load in `gtfs_digest/_section2_utils/operator_profiles`
* Monthly Services
    * This is a new area in the mermaid diagram in `gtfs_funnel`.
    * Because there isn't a broad use for it and it's portfolio specific, just leave it in the `gtfs_digest` funnel but upload it to GCS.
    
* How to upload stuff to the public GCS folder.
    * `gtfs_digest/publish_public_data`.
    * Remove `monthly_scheduled_service` and replace it with my dataframe from above.
### 7/1/2024 Questions 
[Issue 1159](https://github.com/cal-itp/data-analyses/issues/1159)
* Cardinal Direction [Issue 1135](https://github.com/cal-itp/data-analyses/issues/1135)
    * I reran everything for all the dates.
    * How do I publish this to public GCS?
    * All the charts run properly after my change (tested only on a few operators)
    * Filling in nan with direction_id. 
        * I was merging the work from `gtfs_digest/merge_data` incorrectly with the stuff from `schedule_stats_by_route_direction`
        * However, when you say all rows should merge, that's not true since since some rows are found in schedule data only and some are RT data only? 
        * Also in `gtfs_funnel/schedule_stats_by_route_direction.py` a lot of rows simply are `nan` for `direction_id` at the `trips` grain. Shouldn't it be filled with 0? 
    * How do I add supporting diagrams/docs? 
* I added a new script with `ntd` stuff called `ntd_annual_database_agency` to be incorporated into `gtfs_funnel/operator_scheduled_stats`.
    * I plan to load this line around lines 158-175.
    * Why do you like to read in parquets instead of just running a funciton and returning a dataframe? 
    * Do I rerun the `preprocess_schedule_only` part in the `MAKEFILE` again?
    * How do I publish this to our public GCS?
* Monthly Service Hours by weekday/weekend
    * How do I add a new dataset to GCS? 
    * Where do I run it? Within `gtfs_funnel`? 


### 6/26/2024 Refactor Summer goals
* Working on cardinal direction stuff. 
* Then incorporate NTD data into the actual pipeline of `operator_profiles`.
* Publish `total_service_hours` by weekday, Saturday, and Sunday onto GCS. This is its own dataset.
* Update colors to be color blind friendly?
    * [This article](https://medium.com/swlh/how-i-designed-a-colorblind-friendly-palette-8a91a49f1220) has some nice palettes [1](https://miro.medium.com/v2/resize:fit:640/format:webp/1*hWdA4lnxgt8Iclg87jFwig.png) and [2](https://miro.medium.com/v2/resize:fit:640/format:webp/1*FTn-we6PFb-BoyWkRQ_sYQ.png).
    * [This one too on page 2](https://www.nceas.ucsb.edu/sites/default/files/2022-06/Colorblind%20Safe%20Color%20Schemes.pdf)
### 6/6/2024
* Troubleshooting the # of unique routes an operator runs. BART seems really high, which is the impetus for this investigation. 
    * Work is in `18_operator_profiles.ipynb`.
    * BART splits the route into essentially 2 because it uses one Route ID when the train goes North and another Route ID when the train goes South.
    * Also took a look at San Francisco Muni: we identify 68 unique routes and their website lists about 70. 
    * IDEA: add verbiage that says "approximately":
    <i>Route Typologies The following data presents an overview of GTFS statistics using data from the most recent date of April 2024. City and County of San Francisco ran <b>approximately</b> 68 unique routes.</i>
* Continue working on adding Cardinal Direction to the pipeline.
* Second draft of common definitions added into `methodology.md`.
### 6/5/2024
* Was working on adding Cardinal Dir to the Pipeline.

### 6/4/2024
* Finish up writing the common definitions that will be added in the `methodology.md`.
* <s>Rerun all of the districts for schedule+GTFS and schedule only operators for TTTF #4.</s>

### 6/3/2024
* <s>Add back operators even if they don't have VP positions such as BART.</s>
    * Updates made in `gtfs_digest/_operators_prep`
    * Deployed the portfolio with only  D4 operators as a test [here](https://gtfs-digest-testing--cal-itp-data-analyses.netlify.app/district_04-oakland/0__03_report__district_04-oakland__organization_name_stanford-university).
    * After deploying the D4 subset, I rearranged the organization names in the Table of Content to be alphabetical. 
    * Bugs in the portfolio
        <s> * The `try except` clause i put behind <i>Detailed Route Overview</i> doesn't show anything even for operators that have GTFS data. 
            * For operators without RT data,  the area under <i>Detailed Route Overview</i> displays nothing when it's supposed to display the message I set.</s>
            * Fixed: have to wrap `display` around the function that makes all the charts. 
        * BART runs 12 lines according to the `operator_profile` but the map only displays one route.
            * Same thing with Emeryville Transportation Management, runs 2 routes but only 1 route is on the map.
        * City of Rio Vista's Route Typology Pie Chart isn't showing up
            * Was missing the new category '# Coverage Route Types'
        * <s>The 'longest vs shortest' route charts aren't appearing even in the actual portfolio.</s>
            * Fixed: I was using a color_palette reference that was renamed awhile back.
### 5/30/2024
* <s>Rerun all the operators.</s>
* Ideas on next steps (in order of priority)
    * Update the methodology.
    * <s>Add back operators even if they don't have VP positions such as BART.</s>
    * Figure out how to add in the cardinal directions back to frequency/text tables as some of the routes switch directions. 
    * Double check NTD stuff. 
    * <s>Take out the subtitle for the following graphs since it looks repetitive and cluttered. 
        * `Frequency of Trips in Minutes` 
        * `Daily Scheduled Service Hours` charts following Weekday</s>
### 5/29/2024
* [Netlify test link I created 5/17.](https://gtfs-digest-testing--cal-itp-data-analyses.netlify.app/district_07-los-angeles/0__03_report__district_07-los-angeles__organization_name_los-angeles-county-metropolitan-transportation-authority)

### 5/17/2024
* State of the GTFS Digest Portfolio 
    * All the tweaks in [Issue](https://github.com/cal-itp/data-analyses/issues/1101) except cardinal directions & mapping the routes based on the `route_colors` provided by operators have been addressed. 
    * There is a rough estimate of the cardinal directions a route is headed.
        * Cons:
            * The function to grab the cardinal direction takes a long time. It will take about 2 hours for all the operators to run.
            * The function looks across the route for all time periods, rather than looking at the direction per date. This is less accurate.
            * Not all routes record data for both directions. There will be empty graphs for these routes. 
        * Current tackling:
            * `Route_id` and the associated names for routes change over time. I am still working on refining the function that attaches the most current `route_id` to the same routes across the entire time span of the dataset. 
            * Ex: Main Street Route had the ID of 123 in April 2023 but ABC in April 2024. I have to go back and update the ID for April 2023 to be ABC. 
            * The function is very slow. Grabbing all of the stops and finding the direction they points means the dataset is huge, especially because we need all the dates possible. I am working on using `dask deployed` to speed things up. 
    * Service Hour Charts
        * Finished. There are now 3 charts for daily weekday, Saturday, and Sunday total service hours across all the routes.
        * Right now, we only have data for the entire weeks of April and October 2023. April 2024 to come.
        * Added a new column `weekday_service_hours` to `_section1_utils.total_service_hours` that divides the sum of `service_hours` by 5 for an accurate count. Previously, this was done incorrectly. 

### 5/16/2024
To-Do
* <s>Noticed a bug after running all of the operators...Figure out why the cardinal direction shows southbound and northbound even for the east/west directions.</s>
* In the future, speed up the cardinal direction work? Or it'll be added to the pipeline? Right now it takes a very long time. 
* Table of Contents on the portfolio site: the operators are not alphabetical. 
    * D7: LA Metro is before City of Santa Monica/City of X/City of Y
* <s>Fix service_hours chart-> divide weekday by 5</s>
* <s>Add in dropdown menu to show only the first route</s>
    * Found a solution through Stackoverflow. 
### 5/15/2024
* <s>Add explanations for why we chose these cutoffs in the `color_palette.yml`</s>
* Continue cardinal directions work.
* Rerun portfolio. 

### 5/14/2024
* Add back cardinal directions to `direction_id`. 
    * I am done grabbing all the dates available, stacking them together, and aggregating for only one particular operator, let's call the dataframe `cardinal_dir_df`. 
    * This is a big dataset, so I want to read in only the rows that are absolutely necessary. 
    * When I merge this `cardinal_dir_df` with `sched_vp_df`, none of the routes match. I need to use [this script](https://github.com/cal-itp/data-analyses/blob/b1e5d4f870400251240eeba4a6515a0848e5d6f8/gtfs_funnel/clean_route_naming.py#L4) to clean up the names on the `cardinal_dir_df`
* Displaying only the first route -> 
    * Even after upgrading `altair` to the latest version of `5.3` I am still unable to accomplish this goal using the sample `cars` dataset. 
### 5/13/2024
* <s>For Service Hour Charts by weekday, Saturday, and Sunday.
    * Added this to the target notebook for the GitHub site</s>
* Didn't receive any answers to my question on  Stackoverflow yet about displaying only the first route. 
* Add back cardinal directions to `direction_id`. 
    * Met with Tiffany yesterday to understand how to aggregate everything properly. 
* Section 2
         * Vehicle Positions per Minute, % of RealTime Trips, % of Scheduled Trips, and Spatial Accuracy charts, figure out if there's a way to color the background so the bottom  red, middle is yellow, and the top is green to signify the different "zones/grades".
        * <b> Note 4/16 </b> Having a hard time getting this to work. Facet doesn't allow for layering and it seems like the background impacts the colors of the main chart, making everything hard to view.
        * <b> Note 5/13 </b>: I was able to get the background to be shaded perfectly for the `Service Hour Charts` but `faceting` charts causes this shading to become wonky. Therefore, this goal is now retired. 
        <s>* Create an Altair chart with just a title/subtitle to divide out metrics about the quality of the rider's experience on a route vs the quality of the data collected about the route.</s>
### 5/10/2024
* How to display only the first value before filtering for charts. Right now, the charts show stuff for all months/routes and this is confusing.
    * Still couldn't get this to work even with a very simple `cars` dataset from the Altair example. 
    * Submitted a question onto StackOverflow for help. 
* <s>Service Hour chart.</s>
    * Show chart to Eric and Katrina & double check the aggregation with Tiffany before making it into functions.


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

