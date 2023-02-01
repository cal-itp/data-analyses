# Cell Coverage
<i>"GTFS Real Time publication is associated with cellular coverage. How many vehicles are operating in areas with poor cellular coverage?"</i>

## Workflow
### Federal Communications Commission (FCC) Data Prep
The AT&T, Verizon, and T-Mobile files is downloaded [here](https://us-fcc.app.box.com/s/f220avmxeun345o6gzr7rwcnp1wslocf). Only `data` was downloaded, not `voice.` The main landing page of the data is [here](https://fcc.maps.arcgis.com/apps/webappviewer/index.html?id=6c1b2e73d9d749cdb7bc88a0d1bdd25b). 
1. The shapefiles for AT&T and Verizon are for the entire USA. Use the function `A1_provider_prep.create_california_coverage` to clip these maps to only include California. [The end results in GCS are titled](calitp-analytics-data/data-analyses/cellular_coverage): 
    * `att_ca_only.parquet`
    * `verizon_ca_only.parquet` 
2. The shapefiles for T-Mobile are split by region. California's data is combined with other neighboring states across several files. As such, T-Mobile was concatted together. 
    * `T_Mobile_349525.zip`
    * `T_Mobile_349527.zip`
    * `T_Mobile_349687.zip`
    * `tmobile_california.parquet` is the final result. However, it includes parts of neighboring states. 
3. The initial approach using `overlay` on `unique_routes` and the `provider maps` clipped to California yielded inaccurate results. For example, routes running in highly urbanized areas such as the middle of Los Angeles County and San Francisco's Golden Gate Bridge showed up as having poor cellular coverage. Additionally, the original provider maps were large, spanning hundreds of rows.  Thus, the provider maps were further manipulated in several steps, looping over by Caltrans Districts. 
    * Step 1 `A1.sjoin_gdf`: `sjoin` the provider map against the Caltrans districts shapefile. 
    * Step 2 `A1.clip_sjoin_gdf`: the results from the `sjoin` are scraggly and includes other portions of neighboring districts. Use `clip` to clean up the edges. 
    * Step 3 `A1.dissolve_clipped_gdf`: provider maps are still large. `dissolve` the results above. 
    * Step 4 `A1.find_difference_gdf`: the maps originally show areas <b>with</b> coverage. However, as explained above, this led to wrong results. The other approach is to use `difference` to depict areas <b>without</b> coverage. 
    * Step 5 `A1.stack_all_maps`: after running step 4, the provider maps are scattered among 12 files. `Concat` them to create a map for all of California. 

### Other Data Sources Prep
To answer how many buses run in a low coverage area, this requires bringing other data sets.
1. Find unique routes with `A2.load_unique_routes_df`. Dig deeper to find whether a route runs in one or multiple districts using `A2.find_multi_district_routes`.
2. Load `A2.trip_df` for the number of trips an operator runs across all routes on September 14, 2022. 
3. Use `A2.ntd_vehicles` to find the number of buses each agency owns. 
4. Use `A2.load_gtfs` to add on the GTFS statuses by agency. 

### Analysis 
1. `Overlay` all the unique routes for each provider with `A3_analysis.stack_all_routes`. Now, there are 3 different dataframes. Use `A3.merge_all_providers` to return a single dataframe with routes that cross an area without data cellular among all three providers. 
2. To add NTD, GTFS, and Trips information and find an estimate of total buses that are running in poor cellular coverage routes, `merge` the dataframe above using `A3.final_merge`. 