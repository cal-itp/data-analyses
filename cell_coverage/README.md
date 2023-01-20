# Cell Coverage
<i>"GTFS Real Time publication is associated with cellular coverage. How many vehicles are operating in areas with poor cellular coverage?"</i>

## Workflow
### Federal Communications Commission (FCC) Data Prep
The AT&T, Verizon, and T-Mobile files were downloaded [here](https://us-fcc.app.box.com/s/f220avmxeun345o6gzr7rwcnp1wslocf). Only `data` was downloaded, not `voice.` The main landing page of the data is [here](https://fcc.maps.arcgis.com/apps/webappviewer/index.html?id=6c1b2e73d9d749cdb7bc88a0d1bdd25b). 
1. The shapefiles for AT&T and Verizon were for the entire USA. Use the function `A1_provider_prep.create_california_coverage` to clip these maps California. The end results in GCS: 
    * `att_ca_only.parquet`
    * `verizon_ca_only.parquet` 
2. The shapefiles for T-Mobile were split by region. California's data is combined with other neighboring states across several files. As such, T-Mobile was concatted together. 
    * `T_Mobile_349525.zip`
    * `T_Mobile_349527.zip`
    * `T_Mobile_349687.zip`
    * `tmobile_california.parquet` is the final result, which includes parts of other states surrounding California. 
3. Using `overlay` between `unique_routes` and the provider maps led to inaccurate results. For example, routes running in highly urbanized areas such as the middle of Los Angeles County and San Francisco's Golden Gate Bridge showed up as having low data cellular coverage. Additionally, the original provider maps were large. As such, the provider maps were further manipulated in several steps, district by district. 
    * Step 1 `A1_provider_prep.sjoin_gdf`: `sjoin` the provider map against the Caltrans districts shapefile. 
    * Step 2 `A1_provider_prep.clip_sjoin_gdf`: the results from the `sjoin` are scraggly and not an accurate depiction of the actual district shape. Use `clip` to clean up the shape. 
    * Step 3 `A1_provider_prep.dissolve_clipped_gdf`: provider maps are still large and unwieldy. `dissolve` the results above. 
    * Step 4 `A1_provider_prep.find_difference_gdf`: the maps originally show areas <b>with</b> coverage. However, this led to inaccurate results, as explained above. The other approach is to use `difference` to depict areas <b>without</b> coverage. 
    * Step 5 `A1_provider_prep.stack_all_maps`: after running step 4, the provider maps are scattered among 12 files. `Concat` them to create a map for all of California. 

### Other Data Sources Prep
The approach to answer the question above requires using `traffic_ops/export/ca_transit_routes`, `rt_delay/compiled_cached_views/trips_2022-09-14` and NTD Vehicle data. 
1. First, we need to find where routes are located. Find all unique routes with `A2_other.load_unique_routes_df`. Dig deeper to find whether a route runs in one or multiple districts using `A2_other.find_multi_district_routes`.
2. Load `A2_other.trip_df` to find the number of trips an operator runs across all routes. 
3. Use `A2_other.ntd_vehicles` to find the number of buses an agency owns. 

### Analysis 
After preparing all the data, it's time to answer the question. 
1. `Overlay` all the unique routes for each provider with `A3_analysis.stack_all_routes`. Now, there are 3 different dataframes. Use `merge_all_providers` to return a single dataframe with routes that cross an area without data cellular among all three providers. 
2. To add NTD and Trips information and find an estimate of total buses that may are running in poor cellular coverage routes, `merge` the dataframe above using `A3_analysis.final_merge`. 