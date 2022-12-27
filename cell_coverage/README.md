# Cell Coverage
## Workflow
### Federal Communications Commission (FCC) data prep
1. The AT&T, Verizon, and T-Mobile files were downloaded [here](https://us-fcc.app.box.com/s/f220avmxeun345o6gzr7rwcnp1wslocf). Only `data` was downloaded, not `voice.` The main landing page of the data is [here](https://fcc.maps.arcgis.com/apps/webappviewer/index.html?id=6c1b2e73d9d749cdb7bc88a0d1bdd25b). 
2. The shapefiles for AT&T and Verizon were for the entire USA. Using the function `create_california_coverage`, these maps were clipped to California. 
    * `att_ca_only.parquet`
    * `verizon_ca_only.parquet` 
3. The shapefiles for T-Mobile were split by region. California's data is combined with other neighboring states across several files. As such, T-Mobile had to be concatted together. 
    * `T_Mobile_349525.zip`
    * `T_Mobile_349527.zip`
    * `T_Mobile_349687.zip`
    * `tmobile_california.parquet` is the concatted mostly CA-only file. 
4. After narrowing down the provider maps to only include California,use `complete_difference_provider_district_level` to find the areas of California by district <b>without</b> coverage. The final outputs are: 
    * `load_att`
    * `load_tmobile`
5. Verizon did not play nicely with the `districts` shapefile. As such, had to use `get_counties` counties to find the difference and clip. 
    * Kern County threw the error message: <i>Non node intersection line string error</i>  
        * Corrected Kern using `correct_kern`
    * For the rest of the counties, use `breakout_counties` to find the difference and clip. 
    * Concat all counties together using `concat_all_areas`. 
    * The final shapefile is `load_verizon`.