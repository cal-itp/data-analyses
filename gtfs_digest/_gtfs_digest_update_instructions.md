# How to run GTFS Digest
1. At `data-analyses/gtfs_digest` update the following variables in `update_vars.py`.
    - `analysis_month`: the month with the most current data you want to update the portfolio with. Ensure the format is `yyy-mm-dd`. 
    - `last_year`: the month from above, only from last year.
    - `previous_month`: the previous month from the analysis_month.
   
2. Run the portfolio on the **operator** grain.<br>
    -  `cd` back to the root of the repo `jovyan@jupyter-amandaha8 ~/data-analyses (ah_gtfs) $ `.
    -  Run `make build_gtfs_digest` to build the portfolio. This will take a very long time!3. To update the **Caltrans district** and **legislative district portfolios**.<br>
-  Make sure you are back at the root of the repo. Run `build_district_digest` and `build_legislative_district_digest` respectively. <br>