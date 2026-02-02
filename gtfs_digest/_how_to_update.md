# How to run GTFS Digest
1. At `data-analyses/gtfs_digest` update the following variables in `update_vars.py`.
    - `analysis_month`: the month with the new data that you want to update the portfolio with. Ensure the format is `yyyy-mm-dd`. Using January 2026 as our example: `2026-01-01.`
    - `last_year`: the date above but from last year. Example: `2025-01-01.`
    - `previous_month`: the previous month from the analysis_month. Example: `2025-12-01.`<p>
   
2. To update the portfolio for the **operator** grain.<br>
    -  `cd` back to the root of the repo `jovyan@jupyter-amandaha8 ~/data-analyses`.
    -  Run `make build_gtfs_digest` to build the portfolio. This will take a very long time!<p>
      
3. To update the **Caltrans district** and **legislative district portfolios**.<br>
-  Make sure you are back at the root of the repo. Run `build_district_digest` and `build_legislative_district_digest` respectively. <p>

4. Alternatively, if you want to update the data without updating the portfolio sites.
- `cd` to `jovyan@jupyter-amandaha8 ~/data-analyses/gtfs_digest` and run `make digest_report` in the terminal. 