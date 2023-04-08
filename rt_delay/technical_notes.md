# Technical Notes

## Building the [Speedmap Site](https://analysis.calitp.org/rt/README.html)

### The Scripts

`build_speedmaps_index.py` queries the warehouse to map organizations to associated RT and Schedule datasets. Set `ANALYSIS_DATE` here before running, (currently must be a date with vehicle positions data already fetched using `open_data/download_vehicle_positions.py`). It also creates a temporary parquet in this directory, `rt_progress_{date}.parquet` used to track status across all scripts.

`check_stage_intermediate.py` checks if there is intermediate data present in GCS from running `rt_parser.OperatorDayAnalysis` for each organization on the date specified in the last script. If there is not, this script will attempt to generate and save that data using `OperatorDayAnalysis`. The status of each attempt is saved in `rt_progress_{date}.parquet`.

`check_test_mapping.py` checks if it is possible to generate a map using `rt_filter_map_plot.RtFilterMapper` for each organization. The status of each attempt is saved in `rt_progress_{date}.parquet`. This avoids attempting to run the portfolio script for operators that will error or create a blank speedmap page.

`stage_run_portfolio.py` edits `portfolio/sites/rt.yml` to include all organizations for which it was possible to generate a map. It then runs the portfolio scripts to stage and deploy the site!

### The Makefile

Use `make generate_speedmaps_slowly` to run the above scripts in order. If intermediate data is not already present this process is currently slow, otherwise it's just a little bit slow. Note that you only need to set `ANALYSIS_DATE` once, in `build_speedmaps_index.py`. If this process is interrupted for some reason, the status parquet enables a graceful resume without duplicate work. Simply run this command again and the scripts will pick up where they left off.

`make clean_speedmap_progress` removes the status parquet. Use this to clean up after a successful run, or remove it before running the first command to ensure the scripts try to generate intermediate data and maps for each organization, even if they failed before.
