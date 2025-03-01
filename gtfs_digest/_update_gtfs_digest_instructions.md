# How to run GTFS Digest
Make sure these files include the most recent date's worth of data, **especially** at the  beginning of a new calendar year. 
- `cd _shared_utils/_shared_utils/rt_dates.py`
-  `cd gtfs_digest/merge_data.py`
-  `cd gtfs_digest/merge_operator_data.py`
-  `cd_gtfs_funnel`: double check that `update_vars.py` is correct before rerunning `python clean_route_naming.py`.

Proceed with assembling the data using the `Makefile`.<br>
- `cd gtfs_digest && make assemble_data`<br>

Double check that all the charts/text include the most recent date's data.<br>
-  In `gtfs_digest/03_report.ipynb` run a couple of operators which are located in the third cell, below all the `pd.options` adjustments.<br>
-  If everything looks ok, make sure to comment out all the operators again and that the fourth cell is **not** commented out. <br>

Now, it's time to run the portfolio on the **operator** grain.<br>
-  `cd` back to the root of the repo `jovyan@jupyter-amandaha8 ~/data-analyses (ah_gtfs) $ `. Run `make build_gtfs_digest` to build the portfolio. This will take a very long time!<br>

To update the **Caltrans district** and **legislative district portfolios**.<br>
-  Make sure you are back at the root of the repo. Run `build_district_digest` and `build_legislative_district_digest` respectively. <br>