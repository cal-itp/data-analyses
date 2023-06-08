# IIJA Data Cleaning

This folder includes the data exploration, cleaning and script for the IIJA program data that is uploaded to [RebuildingCA](https://rebuildingca.ca.gov/map/??). 

#### To run the script, run through the following steps:
1. Upload exported data from FMIS (CSV or Excel) to the GCS Bucket
2. Open `run_script.ipynb` and change path to relfect the uploaded exported data
3. Check that the exported file has no empty first rows
4. Run cells up to `Test & Export`
5. In the `Test & Export` section, run the function 
<blockquote>`_script_utils.get_clean_data()`</blockquote> 
to get the final cleaned data. If you want to get the data aggregated to the program level, use kwargs <blockquote>_script_utils.get_clean_data(df, full_or_agg = 'agg')</blockquote>
If you want the full dataframe where each row is a project phase, use kwarg
<blockquote>_script_utils.get_clean_data(df, full_or_agg = 'full')</blockquote>

Note: In the aggregated data, a project can have more than one row, if the project is funded with more than one IIJA program. 


#### Scripts
* [_data_utils.py](https://github.com/cal-itp/data-analyses/blob/main/dla/iija/_data_utils.py): Functions to read in data and identify the implementing agency
* [_script_utils.py](https://github.com/cal-itp/data-analyses/blob/main/dla/iija/_script_utils.py): Functions to clean data.