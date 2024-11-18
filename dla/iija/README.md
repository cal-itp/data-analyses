# IIJA Data Cleaning

This folder includes the data exploration, cleaning and script for the IIJA program data that is uploaded to [RebuildingCA](https://rebuildingca.ca.gov/map/??). 

#### To run the script, run through the following steps:
0. Receive list of FMIS IIJA funded projects from DOTP Office of Technical Freight & Project Integration

1. Upload list (CSV or Excel) to the GCS Bucket `/data-analyses/dla/dla-iija/`

2. In terminal, cd to `data-analyses/dla`, then run `make setup_env` to install requirements. cd to `iija` afterwards. 

    **Note:** install may fail during conda install, but should still be able to continue with the rest of these instructions.

3. Open `run_script.ipynb`, in the `Read in Data and function development` section update the `my_file` path to the latest IIJA project list in GCS bucket.

4. In the `Check Data` section, run the cells to ensure the dataframe has no empty first rows

5. In the `Run Script` section, run the `_script_utils.run_script(*,*,'agg')` function to get the final cleaned data. 

    The format preferred by stakeholders is for the data to be aggregated to the program level, use this kwarg in the fucntion 
    <blockquote>_script_utils.run_script(*,*,df_agg_level = 'agg')</blockquote>
    Or, If you want the full dataframe where each row is a project phase, use this kwarg in the fucntion
    <blockquote>_script_utils.run_script(*,*,df_agg_level = 'full')</blockquote>

6. In the `Export Data` section, Use the current date `(MMDDYYYY)`, (or `(MMDDYYYY_agg)` if the df_agg_level was set to `agg`), to set the filename suffix. Then, run the `_script_utils.export_to_gcs()` function to export the data to GCS.  
    The data can be found in the same file path stated in the previous steps, with the file name `FMIS_Projects_Universe_IIJA_Reporting_*.csv`

**Note:** In the aggregated data, a project can have more than one row, if the project is funded with more than one IIJA program. 


#### Scripts
* [_data_utils.py](https://github.com/cal-itp/data-analyses/blob/main/dla/iija/_data_utils.py): Functions to read in data and identify the implementing agency
* [_script_utils.py](https://github.com/cal-itp/data-analyses/blob/main/dla/iija/_script_utils.py): Functions to clean data.