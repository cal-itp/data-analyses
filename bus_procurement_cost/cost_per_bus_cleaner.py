import numpy as np
import pandas as pd
import seaborn as sns
import shared_utils

gcs_path = "gs://calitp-analytics-data/data-analyses/bus_procurement_cost/"
fta_data = "fta_bus_cost_clean.parquet"
tircp_data = "tircp_project_bus_only.parquet"
dgs_data = "dgs_agg_clean.parquet"

fta = pd.read_parquet(f"{gcs_path}{fta_data}")
tircp = pd.read_parquet(f"{gcs_path}{tircp_data}")
dgs = pd.read_parquet(f"{gcs_path}{dgs_data}")

# renaming dgs column name from quantity to bus_count
dgs.rename(columns={"quantity": "bus_count"}, inplace=True)

# make new column for dgs data, project_title that concats agency name and total cost?
dgs["project_title"] = dgs["ordering_agency_name"] + dgs["total_cost"].astype(str)

# add new col to identify source
fta["source"] = "fta_press_release"
tircp["source"] = "tircp_project_tracking"


# shorten FTA df
fta_short = fta[
    [
        "project_sponsor",
        "project_title",
        "funding",
        "bus_count",
        "prop_type",
        "bus_size_type",
        'source'
    ]
]

# shorten tircp df
tircp_short = tircp[
    [
        "grant_recipient",
        "project_title",
        "tircp_award_amount_($)",
        "bus_count",
        "prop_type",
        "bus_size_type",
        'source'
    ]
]

# dictionary to update column names in df
col_dict = {
    "project_sponsor": "agency_name",
    "funding": "project_award_amount",
    "grant_recipient": "agency_name",
    "tircp_award_amount_($)": "project_award_amount",
    "ordering_agency_name": "agency_name",
    "total_cost": "project_award_amount",
}

df_list=[fta_short, tircp_short, dgs]

# loop through df_list to rename columns per dictionary
for df in df_list:
    df.rename(columns=col_dict, inplace=True)
    
# outer merge on all columns via chaining
all_data = fta_short.merge(tircp_short, on=['agency_name',
 'bus_count',
 'project_award_amount',
 'prop_type',
 'bus_size_type',
 'source',
 'project_title'], how='outer'
).merge(dgs, on=['agency_name',
 'bus_count',
 'project_award_amount',
 'prop_type',
 'bus_size_type',
 'source',
 'project_title'], how='outer')  

# export to parquet
all_data.to_parquet(
    "gs://calitp-analytics-data/data-analyses/bus_procurement_cost/cpb_analysis_data_merge.parquet"
)