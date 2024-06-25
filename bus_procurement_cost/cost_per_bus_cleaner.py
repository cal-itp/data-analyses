import pandas as pd
from bus_cost_utils import *
from scipy.stats import zscore



def prepare_all_data() ->pd.DataFrame:
    """
    primary function to read-in, merge data across FTA, TIRCP and DGS data.
    standardizes columns names, then exports as parquet.
    """
    # variables for file names


    
    # dictionary to update columns names 
    col_dict = {
        "funding": "total_cost",
        "grant_recipient": "transit_agency",
        "new_bus_size": "bus_size_type",
        "new_bus_size_type": "bus_size_type",
        "new_prop_type": "prop_type",
        "new_prop_type_finder": "prop_type",
        "ordering_agency_name": "transit_agency",
        "purchase_order_number": "ppno",
        "quantity": "bus_count",
        "total_project_cost": "total_cost",
        "project_sponsor": "transit_agency",
    }

    # reading in data
    # bus only projects for each datase
    fta = pd.read_parquet(f"{GCS_PATH}clean_fta_bus_only.parquet")
    tircp = pd.read_parquet(f"{GCS_PATH}clean_tircp_bus_only_clean.parquet")
    dgs = pd.read_parquet(f"{GCS_PATH}clean_dgs_bus_only_w_options.parquet")
    
    # adding new column to identify source
    fta["source"] = "fta"
    tircp["source"] = "tircp"
    dgs["source"] = "dgs"

    # using .replace() with dictionary to update column names
    fta2 = fta.rename(columns=col_dict)
    tircp2 = tircp.rename(columns=col_dict)
    dgs2 = dgs.rename(columns=col_dict)
    
    # merging fta2 and tircp 2
    merge1 = pd.merge(fta2,
        tircp2,
        on=[
            "transit_agency",
            "prop_type",
            "bus_size_type",
            "total_cost",
            "bus_count",
            "source",
            "new_project_type"
        ],
        how="outer",
    )
    
    # mergeing merge1 and dgs2
    merge2 = pd.merge(merge1,
        dgs2,
        on=[
            "transit_agency",
            "prop_type",
            "bus_size_type",
            "total_cost",
            "bus_count",
            "source",
            "ppno",
        ],
        how="outer",
    )
    #normalizing data with cost per bus
    #calculating cost per bus here
    merge2["cost_per_bus"] = (merge2["total_cost"] / merge2["bus_count"]).astype("int64")
    
    #calculating zscore on cost per bus
    merge2["zscore_cost_per_bus"] = zscore(merge2["cost_per_bus"])
   
    #flag any outliers
    merge2["is_cpb_outlier?"] = merge2["zscore_cost_per_bus"].apply(outlier_flag)
    return merge2




if __name__ == "__main__":
    
    # initial df
    df1 = prepare_all_data()
    #remove outliers based on cost per bus zscore
    df2 = df1[df1["is_cpb_outlier?"]==False]
    
    # export to gcs
    #full data, with outliers
    df1.to_parquet(f'{GCS_PATH}cleaned_cpb_analysis_data_merge.parquet')
    # no outliers
    df2.to_parquet(f'{GCS_PATH}cleaned_no_outliers_cpb_analysis_data_merge.parquet')