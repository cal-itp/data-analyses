import pandas as pd
from fta_data_cleaner import gcs_path

def prepare_data() ->pd.DataFrame:
    """
    primary function to read-in, merge data across FTA, TIRCP and DGS data.
    standardizes columns names, then exports as parquet.
    """
    # variables for file names
    fta_bus_data = "fta_bus_cost_clean.parquet"
    tircp_bus_data = "clean_tircp_project_bus_only.parquet"
    dgs_bus_data = "dgs_agg_w_options_clean.parquet"
    
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
    fta = pd.read_parquet(f"{gcs_path}{fta_bus_data}")
    tircp = pd.read_parquet(f"{gcs_path}{tircp_bus_data}")
    dgs = pd.read_parquet(f"{gcs_path}{dgs_bus_data}")
    
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
    
    return merge2

if __name__ == "__main__":
    
    # initial df
    df1 = prepare_data()
    
    # export to gcs
    df1.to_parquet(f'{gcs_path}cpb_analysis_data_merge.parquet')