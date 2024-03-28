import numpy as np
import pandas as pd
import shared_utils
from calitp_data_analysis.sql import to_snakecase


def calculate_total_cost(row):
    """
    Calculate new column for total cost by checking if total_with_options_per_unit is present or not.
    if not, then calculate using contract_unit_price.
    to be used with .assign()
    """
    if row["total_with_options_per_unit"] > 0:
        return row["total_with_options_per_unit"] * row["quantity"]
    else:
        return row["contract_unit_price"] * row["quantity"]


def new_bus_size_finder(description: str) -> str:
    """
    Similar to prop_type_find, matches keywords to item description col and return standardized bus size type.
    now includes variable that make description input lowercase.
    To be used with .assign()
    """

    articulated_list = [
        "60 foot",
        "articulated",
    ]

    standard_bus_list = [
        "30 foot",
        "35 foot",
        "40 foot",
        "40ft",
        "45 foot",
        "standard",
    ]

    cutaway_list = [
        "cutaway",
    ]

    other_bus_size_list = ["feeder bus"]

    otr_bus_list = [
        "coach style",
        "over the road",
    ]

    item_description = description.lower().replace("-", " ").strip()

    if any(word in item_description for word in articulated_list):
        return "articulated"

    elif any(word in item_description for word in standard_bus_list):
        return "standard/conventional (30ft-45ft)"

    elif any(word in item_description for word in cutaway_list):
        return "cutaway"

    elif any(word in item_description for word in otr_bus_list):
        return "over-the-road"

    elif any(word in item_description for word in other_bus_size_list):
        return "other"

    else:
        return "not specified"


# new prop_finder function
def new_prop_finder(description: str) -> str:
    """
    function that matches keywords from each propulsion type list against the item description col, returns a standardized prop type
    now includes variable that make description input lowercase.
    to be used with .assign()
    """

    BEB_list = [
        "battery electric",
        "BEBs paratransit buses"
    ]

    cng_list = [
        "cng",
        "compressed natural gas"
        
    ]

    electric_list = [
        "electric buses",
        "electric commuter",
        "electric",
    ]

    FCEB_list = [
        "fuel cell",
        "hydrogen",
        #"fuel cell electric",
        #"hydrogen fuel cell",
        #"fuel cell electric bus",
        #"hydrogen electric bus",
    ]

    # low emission (hybrid)
    hybrid_list = [
        #"diesel electric hybrids",
        #"diesel-electric hybrids",
        #"hybrid electric",
        #"hybrid electric buses",
        #"hybrid electrics",
        "hybrids",
        "hybrid",
    ]

    # low emission (propane)
    propane_list = [
        #"propane buses",
        #"propaned powered vehicles",
        "propane",
    ]

    mix_beb_list = [
        "2 BEBs and 4 hydrogen fuel cell buses",
    ]

    mix_lowe_list = [
        "diesel and gas",
    ]

    mix_zero_low_list = [
        "15 electic, 16 hybrid",
        "4 fuel cell / 3 CNG",
        "estimated-cutaway vans (PM- award will not fund 68 buses",
        "1:CNGbus ;2 cutaway CNG buses",
    ]

    zero_e_list = [
        #"zero emission buses",
        #"zero emission electric",
        #"zero emission vehicles",
        "zero-emission",
        "zero emission",
    ]

    item_description = description.lower().replace("‐", " ").strip()

    if any(word in item_description for word in BEB_list) and not any(
        word in item_description for word in ["diesel", "hybrid", "fuel cell"]
    ):
        return "BEB"

    elif any(word in item_description for word in FCEB_list):
        return "FCEB"

    elif any(word in item_description for word in hybrid_list):
        return "low emission (hybrid)"

    elif any(word in item_description for word in mix_beb_list):
        return "mix (BEB and FCEB)"

    elif any(word in item_description for word in mix_lowe_list):
        return "mix (low emission)"

    elif any(word in item_description for word in mix_zero_low_list):
        return "mix (zero and low emission)"

    elif any(word in item_description for word in zero_e_list):
        return "zero-emission bus (not specified)"

    elif any(word in item_description for word in propane_list):
        return "low emission (propane)"

    elif any(word in item_description for word in electric_list):
        return "electric (not specified)"
    
    elif any(word in item_description for word in cng_list):
        return "CNG"

    else:
        return "not specified"

#project type checker
def project_type_checker(description: str) -> str:
    """
    function to match keywords to project description col to identy projects that only have bus procurement.
    used to identify projects into diffferent categories: bus only, bus + others, no bus procurement.
    use with .assign() to get a new col.
    """
    bus_list =[
        "bus",
        "transit vehicles",# for fta list
        "cutaway vehicles",# for fta list
        "zero-emission vehicles", # for tircp list
        "zero emission vehicles",
        "zero‐emissions vans",
        "hybrid-electric vehicles",
        "battery-electric vehicles",
        "buy new replacement vehicles", # specific string for fta list
    ]
    
    exclude_list =[
        "facility",
        #"station",
        "stops",
        "installation",
        "depot",
        "construct",
        "infrastructure",
        "signal priority",
        "improvements",
        "build",
        "chargers",
        "charging equipment",
        "install",
        "rail",
        "garage",
        "facilities",
        "bus washing system",
        "build a regional transit hub" # specific string needed for fta list
        #"associated infrastructure" may need to look at what is associated infrastructure is for ZEB 
        
    ]
    proj_description = description.lower().strip()

    if any(word in proj_description for word in bus_list) and not any(
        word in proj_description for word in exclude_list
    ):
        return "bus only"
    
    elif any(word in proj_description for word in exclude_list) and not any(
        word in proj_description for word in bus_list
    ):
        return "non-bus components"
    
    elif any(word in proj_description for word in exclude_list) and any(
        word in proj_description for word in bus_list
    ):
        return "includes bus and non-bus components"
    
    else:
        return "needs review"

# included assign columns
def clean_dgs_columns() -> pd.DataFrame:
    """
    reads in 2 dgs sheets, adds source column, merges both DFs, snakecase columns, update dtypes for monetary columns.
    merged first becaues the snakecase function messes with the dtypes for some reason
    """
    
    from fta_data_cleaner import gcs_path
    
    # params
    file_17c = "17c compiled-Proterra Compiled Contract Usage Report .xlsx"
    file_17b = "17b compiled.xlsx"
    sheet_17c = "Proterra "
    sheet_17b = "Usage Report Template"

    # merge columns for dataframes
    merge_col = [
        "Agency Billing Code",
        "Contract Line Item Number (CLIN)                (RFP ID)",
        "Contract Unit Price",
        "Delivery Date",
        "Extended Contract Price Paid",
        "Index Date / Catalog Version",
        "Item Description",
        "List Price/MSRP",
        "Manufacturer (OEM)",
        "Manufacturer Part Number (OEM #)",
        "Ordering Agency Name",
        "Purchase Order Date",
        "Purchase Order Number",
        "Purchasing Authority Number                    (for State departments)",
        "Quantity in \nUnit of Measure\n",
        "Quantity",
        "source",
        "State (S) or Local (L) agency",
        "Unit of Measure",
        "UNSPSC Code\n(Version 10)",
        "Supplier Contract Usage ID",
    ]

    # columns to change dtype
    to_int64 = [
        "contract_unit_price",
        "extended_contract_price_paid",
        "total_with_options_per_unit",
        "grand_total",
    ]
    
    # read in data
    dgs_17c = pd.read_excel(f"{gcs_path}{file_17c}", sheet_name=sheet_17c)
    dgs_17b = pd.read_excel(f"{gcs_path}{file_17b}", sheet_name=sheet_17b)

    # add new column to identify source
    dgs_17c["source"] = "17c"
    dgs_17b["source"] = "17b"

    # merge
    dgs_17bc = pd.merge(dgs_17b, dgs_17c, how="outer", on=merge_col).fillna(0)

    # snakecase
    dgs_17bc = to_snakecase(dgs_17bc)

    # takes list of columns and updates to int64
    dgs_17bc[to_int64] = dgs_17bc[to_int64].astype("int64")

    # change purchase_order_number col to str
    dgs_17bc["purchase_order_number"] = dgs_17bc["purchase_order_number"].astype("str")

    # adds 3 new columns from functions
    dgs_17bc2 = dgs_17bc.assign(
        total_cost=dgs_17bc.apply(calculate_total_cost, axis=1),
        new_prop_type=dgs_17bc["item_description"].apply(new_prop_finder),
        new_bus_size=dgs_17bc["item_description"].apply(new_bus_size_finder),
    )

    return dgs_17bc2

def agg_by_agency(df: pd.DataFrame) -> pd.DataFrame:
    """
    function that aggregates the DGS data frame by transit agency and purchase order number (PPNO) to get total cost of just buses without options.
    first, dataframe is filtered for rows containing buses (does not include rows with 'not specified').
    then, group by agency, PPNO, prop type and bus size. and aggregate the quanity and total cost of just buses.
    Possible for agencies to have multiple PPNOs for different bus types and sizes.
    """
    # filter for rows containing bus, does not include accessories/warranties/parts/etc.
    agg_agency_bus_count = df[~df["new_prop_type"].str.contains("not specified")]

    agg_agency_bus_count2 = agg_agency_bus_count[
        [
            "ordering_agency_name",
            "purchase_order_number",
            "item_description",
            "quantity",
            "source",
            "total_cost",
            "new_prop_type",
            "new_bus_size",
        ]
    ]

    agg_agency_bus_count3 = (
        agg_agency_bus_count2.groupby(
            [
                "ordering_agency_name",
                "purchase_order_number",
                "new_prop_type",
                "new_bus_size",
            ]
        )
        .agg(
            {
                "quantity": "sum",
                "total_cost": "sum",
                "source": "max",
            }
        )
        .reset_index()
    )

    return agg_agency_bus_count3


def agg_by_agency_w_options(df: pd.DataFrame) -> pd.DataFrame:
    """
    similar to the previous function, aggregates the DGS dataframe by transit agency to get total cost of buses with options.
    agencies may order buses with different configurations, resulting in different total cost.
    function creates 1 df of only buses to retain initial proulsion type, size type and quanity of buses.
    then, creates 2nd df of aggregated total cost of buses+options, by transit agency.
    lastly, both df's are merged together.
    """
    # filter df for rows NOT containing 'not specified'. only returns rows with buses
    dfa = df[~df["new_prop_type"].str.contains("not specified")]

    # keep specific columns
    df2 = dfa[
        [
            "ordering_agency_name",
            "purchase_order_number",
            "quantity",
            "new_prop_type",
            "new_bus_size",
            "source",
        ]
    ]

    # aggregate by agency and PPNO, get total cost of buses with options
    df3 = (
        df.groupby(["ordering_agency_name", "purchase_order_number"])
        .agg({"total_cost": "sum"})
        .reset_index()
    )

    # merge both dataframes on agency and PPNO to get bus only rows & total cost with options.
    merge = pd.merge(
        df2, df3, on=["ordering_agency_name", "purchase_order_number"], how="left"
    )

    return merge

if __name__ == "__main__":
    
    from fta_data_cleaner import gcs_path
    # initial df
    df1 = clean_dgs_columns()
    
    #df of just bus cost (no options)
    just_bus = agg_by_agency(df1)
    
    #df of bus cost+options
    bus_w_options = agg_by_agency_w_options(df1)
    
    #export serperate df's as parquet to GCS
    just_bus.to_parquet(f'{gcs_path}dgs_agg_clean.parquet')
    bus_w_options.to_parquet(f'{gcs_path}dgs_agg_w_options_clean.parquet')