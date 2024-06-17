#script with shared functions used throughout the bus cost analysis.

import pandas as pd

GCS_PATH = "gs://calitp-analytics-data/data-analyses/bus_procurement_cost/"

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
    
def project_type_finder(description: str) -> str:
    """
    function to match keywords to project description col to identify projects that only have bus procurement.
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
    
def col_row_updater(df: pd.DataFrame, col1: str, val1, col2: str, new_val):
    """
    function used to update values at specificed columns and row value.
    """
    df.loc[df[col1] == val1, col2] = new_val
    
    return