import numpy as np
import pandas as pd
import shared_utils
from calitp_data_analysis.sql import to_snakecase
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
to_int64 =[
    'contract_unit_price',
    'extended_contract_price_paid',
    'total_with_options_per_unit',
    'grand_total'
]

def get_data(path:str, file:str, sheet:str)->pd.DataFrame:
    """
    read in DGS usage report excel data from bus_procurement_cost folder
    """
    df = pd.read_excel(path + file, sheet_name=sheet)

    return df

def clean_dgs_columns():
    '''
    reads in 2 dgs sheets, adds source column, merges both DFs, snakecase columns, update dtypes for monetary columns.
    merged first becaues the snakecase function messes with the dtypes for some reason
    '''
    #read in data
    dgs_17c = get_data(gcs_path, file_17c, sheet_17c)
    dgs_17b = get_data(gcs_path, file_17b, sheet_17b)

    # add new column to identify source
    dgs_17c["source"] = "17c"
    dgs_17b["source"] = "17b"
    
    # merge
    dgs_17bc = pd.merge(dgs_17b, dgs_17c, how="outer", on=merge_col).fillna(0)
    
    # snakecase
    dgs_17bc = to_snakecase(dgs_17bc)
    
    #loop through list to update dtype
    for column in to_int64:
        dgs_17bc[column] = dgs_17bc[column].astype('int64')
    
    return dgs_17bc

def calculate_total_cost(row):
    '''
    Calculate new column for total cost by checking if total_with_options_per_unit is present or not.
    to be used with .assign()
    '''
    if row['total_with_options_per_unit'] > 0:
        return row['total_with_options_per_unit'] * row['quantity']
    else:
        return row['contract_unit_price'] * row['quantity']
    
def prop_type_finder(description):
    '''
    function that matches prop_list keywords to item_description col. returns the prop type keywords it matched on. else returns not specified.
    To be used with .assign()
    '''
    prop_list = [
        'Battery Electric Bus',
        'battery electric bus',
        'Fuel Cell Electric Bus',
        'fuel cell electric bus',
        'Hydrogen Electic Bus',
        'hydrogen electric bus',
        'battery electric',
    ]
    
    for keyword in prop_list:
        if keyword in description:
            return keyword
        
    return "not specified"

def bus_size_finder(description):
    '''
    Similar to prop_type_find, matches keywords to item description and return matching bus size type.
    To be used with .assign()
    '''
    
    size_list = [
        '35 Foot',
        '40 Foot',
        '60 foot',
        '40 foot',
        '35 foot',
    ]
    
    for keyword in size_list:
        if keyword in description:
            return keyword
    return "not specified"

def agg_by_agency(df:pd.DataFrame) ->pd.DataFrame:
    '''
    Function to aggregate dgs dataframe by transit agency and returns: the total quantity of buses, total cost of all buses, propulsion type, bus size, and source.
    First, filters the df if rows in the item description column contains 'bus' or 'Bus'.
    then drops columns. and finally aggregates by transit agency
    '''
    agg_agency_bus_count = df[(df['item_description'].str.contains('bus')) | (df['item_description'].str.contains('Bus'))]

    agg_agency_bus_count2 = agg_agency_bus_count[['ordering_agency_name',
                                            'item_description',
                                            'quantity',
                                            'source',
                                            'total_cost',
                                            'prop_type',
                                            'bus_size_type']]

    agg_agency_bus_count3 = agg_agency_bus_count2.groupby('ordering_agency_name').agg({
        'quantity':'sum',
        'total_cost':'sum',
        'prop_type':'max',
        'bus_size_type':'max',
        'source':'max',
    }).reset_index()
    return agg_agency_bus_count3


if __name__ == "__main__":

    # initial df
    dgs_17bc = clean_dgs_columns()
    
    # assigning new columns
    df2 = dgs_17bc.assign(
        total_cost=dgs_17bc.apply(calculate_total_cost, axis=1),
        prop_type=dgs_17bc["item_description"].apply(prop_type_finder),
        bus_size_type=dgs_17bc["item_description"].apply(bus_size_finder)
    )
    
    # aggregation
    df3 = agg_by_agency(df2)
    
    # export as parquet to gcs
    df3.to_parquet(f'{gcs_path}dgs_agg_clean.parquet')