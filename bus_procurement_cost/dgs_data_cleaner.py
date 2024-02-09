import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter
import numpy as np
import pandas as pd
import seaborn as sns
import shared_utils
from scipy.stats import zscore

#args for get_data
GCS_FILE_PATH ='gs://calitp-analytics-data/data-analyses/bus_procurement_cost/'
file_17c = '17c compiled-Proterra Compiled Contract Usage Report .xlsx'
file_17b = '17b compiled.xlsx'
sheet_17c = 'Proterra '
sheet_17b = 'Usage Report Template'

def get_data(path, file, sheet):
    '''
    read in DGS usage report excel data from bus_procurement_cost folder'''
    df = pd.read_excel(path + file, sheet_name=sheet)
    
    return df

dgs_17c = get_data(GCS_FILE_PATH, file_17c, sheet_17c)
dgs_17b = get_data(GCS_FILE_PATH, file_17b, sheet_17b)

# add new column to identify source
dgs_17c['source'] = '17c'
dgs_17b['source'] = '17b'


# merge dataframes
merge_col=['Supplier Contract Usage ID',
          'Ordering Agency Name',
          'State (S) or Local (L) agency',
          'Purchasing Authority Number                    (for State departments)',
          'Agency Billing Code',
          'Purchase Order Number',
          'Purchase Order Date',
          'Delivery Date',
          'Contract Line Item Number (CLIN)                (RFP ID)',
          'UNSPSC Code\n(Version 10)',
          'Manufacturer Part Number (OEM #)',
          'Manufacturer (OEM)',
          'Item Description',
          'Unit of Measure',
          'Quantity in \nUnit of Measure\n',
          'Quantity',
          'List Price/MSRP',
          'Index Date / Catalog Version',
          'Contract Unit Price',
          'Extended Contract Price Paid',
          'source']

dgs_17bc = pd.merge(dgs_17b, dgs_17c, how='outer', on= merge_col).fillna(0)


def snake_case(df):
    '''
    Snake case df columns
    '''
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.strip()
    
snake_case(dgs_17bc)

# check financial columns to be `int64`
money =['contract_unit_price',
       'extended_contract_price_paid',
       'total_with_options_per_unit',
       'grand_total']

# loop that takes money list to convert to int64 dtype
for column in money:
    dgs_17bc[column] = dgs_17bc[column].astype('int64')
    
# drop unnessary columns?
drops =['supplier_contract_usage_id',
       'state_(s)_or_local_(l)_agency',
       'purchasing_authority_number____________________(for_state_departments)',
       'agency_billing_code',
       'unspsc_code\n(version_10)',
       'unit_of_measure',
       'epp_(y/n)_x',
       'epp_(y/n)_y',
       'list_price/msrp',
       'index_date_/_catalog_version',
       'core/_noncore',
       'group_id/_segment_id']

dgs_17bc.drop(columns=drops, inplace=True)

def calculate_total_cost(row):
    '''
    Calculate new column for total cost by checking is total_with_options_per_unit is present or not
    '''
    if row['total_with_options_per_unit'] > 0:
        return row['total_with_options_per_unit'] * row['quantity']
    else:
        return row['contract_unit_price'] * row['quantity']
    
dgs_17bc['total_cost'] = dgs_17bc.apply(calculate_total_cost, axis=1)


# new column for prop_type

prop_list = ['Battery Electric Bus',
            'battery electric bus',
            'Fuel Cell Electric Bus',
            'fuel cell electric bus',
            'Hydrogen Electic Bus',
            'hydrogen electric bus',
            'battery electric',
            ]


def prop_type_finder(description):
    '''
    function that matches prop_list keywords to item_description col. returns the prop type keywords it matched on. else returns not specified
    '''
    for keyword in prop_list:
        if keyword in description:
            return keyword
    return "not specified"


# add new col `prop_type`, fill it with values based on project_description using prop_type_finder function
dgs_17bc["prop_type"] = dgs_17bc["item_description"].apply(prop_type_finder)

size_list =['35 Foot',
           '40 Foot',
           '60 foot',
           '40 foot',
           '35 foot',
           ]

def bus_size_finder(description):
    '''
    Similar to prop_type_find, matches keywords to item description and return matching bus size type'''
    for keyword in size_list:
        if keyword in description:
            return keyword
    return "not specified"

dgs_17bc["bus_size_type"] = dgs_17bc["item_description"].apply(bus_size_finder)


# agency bus count
# filtered df by item desc containing 'bus' or 'Bus'
agg_agency_bus_count = dgs_17bc[(dgs_17bc['item_description'].str.contains('bus')) | (dgs_17bc['item_description'].str.contains('Bus'))]

agg_agency_bus_count = agg_agency_bus_count[['ordering_agency_name',
                                            'item_description',
                                            'quantity',
                                            'source',
                                            'total_cost',
                                            'prop_type',
                                            'bus_size_type']]

#i think this is it.. the numbers are matching up
agg_agency_bus_count = agg_agency_bus_count.groupby('ordering_agency_name').agg({
    'quantity':'sum',
    'total_cost':'sum',
    'prop_type':'max',
    'bus_size_type':'max',
    'source':'max',
}).reset_index()


'''export as parquet to GCS bus_procurement_cost folder'''
agg_agency_bus_count.to_parquet('gs://calitp-analytics-data/data-analyses/bus_procurement_cost/dgs_agg_clean.parquet')

