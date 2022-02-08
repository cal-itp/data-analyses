import geopandas as gpd
import os
import numpy as np
import pandas as pd

from calitp.tables import tbl
from siuba import *

import utils
import shared_utils

ix = pd.IndexSlice

DATA_PATH = "./data/test/"

# Merge routes with tract type (notebook A3)
def merge_routes_with_tract_type():
    
    service = pd.read_parquet(f"{DATA_PATH}shape_frequency_funding.parquet")
    processed_shapes = gpd.read_parquet(f'{DATA_PATH}shapes_processed.parquet')
    
    service_tract_type = pd.merge(
        (service 
         >> select(_.calitp_itp_id, _.shape_id, 
                   _.day_name, _.departure_hour, 
                   _.trips_per_hour, _.mean_runtime_min)
        ),
        (processed_shapes
         >> select(_.calitp_itp_id, _.shape_id, _.tract_type)
        ),
        on = ['calitp_itp_id', 'shape_id'],
        how = "inner",
        validate = "m:1",
    )
    
    # Filter and keep 5am-9pm hours
    service_tract_type = (service_tract_type 
                          >> filter(_.departure_hour > 4, _.departure_hour < 21) 
                          ## filter for performance
                         )
    
    service_tract_type = service_tract_type.assign(
        min_runtime_min = (service_tract_type.groupby(["calitp_itp_id", "shape_id"])
                       ["mean_runtime_min"].transform("min")
                      )
    )
    
    service_tract_type = (service_tract_type
                      .dropna(subset=["tract_type", "min_runtime_min"])
                      .reset_index(drop=True)
                     )
    
    
    ## runtime for analysis is the mean runtime for a shape/day/hour for existing service,
    # or the min runtime for new service
    service_tract_type = service_tract_type.assign(
        runtime = service_tract_type[
            ["mean_runtime_min", "min_runtime_min"]].max(axis=1).astype(int)
    )
    
    return service_tract_type



## {tract type: target trips per hour}
target_frequencies = {
    'urban': 4, 
    'suburban': 2, 
    'rural': 1
} 


def calculate_additonal_trips(row, target_frequencies):
    if row.trips_per_hour < target_frequencies[row.tract_type]:
        additional_trips = (target_frequencies[row.tract_type]
                            - row.trips_per_hour)
    else:
        additional_trips = 0
    row['additional_trips'] = additional_trips
    return row


def annual_service_hours(df, MULTIPLIER):
    df = df.assign(
        service_hours_annual = df.service_hrs * MULTIPLIER,
        addl_service_hrs_annual = df.addl_service_hrs * MULTIPLIER,
    )
    return df  


# Calculate route-level additional service hours / trips / annual extrapolation
def calculate_additional_trips_service_hours(df):
    WEEKEND = ["Saturday", "Sunday"]

    service_dfs = {
        "weekday": df[~df.day_name.isin(WEEKEND)].reset_index(drop=True),
        "weekend": df[df.day_name.isin(WEEKEND)].reset_index(drop=True),
    }
    
    new_service_df = pd.DataFrame()
    
    for key, data in service_dfs.items():
        days_in_sample = data.day_name.nunique()
        
        # For weekday (if just 1 day present, 52 * (5/1) = 52*5 = 260)
        # For weekend (if both days present, 52 * (2/2) = 52*1 = 52)
        # For weekend (if 1 day present, 52 * (2/1) = 52*2 = 104)
        if key == "weekday":
            MULTIPLIER = (52 * (5 / days_in_sample))
        elif key == "weekend":
            MULTIPLIER = (52 * (2 / days_in_sample))
        
        # calculate additional trips
        df = data.apply(calculate_additonal_trips, axis=1, 
                        args=(target_frequencies,))
        
        ## divide minutes to hours
        df = df.assign(
            service_hrs = (df.runtime * df.trips_per_hour) / 60,
            addl_service_hrs = (df.runtime * df.additional_trips) / 60
        )
        
        
        # Grab the annual service hours calculation
        # Correctly scale up, depending on how many days are represented
        df2 = annual_service_hours(df, MULTIPLIER)
        
        new_service_df = pd.concat(
            [new_service_df, df2], 
            axis=0, ignore_index=True)
    
    
    new_service_df = new_service_df >> arrange(_.calitp_itp_id, _.shape_id, 
                                               _.day_name, _.departure_hour)
    return new_service_df



def prep_ntd_metrics():
    df = (pd.read_csv(f"{utils.GCS_FILE_PATH}ntd_metrics_2019.csv") 
                        >> filter(_.State == 'CA')
                       )
        
    df = (df[['Agency', 'NTD ID','Mode', 'Vehicle Revenue Hours']]
          .rename(columns = {'NTD ID': 'ntd_id', 
                             'Vehicle Revenue Hours': 'vrh',
                            })
         )
          
    def fix_vrh(value):
        if type(value) != str:
            return None
        else:
            return value.replace(',', '').strip()
          
    df['vrh'] = df['vrh'].apply(fix_vrh).astype('int64')
    bus_modes = ['CB', 'MB', 'RB', 'TB']
          
    total_vrh = (df >> filter(_.Mode.isin(bus_modes)) 
                 >> group_by('Agency', 'ntd_id') 
                 >> summarize(total_vrh = _.vrh.sum())
                )
          
    return total_vrh


def prep_ntd_vehicles():
    df = (pd.read_csv(f"{utils.GCS_FILE_PATH}ntd_vehicles_2019.csv") 
          >> filter(_.State == 'CA')   
         )

    bus_modes = ['bus', 'artic_bus', 'otr_bus', 'dbl_deck_bus', 'trolleybus']  

    df = (df.assign(
              Bus = df.Bus.str.replace(',', '')
          ).rename(columns={'NTD ID': 'ntd_id', 'Bus': 'bus', 
                           'Articulated Bus': 'artic_bus',
                           'Over-The-Road Bus': 'otr_bus', 
                           'Double Decker Bus':'dbl_deck_bus',
                           'Trolleybus': 'trolleybus'})
          [['ntd_id'] + bus_modes]
         )

    df[bus_modes] = df[bus_modes].astype(int)
    
    return df
    

def add_ntd_operator_vrh_buses():
    total_vrh = prep_ntd_metrics()
    vehicles = prep_ntd_vehicles()
    
    ntd = vehicles >> inner_join(_, total_vrh, on='ntd_id')
    
    bus_modes = list(vehicles.columns) 
    bus_modes.remove('ntd_id')
    
    ntd['total_buses'] = ntd[bus_modes].sum(axis=1)
    ntd['vrh_per_bus'] = ntd['total_vrh'] / ntd['total_buses']
    
    ## filter outliers with very small fleets
    ntd = ntd[ntd['total_buses'] > 5] 
    
    # Add in 5311 funds info
    ntd_joined = (tbl.views.transitstacks()
                  >> select(_.calitp_itp_id == _.itp_id, _.ntd_id, _._5311_funds)
                  >> collect()
                  >> inner_join(_, ntd, on='ntd_id')
                 )
    
    return ntd_joined
    

def calculate_operator_capex():
    # Bring in service df
    df = pd.read_parquet(f"{DATA_PATH}service_increase.parquet")
    by_operator = (df.groupby(['calitp_itp_id', 'tract_type'])
                   [['addl_service_hrs_annual']].sum()
                  )
    
    ## https://ww2.arb.ca.gov/resources/documents/transit-fleet-cost-model
    BUS_COST = 776_941
    BUS_SERVICE_LIFE = 14 # use this assumption
    
    ntd_joined = pd.read_parquet(f'{DATA_PATH}vehicles_ntd_joined.parquet')
    MEDIAN_VRH_PER_BUS = ntd_joined['vrh_per_bus'].median()
    
    by_operator['additional_buses'] = by_operator.addl_service_hrs_annual / MEDIAN_VRH_PER_BUS
    by_operator['bus_capex'] = by_operator.additional_buses * BUS_COST
    by_operator['bus_capex_annualized'] = by_operator['bus_capex'] / BUS_SERVICE_LIFE

    return by_operator

    
# Put all the functions together
if __name__ == "__main__":
    
    # Merge in tract type
    service_tract_type = merge_routes_with_tract_type()
    # Calculate service hrs, trips by route and extrapolate to annual numbers
    service_combined = calculate_additional_trips_service_hours(service_tract_type)
    service_combined.to_parquet(f"{DATA_PATH}service_increase.parquet")
    
    # Prep NTD data for VRH and # buses
    ntd_joined = add_ntd_operator_vrh_buses()
    ntd_joined.to_parquet(f'{DATA_PATH}vehicles_ntd_joined.parquet')
    
    # Calculate operator capital expenditures
    hours_by_operator = calculate_operator_capex()
    hours_by_operator.to_parquet(f'{DATA_PATH}increase_by_operator.parquet')
