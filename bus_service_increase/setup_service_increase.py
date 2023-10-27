"""
"""
import geopandas as gpd
import pandas as pd

from siuba import *

from bus_service_utils import utils as bus_utils

## {tract type: target trips per hour}
TARGET_FREQ = {
    'urban': 4, 
    'suburban': 2, 
    'rural': 1
} 


# Merge routes with tract type (notebook A3)
def merge_shapes_with_tract_type(selected_date: str) -> pd.DataFrame:
    """
    """
    # Filter and keep 5am-9pm hours
    service_by_shape = dd.read_parquet(
        f"{DATA_PATH}shapes_processed.parquet", 
        filters = [[("departure_hour", ">", 4),
                    ("departure_hour", "<", 21)]]
    ).repartition(npartitions=20)
    
    shapes_categorized = pd.read_parquet(
        f"{DATA_PATH}shapes_categorized_{selected_date}.parquet",
        columns = ["schedule_gtfs_dataset_key", "shape_id", "tract_type"]
    )
    
    shapes_categorized = shapes_categorized.assign(
        target_trips = shapes_categorized.tract_type.map(TARGET_FREQ)
    )
    
    service_by_tract_type = dd.merge(
        service_by_shape,
        shapes_categorized,
        on = ["schedule_gtfs_dataset_key", "shape_id"],
        how = "inner"
    )
    
    avg_service = (service_by_tract_type
                   .groupby(["schedule_gtfs_dataset_key", 
                             "shape_id"],
                            observed=True, group_keys=False)
                   .agg({"avg_service_minutes": "mean"})
                   .reset_index()
                  ).query(
        'avg_service_minutes.notna()'
    ).rename(
        columns = {"avg_service_minutes": "avg_runtime"}
    ).repartition(npartitions=1)

    
    # fill in missing avg_service_minutes with the mean of 
    # avg_service_minutes across hours / days / routes (but same shape)
    # if it can't be filled in, then drop
    df = dd.merge(
        service_by_tract_type,
        avg_service,
        on = ["schedule_gtfs_dataset_key", "shape_id"],
        how = "left"
    )
    
    df = df.assign(
        avg_service_minutes = df.avg_service_minutes.fillna(
            df.avg_runtime)
    ).query(
        'avg_runtime.notna()'
    ).drop(columns = ["avg_runtime"])
    
    df = df.assign(
        additional_trips = df.target_trips - df.n_trips
    )
    
    return df.compute()

# Calculate route-level additional service hours / trips / annual extrapolation
def add_additional_trips_and_annualize(df):
    df = df.assign(
        additional_trips = df.target_trips - df.n_trips,
    )
    
    # Annualize
    # Since we just have 1 weekday (wed), this will need to be scaled up by 5 still
    # we have both weekend days, so scaling it up here is enough
    df = df.assign(
        annual_service_hrs = (
            (df.avg_service_minutes * df.n_trips).divide(60)
        ) * 52,
        annual_addl_service_hrs = (
            (df.avg_service_minutes * df.additional_trips).divide(60)
        ) * 52
    )
    
    return df


def prep_ntd_metrics():
    df = (pd.read_csv(f"{bus_utils.GCS_FILE_PATH}ntd_metrics_2019.csv") 
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
    df = (pd.read_csv(f"{bus_utils.GCS_FILE_PATH}ntd_vehicles_2019.csv") 
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
    

def calculate_operator_capex(service_increase_df, ntd_joined):
    # Bring in service df
    by_operator = (service_increase_df.groupby(['calitp_itp_id', 'tract_type'])
                   [['addl_service_hrs_annual']].sum()
                  )
    
    ## https://ww2.arb.ca.gov/resources/documents/transit-fleet-cost-model
    BUS_COST = 776_941
    BUS_SERVICE_LIFE = 14 # use this assumption
    
    MEDIAN_VRH_PER_BUS = ntd_joined['vrh_per_bus'].median()
    
    by_operator['additional_buses'] = by_operator.addl_service_hrs_annual / MEDIAN_VRH_PER_BUS
    by_operator['bus_capex'] = by_operator.additional_buses * BUS_COST
    by_operator['bus_capex_annualized'] = by_operator['bus_capex'] / BUS_SERVICE_LIFE

    return by_operator

    
if __name__ == "__main__":
    
    # Merge in tract type
    df = merge_shapes_with_tract_type(selected_date)
    df = df.persist()
    
    # Calculate service hrs, trips by route and extrapolate to annual numbers
    df2 = df.map_partitions(
        add_additional_trips_and_annualize,
        meta = {**df.dtypes.to_dict(),
            "additional_trips": "int",
            "annual_service_hrs": "float",
            "annual_addl_service_hrs": "float",
           },
        align_dataframes = True
    ).compute()
    
    df2.to_parquet(f"{DATA_PATH}service_increase.parquet")
    
    
    # Prep NTD data for VRH and # buses
    #ntd_joined = add_ntd_operator_vrh_buses()
    #ntd_joined.to_parquet(f'{DATA_PATH}vehicles_ntd_joined.parquet')
    
    # Calculate operator capital expenditures
    #hours_by_operator = calculate_operator_capex(service_combined, ntd_joined)
    #hours_by_operator.to_parquet(f'{DATA_PATH}increase_by_operator.parquet')
    
