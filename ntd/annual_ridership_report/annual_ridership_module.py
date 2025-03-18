# all functions used for annual ridership report

import pandas as pd
from siuba import _, collect, count, filter, select, show_query
from calitp_data_analysis.tables import tbls
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/ntd/"

def get_percent_change(
    df: pd.DataFrame, 
) -> pd.DataFrame:
    """
    Calculates % change of UPT from previous year 
    """
    df["pct_change_1yr"] = (
        (df["upt"] - df["previous_y_upt"])
        .divide(df["upt"])
        .round(4)
    )
    
    return df

def add_change_columns(
    df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates (value) change of UPT from previous year.
    Sorts the df by ntd id, year, mode, service. then shifts the upt value down one row (to the next year,mode,service UPT row). then adds  new columns: 
        1. previous year/month UPT
        2. change_1yr
    """

    sort_cols2 =  ["ntd_id",
                   "year",
                   "mode", 
                   "service",
                  ]
    
    group_cols2 = ["ntd_id",
                   "mode", 
                   "service"
                  ]
    
    df = df.assign(
        previous_y_upt = (df.sort_values(sort_cols2)
                        .groupby(group_cols2)["upt"] 
                        .apply(lambda x: x.shift(1))
                       )
    )

    df["change_1yr"] = (df["upt"] - df["previous_y_upt"])
    
    df = get_percent_change(df)
    
    return df

def produce_annual_ntd_ridership_data_by_rtpa():
    """
    Function that ingest ridership data from `dim_annual_service_agencies`, filters for CA agencies.
    Merges in ntd_id_to_RTPA_crosswalk. Aggregates by agency, mode and TOS. calculates change in UPT.
    """
    from annual_ridership_module import add_change_columns
    
    print("ingest annual ridership data from warehouse")
    
    ntd_service = (
    tbls.mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt()
    >> filter(_.state.str.contains("CA") | 
              _.state.str.contains("NV"), # to get lake Tahoe Transportation back
              _.year >= "2018",
              _.city != None,
              _.primary_uza_name.str.contains(", CA") | 
              _.primary_uza_name.str.contains("CA-NV") |
              _.primary_uza_name.str.contains("California Non-UZA") | 
              _.primary_uza_name.str.contains("El Paso, TX--NM") # something about Paso 
             )
    >> select(
        'agency_name',
        'agency_status',
        'city',
        'legacy_ntd_id',
        'mode',
        'ntd_id',
        'reporter_type',
        'reporting_module',
        'service',
        'state',
        'uace_code',
        'primary_uza_name',
        'uza_population',
        'year',
        'upt',
    )
    >> collect())
    
    ntd_service = ntd_service.groupby(
        [
            "agency_name",
            'agency_status',
            "city",
            "state",
            "ntd_id",
            'primary_uza_name',
            "reporter_type",
            "mode",
            "service",
            "year"
        ]
    ).agg({
        "upt":"sum"
    }).sort_values(by="ntd_id").reset_index()

    
    print("read in new `ntd_id_to_rtpa_all_reporter_types` crosswalk") 
    
    ntd_to_rtpa_crosswalk = pd.read_parquet(f"{GCS_FILE_PATH}ntd_id_rtpa_crosswalk_all_reporter_types.parquet")
        
    print("merge ntd data to crosswalk")
    
    ntd_data_by_rtpa = ntd_service.merge(
    ntd_to_rtpa_crosswalk,
    how="left",
    on=[
        "ntd_id",
        #"agency", "reporter_type", "city" # sometime agency name, reporter type and city name change or are inconsistent, causing possible fanout
    ],
    indicator=True
    ).rename(
    columns={
        "actual_vehicles_passenger_car_revenue_hours":"vrh",
        "actual_vehicles_passenger_car_revenue_miles":"vrm",
        "unlinked_passenger_trips_upt":"upt",
        'agency_name_x':"agency_name", 
        'agency_status_x':"agency_status", 
        'city_x':"city", 
        'state_x':"state",
        'reporter_type_x':"reporter_type",
        "agency_name_y":"xwalk_agency_name",
        'reporter_type_y':"xwalk_reporter_type",
        'agency_status_y':"xwalk_agency_status",
        'city_y':"xwalk_city",
        'state_y':"xwalk_state",
    }
    )
    
    print(ntd_data_by_rtpa._merge.value_counts())
        
    if len(ntd_data_by_rtpa[ntd_data_by_rtpa._merge=="left_only"]) > 0:
        raise ValueError("There are unmerged rows to crosswalk")
    
    print("add `change_column` to data")
    ntd_data_by_rtpa = add_change_columns(ntd_data_by_rtpa)
    
    return ntd_data_by_rtpa


if __name__ == "__main__":
    
    df = produce_annual_ntd_ridership_data_by_rtpa()
    print("saving to GCS")
    df.to_parquet(f"{GCS_FILE_PATH}annual_ridership_report_data.parquet")
