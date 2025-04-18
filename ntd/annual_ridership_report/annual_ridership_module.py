# all functions used for annual ridership report
import os
import shutil
import sys
import annual_ridership_module
sys.path.append("../")  # up one level

import pandas as pd
from siuba import _, collect, count, filter, select, show_query
from calitp_data_analysis.tables import tbls
from update_vars import GCS_FILE_PATH, NTD_MODES, NTD_TOS, MONTH, YEAR
from segment_speed_utils.project_vars import PUBLIC_GCS
import gcsfs
fs = gcsfs.GCSFileSystem()
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

def sum_by_group(df: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    """
    since data is now long to begin with, this replaces old sum_by_group, make_long and assemble_long_df functions.

    """
    grouped_df = (
        df.groupby(group_cols + ["year"])
        .agg(
            {
                "upt": "sum",
                # "vrm":"sum",
                # "vrh":"sum",
                "previous_y_upt": "sum",
                "change_1yr": "sum",
            }
        )
        .reset_index()
    )

    # get %change back
    grouped_df = get_percent_change(grouped_df)

    # decimal to whole number
    grouped_df["pct_change_1yr"] = grouped_df["pct_change_1yr"] * 100

    return grouped_df

def produce_annual_ntd_ridership_data_by_rtpa(min_year:str):
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
              _.year >= min_year,
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
    
    print("map mode and tos desc.")
    ntd_data_by_rtpa = ntd_data_by_rtpa.assign(
        mode_full = ntd_data_by_rtpa["mode"].map(NTD_MODES),
        service_full = ntd_data_by_rtpa["service"].map(NTD_TOS)
    )
    
    return ntd_data_by_rtpa

def save_rtpa_outputs(
    df: pd.DataFrame, 
    year: int, 
    month: str, 
    upload_to_public: bool = False
):
    
    """
    Export an excel for each RTPA, adds a READ ME tab, then writes into a folder.
    Zip that folder.
    Upload zipped file to GCS.
    """
    #col_dict = {
        #"agency_name":,
        #"agency_status":,
        #"city":,
        #"state":,
        #"ntd_id":,
        #"primary_uza_name":,
        #"reporter_type":,
        #"mode":,
        #"service":,
        #"year":,
        #"upt":,
        #"RTPA":,
        #"previous_y_upt":,
        #"change_1yr":,
        #"pct_change_1yr":,
        #"mode_full":,
        #"service_full":,
    #}
    print("creating individual RTPA excel files")

    for i in df["RTPA"].unique():

        print(f"creating excel file for: {i}")

        # Filename should be snakecase
        rtpa_snakecase = i.replace(" ", "_").replace("/","_").lower() #this fixes 'Lake County/City Area Planning Council`

        # insertng readme cover sheet,
        cover_sheet = pd.read_excel(
            "./annual_report_cover_sheet_template.xlsx", index_col="**NTD Annual Ridership by RTPA**"
        )
        cover_sheet.to_excel(
            f"./{year}_{month}/{rtpa_snakecase}.xlsx", sheet_name="README"
        )
        
        #filter data by single RTPA
        rtpa_data = (
            df[df["RTPA"] == i].sort_values("ntd_id")
            .drop(columns=[
                "_merge", 
                "xwalk_agency_name",
                "xwalk_reporter_type",
                "xwalk_agency_status",
                "xwalk_city",
                "xwalk_state",
            ])
            # cleaning column names
            .rename(columns=lambda x: x.replace("_", " ").title().strip())
            # rename columns
            #.rename(columns=col_dict)
        )
        # column lists for aggregations
        agency_cols = ["ntd_id", "agency_name", "RTPA"]
        mode_cols = ["mode", "RTPA"]
        tos_cols = ["service", "RTPA"]
        reporter_type = ["reporter_type", "RTPA"]

        # Creating aggregations
        by_agency_long = annual_ridership_module.sum_by_group((df[df["RTPA"] == i]), agency_cols)
        by_mode_long = annual_ridership_module.sum_by_group((df[df["RTPA"] == i]), mode_cols)
        by_tos_long = annual_ridership_module.sum_by_group((df[df["RTPA"] == i]), tos_cols)
        by_reporter_type_long = annual_ridership_module.sum_by_group((df[df["RTPA"] == i]), reporter_type)

        # writing pages to excel file
        with pd.ExcelWriter(
            f"./{year}_{month}/{rtpa_snakecase}.xlsx", mode="a"
        ) as writer:
            rtpa_data.to_excel(
                writer, sheet_name="RTPA Ridership Data", index=False
            )
            by_agency_long.to_excel(
                writer, sheet_name="Aggregated by Agency", index=False
            )
            by_mode_long.to_excel(
                writer, sheet_name="Aggregated by Mode", index=False
            )
            by_tos_long.to_excel(
                writer, sheet_name="Aggregated by TOS", index=False
            )
            by_reporter_type_long.to_excel(
                writer, sheet_name="Aggregate by Reporter Type", index=False
            )

    print("zipping all excel files")

    shutil.make_archive(f"./{year}_{month}_annual_report_data", "zip", f"{year}_{month}")

    print("Zipped folder")

    print("Upload to private GCS")
    fs.upload(f"./{year}_{month}_annual_report_data.zip", f"{GCS_FILE_PATH}{year}_{month}_annual_report_data.zip")

    if upload_to_public:
        fs.upload(
            f"./{year}_{month}_annual_report_data.zip",
            f"{PUBLIC_GCS}ntd_annual_ridership/{year}_{month}_annual_report_data.zip",
        )

        print("Uploaded to public GCS")

    return


def remove_local_outputs(
    year: int, 
    month: str
):
    shutil.rmtree(f"{year}_{month}/")
    os.remove(f"{year}_{month}_annual_report_data.zip")

    
if __name__ == "__main__":
    min_year="2018"
    
    df = produce_annual_ntd_ridership_data_by_rtpa(min_year)
    print("saving parqut to private GCS")
    
    df.to_parquet(f"{GCS_FILE_PATH}annual_ridership_report_data.parquet")

    os.makedirs(f"./{YEAR}_{MONTH}/")

    save_rtpa_outputs(df, YEAR, MONTH, upload_to_public = True)
    remove_local_outputs(YEAR, MONTH)