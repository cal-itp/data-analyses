"""
functions for the new transit metrics portfolio
"""
import pandas as pd
import new_transit_metrics
from calitp_data_analysis.tables import tbls
from siuba import _, collect, count, filter, show_query
import sys
sys.path.append("../")
from update_vars import GCS_FILE_PATH


def make_new_transit_metric_data()-> pd.DataFrame:
    """
    Reads in annual NTD data from the warehoure. Merges with ntd_id-to-rtpa crosswalk
    """
    keep_cols_metrics = [
    "ntd_id",
    "agency",
    "organization_type",
    "reporter_type",
    "city",
    "uza_name",
    "agency_voms",
    "mode",
    "type_of_service",
    "report_year",
    "total_operating_expenses",
    "unlinked_passenger_trips",
    "vehicle_revenue_hours",
    "vehicle_revenue_miles",
    ]
    
    print("read in ntd data from warehouse")
    fct_metrics = (
        tbls.mart_ntd_annual_reporting.fct_metrics()
        >> filter(
            _.state == "CA",
            # _.report_year == 2023
        )
        >> collect()
    )[keep_cols_metrics]
    
    print("aggregate data by ntd id, agency name, report year, etc")
    agg_metrics = (
        fct_metrics.groupby(
            [
                "ntd_id",
                "agency",
                "report_year",
                #"mode",
                #"type_of_service",
                "organization_type",
                "reporter_type",
                "uza_name",
            ]
        )
        .agg(
            total_opex=("total_operating_expenses", "sum"),
            total_upt=("unlinked_passenger_trips", "sum"),
            total_vrh=("vehicle_revenue_hours", "sum"),
            total_vrm=("vehicle_revenue_miles", "sum"),
        )
        .reset_index()
    )
    print("identify and remove outliers")
    z_score = agg_metrics[["total_opex", "total_upt", "total_vrh",
                           "total_vrm"]].apply(
        zscore
    )
    threshold = 3
    agg_metrics_no_outliers = agg_metrics[(z_score.abs() < threshold).all(axis=1)]
    print(f"""
    initial data: {agg_metrics.shape}
    data without outliers: {agg_metrics_no_outliers.shape}
    """)
    
    calc_dict = {
    "opex_per_vrh": ("total_opex", "total_vrh"),
    "opex_per_vrm": ("total_opex", "total_vrm"),
    "upt_per_vrh": ("total_upt", "total_vrh"),
    "upt_per_vrm": ("total_upt", "total_vrm"),
    "opex_per_upt": ("total_opex", "total_upt"),
    }
    print("calculate new performance metrics")
    for new_col, (num, dem) in calc_dict.items():
        agg_metrics_no_outliers[new_col] = (
            agg_metrics_no_outliers[num] / agg_metrics_no_outliers[dem]
        )
    
    print("read in RTPA data")
    
    return agg_metrics_no_outliers
    
def agg_by_group():
    

if __name__ = "__main__":
    df = make_new_transit_metric_data()
    df.to_parquet(f"{GCS_FILE_PATH}explore_transit_performance_metrics.parquet")
