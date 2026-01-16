import _operators_prep
import geopandas as gpd
import pandas as pd
import altair as alt
import _report_utils

from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS, SCHED_GCS
from shared_utils import catalog_utils, rt_dates, rt_utils

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

"""
Data Cleaning
"""
def load_data()->pd.DataFrame:
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"
    
    cols_to_keep = [
    "service_date",
    "organization_name",
    "caltrans_district",
    "sched_rt_category",
    "route_long_name",
    "route_combined_name",
    "route_primary_direction",
    "time_period",
    "n_scheduled_trips",
    "total_vp",
    "total_rt_service_minutes",
    "vp_in_shape"]
    
    df = pd.read_parquet(
       schd_vp_url, 
       filters = [[
        ("time_period", "==", "all_day"), 
        ("sched_rt_category", "==", "schedule_and_vp")]],
        columns = cols_to_keep
        )

    return df

def aggregate_by_agency(df:pd.DataFrame)->pd.DataFrame:
    # Aggregate by district, organization, and service date 
    agg1 = (
    df.groupby(["caltrans_district", "organization_name", "service_date"])
    .agg({"total_vp": "sum", "vp_in_shape": "sum", "total_rt_service_minutes": "sum"})
    .reset_index()
    )
    
    # Find metrics
    agg1["spatial_accuracy"] = ((agg1.vp_in_shape / agg1.total_vp) * 100).round(2)
    agg1["vp_per_min"] = (agg1.total_vp / agg1.total_rt_service_minutes).round(2)
    
    # Sort the data 
    agg1 = agg1.sort_values(
    by=["caltrans_district", "organization_name", "service_date"]).reset_index(drop=True)
    
    return agg1

def harmonize_org_names(df:pd.DataFrame)->pd.DataFrame:
    
    # Find relevant operators
    to_keep = [
    "organization_name",
    "caltrans_district",]
    
    sched_only_and_vp = _operators_prep.operators_schd_vp_rt()[to_keep]
    
    # Merge for only rows found in both
    m1 = pd.merge(df, sched_only_and_vp, on=to_keep, how="inner")
    
    return m1

def heatmap(
    df: pd.DataFrame,
    column: str,
    domain_color: list,
    range_color: list,
    max_y_axis: int,
) -> alt.Chart:
    # Grab District
    district = df.District.iloc[0]

    # Create color scale
    color_scale = alt.Scale(domain=domain_color, range=range_color)

    chart = (
        alt.Chart(df)
        .mark_rect()
        .encode(
            x=alt.X(
                "yearmonthdate(Date):O",
                axis=alt.Axis(labelAngle=-45, format="%b %Y"),
            ),
            y=alt.Y(
                "Organization:N",
                axis=alt.Axis(labelFontSize=9),
            ),
            color=alt.Color(
                f"{column}:Q",
                title=_report_utils.labeling(column),
                scale=color_scale,
            ),
            tooltip=["Organization", "District", column, "Date"],
        )
    )

    chart = chart.properties(width=600, height=300, title=f"District {district}")
    return chart