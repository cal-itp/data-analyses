from functools import cache
import geopandas as gpd
import pandas as pd
import google.auth

from calitp_data_analysis import geography_utils, utils
from calitp_data_analysis.gcs_pandas import GCSPandas
from update_vars import GTFS_DATA_DICT, file_name

# Initialize credentials
credentials, project = google.auth.default()


@cache
def gcs_pandas():
    return GCSPandas()


def prep_schedule_rt_route_direction_summary(file_name: str) -> pd.DataFrame:
    df = gcs_pandas().read_parquet(
        f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}raw/"
        f"{GTFS_DATA_DICT.gtfs_digest_rollup.schedule_rt_route_direction}_{file_name}.parquet"
    )

    # Select relevant columns
    df2 = df[
        [
            "month_first_day", "analysis_name", "route_name", "direction_id",
            "frequency_all_day", "frequency_offpeak", "frequency_peak",
            "daily_service_hours", "daily_trips_peak", "daily_trips_offpeak",
            "daily_trips_all_day", "day_type", "route_type", "route_typology",
        ]
    ].drop_duplicates().reset_index()

    # Clean columns
    df2.route_typology = df2.route_typology.str.title()
    df2.columns = df2.columns.str.replace("_", " ").str.title()
    df2["Month First Day"] = pd.to_datetime(df2["Month First Day"]).dt.strftime("%m/%Y")
    df2 = df2.rename(columns={"Direction Id": "Direction", "Month First Day": "Date", "Route Name": "Route"})

    # Add calculated columns
    df2["Daily Service Minutes"] = df2["Daily Service Hours"] * 60
    df2["Average Scheduled Minutes"] = df2["Daily Service Minutes"] / df2["Daily Trips All Day"]
    df2["Headway All Day"] = 60 / df2["Frequency All Day"]
    df2["Headway Peak"] = 60 / df2["Frequency Peak"]
    df2["Headway Offpeak"] = 60 / df2["Frequency Offpeak"]

    # Save processed file
    gcs_pandas().data_frame_to_parquet(df2, f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/{GTFS_DATA_DICT.gtfs_digest_rollup.schedule_rt_route_direction}_{file_name}.parquet")
    return df2


def prep_operator_summary(file_name: str) -> pd.DataFrame:
    df = gcs_pandas().read_parquet(
        f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}raw/"
        f"{GTFS_DATA_DICT.gtfs_digest_rollup.operator_summary}_{file_name}.parquet"
    )

    # Select relevant columns
    df2 = df[
        [
            "month_first_day", "analysis_name", "caltrans_district", "vp_name", "tu_name",
            "n_trips", "day_type", "daily_trips", "ttl_service_hours", "n_routes", "n_days",
            "n_shapes", "n_stops", "vp_messages_per_minute", "n_vp_trips", "daily_vp_trips",
            "pct_vp_trips", "tu_messages_per_minute",
            "n_tu_trips", "daily_tu_trips", "pct_tu_trips", 
        ]
    ]
    
    # Multiply percetnage columns by 100. Clip any values above 100.
    df2.pct_tu_trips = df2.pct_tu_trips * 100
    df2.pct_vp_trips = df2.pct_vp_trips * 100
    df2.pct_tu_trips = df2.pct_tu_trips.clip(upper=100.0)
    df2.pct_vp_trips = df2.pct_vp_trips.clip(upper=100.0)
    
    # Clean columns
    df2.columns = df2.columns.str.replace("_", " ").str.title()
    df2 = df2.rename(columns={"Month First Day": "Date"})
    df2.columns = df2.columns.str.replace("Vp", "VP").str.replace("Tu", "TU")

    # Save processed file
    gcs_pandas().data_frame_to_parquet(
        df2,
        f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/"
        f"{GTFS_DATA_DICT.gtfs_digest_rollup.operator_summary}_{file_name}.parquet"
    )

    return df2


def prep_fct_monthly_routes(file_name: str) -> pd.DataFrame:
    gdf = gpd.read_parquet(
        f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}raw/"
        f"{GTFS_DATA_DICT.gtfs_digest_rollup.route_map}_{file_name}.parquet",
        storage_options={"token": credentials.token},
    )

    # Keep most recent route geography
    gdf2 = (
        gdf.sort_values(by=["month_first_day", "analysis_name", "route_name"], ascending=[False, True, True])
        .drop_duplicates(subset=["analysis_name", "route_name"])
    )

    # Drop unnecessary columns
    gdf2 = gdf2.drop(columns=["shape_id", "shape_array_key", "n_trips", "direction_id"])

    # Convert to miles
    gdf2["route_length_miles"] = gdf2.geometry.to_crs(geography_utils.CA_NAD83Albers_ft).length / 5_280

    # Clean column names
    gdf2.columns = gdf2.columns.str.replace("_", " ").str.title()

    # Export to GCS
    utils.geoparquet_gcs_export(
        gdf=gdf2,
        gcs_file_path=f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/",
        file_name=f"{GTFS_DATA_DICT.gtfs_digest_rollup.route_map}_{file_name}",
    )

    return gdf2


def prep_fct_operator_hourly_summary(file_name: str) -> pd.DataFrame:

    df = gcs_pandas().read_parquet(f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}raw/{GTFS_DATA_DICT.gtfs_digest_rollup.hourly_day_type_summary}_{file_name}.parquet")
    
    # Prepare data
    df2 = (
        df.groupby(["analysis_name", "month_first_day", "day_type", "departure_hour"])
        .agg({"n_trips": "sum"})
        .reset_index()
    )

    df2.columns = df2.columns.str.replace("_", " ").str.title()

    df2 = df2.rename(columns={"Month First Day": "Date"})

    df2["Date"] = df2["Date"].dt.strftime("%m-%Y")

    gcs_pandas().data_frame_to_parquet(df2, f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/{GTFS_DATA_DICT.gtfs_digest_rollup.hourly_day_type_summary}_{file_name}.parquet")
    
    return df2

if __name__ == "__main__":
    schedule_rt_route_direction_summary_df = prep_schedule_rt_route_direction_summary(file_name)
    monthly_operator_summary_clean = prep_operator_summary(file_name)
    monthly_routes_gdf = prep_fct_monthly_routes(file_name)
    clean_fct_operator_hourly_summary_df = prep_fct_operator_hourly_summary(file_name)
    print("done running")