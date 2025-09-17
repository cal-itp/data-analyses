import geopandas as gpd
import pandas as pd
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS
SHARED_GCS = GTFS_DATA_DICT.gcs_paths.SHARED_GCS

import google.auth
credentials, project = google.auth.default()
import gcsfs
fs = gcsfs.GCSFileSystem()

def readable_district_name(word: str) -> str:
    if "SD" in word:
        return word.replace("SD", "Senate District")
    elif "AD" in word:
        return word.replace("AD", "Assembly District")
    
def load_district_stats(district:str) -> pd.DataFrame:
    OPERATOR_FILE = GTFS_DATA_DICT.digest_tables.operator_profiles

    legislative_crosswalk = pd.read_parquet(
        f"{SHARED_GCS}crosswalk_transit_operators_legislative_districts.parquet",
        filters=[[("legislative_district", "==", district)]],
    )

    operator_df = pd.read_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_FILE}.parquet",
    )
    m1 = operator_df.merge(legislative_crosswalk, on="name", how="inner")
    # Keep only the most recent rows
    m1 = m1.sort_values(
        ["service_date", "name"], ascending=[False, True]
    ).drop_duplicates(subset=["portfolio_organization_name"])

    return m1

def load_gtfs_data(df: pd.DataFrame) -> pd.DataFrame:
    # Load the relevant operators in the district
    operators_in_district = df.schedule_gtfs_dataset_key.unique()

    OPERATOR_ROUTE = GTFS_DATA_DICT.digest_tables.operator_routes_map

    operator_route_gdf = gpd.read_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_ROUTE}.parquet",
        storage_options={"token": credentials.token},
    )
    operator_route_gdf = operator_route_gdf.loc[
        operator_route_gdf.schedule_gtfs_dataset_key.isin(operators_in_district)
    ][
        [
            "portfolio_organization_name",
            "service_date",
            "recent_combined_name",
            "geometry",
        ]
    ]

    # Only keep the most recent transit route geographies
    operator_route_gdf2 = operator_route_gdf.drop_duplicates(
        subset=["portfolio_organization_name", "recent_combined_name"]
    )
    operator_route_gdf2 = operator_route_gdf2.dissolve(
        by=["portfolio_organization_name"]
    ).reset_index()[["portfolio_organization_name", "geometry"]]

    operator_route_gdf2 = operator_route_gdf2.rename(
        columns={"portfolio_organization_name": "Transit Operator"}
    )
    return operator_route_gdf2

def create_gtfs_table(df: pd.DataFrame) -> pd.DataFrame:
    gtfs_service_cols = [c for c in df.columns if "operator_" in c]

    gtfs_table_df = df[["organization_name"] + gtfs_service_cols]

    gtfs_table_df = gtfs_table_df.rename(
        columns={
            "organization_name": "Organization",
            "operator_n_routes": "# Routes",
            "operator_n_trips": "# Trips",
            "operator_n_shapes": "# Shapes",
            "operator_n_stops": "# Stops",
            "operator_n_arrivals": "# Arrivals",
            "operator_route_length_miles": "Operator Service Miles",
            "operator_arrivals_per_stop": "Avg Arrivals per Stop",
        }
    )

    gtfs_table_df = gtfs_table_df.reset_index(drop=True)
    return gtfs_table_df