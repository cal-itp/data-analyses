import pandas as pd
from calitp_data_analysis.sql import to_snakecase

from datetime import datetime, timedelta

def load_download_parse(excel_file:str)-> pd.DataFrame:
    to_keep = [
    "dim_provider_gtfs_data___gtfs_dataset_key_→_organization_name",
    "dim_provider_gtfs_data___gtfs_dataset_key_→_service_name",
    "dim_provider_gtfs_data___gtfs_dataset_key_→_schedule_gtfs_dataset_name",
    "ts",
    "download_exception",
    "pct_files_successfully_parsed",
    "dim_county_geography_→_name"]
    
    df = to_snakecase(
    pd.read_excel(
        "./gtfs_schedule_download_and_parse_errors_2024-03-01T16_55_55.917295Z.xlsx"
    ))[to_keep]
    
    new_cols = {
    "dim_provider_gtfs_data___gtfs_dataset_key_→_organization_name": "organization_name",
    "dim_provider_gtfs_data___gtfs_dataset_key_→_service_name": "service_name",
    "dim_provider_gtfs_data___gtfs_dataset_key_→_schedule_gtfs_dataset_name": "dataset_name",
    "dim_county_geography_→_name": "geography"}
    
    df = df.rename(columns=new_cols)
    
    # Get the current date
    current_date = datetime.now()

    # Calculate the date 7 days ago
    seven_days_ago = current_date - timedelta(days=7)
    
    
    # Filter rows to include only the last 7 days
    df2 = df[df.ts >= seven_days_ago]
    
    df2 = df2.drop_duplicates(
    subset=["download_exception", "organization_name", "service_name", "dataset_name"]).reset_index(drop=True)
    
    return df2

def load_catastrophic_errors(excel_file: str)->pd.DataFrame:
    df = pd.read_excel(excel_file)
    df = to_snakecase(df)

    # Don't look at LACMTA stuff
    no_lacmta = df[~df["dim_gtfs_datasets_→_uri"].str.contains("LACMTA")]

    no_lacmta2 = no_lacmta[
        [
            "dim_provider_gtfs_data_→_service_name",
            "dim_county_geography_→_caltrans_district",
            "dim_gtfs_datasets_→_uri",
            "date",
        ]
    ].sort_values(
        ["dim_provider_gtfs_data_→_service_name", "date"], ascending=[False, False]
    )

    no_lacmta2 = no_lacmta2.rename(
        columns={
            "dim_provider_gtfs_data_→_service_name": "service_name",
            "dim_county_geography_→_caltrans_district": "district",
            "dim_gtfs_datasets_→_uri": "uri",
        }
    )
    return no_lacmta2

def load_airtable(csv_file: str)->pd.DataFrame:
    df = to_snakecase(pd.read_csv(csv_file))

    df = df.fillna("None")

    df = (
        df[
            [
                "gtfs_datasets",
                "services",
                "issue_type",
                "description",
            ]
        ]
        .sort_values(["gtfs_datasets", "services"])
        .reset_index(drop=True)
    )

    df["airtable_ticket"] = "Yes"

    return df

def summarize_cat(
    catastrophic_data: pd.DataFrame, airtable_data: pd.DataFrame
) -> pd.DataFrame:

    cat_summary = (
        catastrophic_data.groupby(["service_name", "uri"])
        .agg({"date": "count"})
        .reset_index()
        .rename(columns={"date": "# of days with expired feed"})
    )

    display(cat_summary)

    m1 = pd.merge(
        cat_summary,
        airtable_data,
        left_on=["service_name"],
        right_on=["services"],
        how="left",
    )

    display(m1)
    
def load_tu_or_vp(excel_file: str, column_to_filter: str, percent_to_filter: int) -> pd.DataFrame:
    df = to_snakecase(pd.read_excel(excel_file))
    df = (df[df[column_to_filter] < percent_to_filter].sort_values([column_to_filter])).reset_index(
        drop=True
    )
    return df
    
def incomplete(tu_excel_file: str, vp_excel_file: str, airtable: pd.DataFrame, percent_to_filter:str) -> pd.DataFrame:
    tu_df = load_tu_or_vp(tu_excel_file, "%_of_trips_with_tu_messages", percent_to_filter)
    vp_df = load_tu_or_vp(vp_excel_file, "%_of_trips_with_vp_messages", percent_to_filter)

    incomplete = pd.merge(tu_df, vp_df, on="name", how="outer")
    incomplete = incomplete.sort_values(["name"]).reset_index(drop=True)

    incomplete = incomplete.fillna("OK")
    
    incomplete2 = (
        pd.merge(
            incomplete, airtable, left_on="name", right_on="gtfs_datasets", how="left", indicator = True
        )
        .sort_values("name")
        .fillna("NA")
    )
    
    incomplete2._merge = incomplete2._merge.str.replace('right_only','problems_w_vp_only').str.replace('left_only','problems_w_tu_only')
    return incomplete2
