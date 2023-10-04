import pandas as pd
from calitp_data_analysis.sql import to_snakecase

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
    
def load_tu_or_vp(excel_file: str, column_to_filter: str) -> pd.DataFrame:
    df = to_snakecase(pd.read_excel(excel_file))
    df = (df[df[column_to_filter] < 41].sort_values([column_to_filter])).reset_index(
        drop=True
    )
    return df

def load_api_511(excel_file: str) -> pd.DataFrame:
    # Read in 511
    df = to_snakecase(pd.read_excel(excel_file))
    df = df.rename(columns={df.columns[0]: "new"})

    # Only keep rows that have the string name or monitored
    df = df[df["new"].str.contains(("Name|Monitored"))].reset_index(drop=True)

    # Get rid of random characters
    df.new = (
        df.new.str.replace(">", "")
        .str.replace("ShortName", "")
        .str.replace("Name", "")
        .str.replace("Monitored", "")
        .str.replace("<", "")
        .str.replace("/", "")
    )

    # display(df)
    print("These are Bay Area feeds that keep track of RT")
    display(df[(df.new == "true").shift(1).fillna(False)])
    
def incomplete(tu_excel_file: str, vp_excel_file: str, airtable: pd.DataFrame) -> pd.DataFrame:
    tu_df = load_tu_or_vp(tu_excel_file, "%_of_trips_with_tu_messages")
    vp_df = load_tu_or_vp(vp_excel_file, "%_of_trips_with_vp_messages")

    incomplete = pd.merge(tu_df, vp_df, on="name", how="outer")
    incomplete = incomplete.sort_values(["name"]).reset_index(drop=True)

    incomplete = incomplete.fillna("OK")

    # incomplete.name = incomplete.name.str.replace('Schedule','')
    incomplete2 = (
        pd.merge(
            incomplete, airtable, left_on="name", right_on="gtfs_datasets", how="left"
        )
        .sort_values("name")
        .fillna("NA")
    )
    display(incomplete2)