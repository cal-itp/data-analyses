"""
Reproduce the list of names to organizations
"""
import pandas as pd
import pyaml
from shared_utils import rt_dates

SCHED_GCS = "gs://calitp-analytics-data/data-analyses/gtfs_schedule/"
EXPORT_YAML_NAME = "portfolio_organization_name.yml"

PORTFOLIO_ORGANIZATION_NAMES = {
    # These are 1 (schedule_gtfs_dataset)_name linked to many organization_names
    # preferred organization_name or combined name is here
    "Amtrak Schedule": "Amtrak",
    "Bay Area 511 Commute.org Schedule": "Commute.org and Menlo Park Community Shuttles Schedule",
    "Bay Area 511 Dumbarton Express Schedule": "Alameda-Contra Costa Transit District",  # or "Dumbarton Bridge Regional Operations Consortium"
    "Bay Area 511 San Francisco Bay Ferry Schedule": "San Francisco Bay Ferry and Oakland Alameda Water Shuttle Schedule",
    "Bay Area 511 SolTrans Schedule": "Solano Transportation Authority",  # or Solano County Transit?
    "Bay Area 511 Sonoma County Transit Schedule": "Sonoma County Transit Schedule",
    "Flixbus Schedule": "FlixBus and Greyhound",
    "Foothill Schedule": "Foothill Transit",
    "Humboldt Flex": "Humboldt Transit Authority",  # should this be included?
    "Humboldt Schedule": "Humboldt Transit Authority",
    "Redding Schedule": "Redding Area Bus Authority",
    "Sacramento Schedule": "Sacramento Regional Transit District",
    "San Diego Schedule": "San Diego Metropolitan Transit System, Airport, Flagship Cruises",  # combined this
    "TART, North Lake Tahoe Schedule": "Tahoe Truckee Area Regional Transportation, North Lake Tahoe",  # combined this
    "Tehama Schedule": "Tehama County",  # or Susanville Indian Rancheria
    "UCSC Schedule": "UCSC and City of Santa Cruz Beach Shuttle",
    "Santa Cruz Schedule": "UCSC and City of Santa Cruz Beach Shuttle",
    "VCTC GMV Schedule": "Ventura County (VCTC, Gold Coast, Cities of Camarillo, Moorpark, Ojai, Simi Valley, Thousand Oaks)",
}


def operators_and_organization_df(date_list: list) -> pd.DataFrame:
    """
    Concatenate the crosswalk (from gtfs_funnel)
    that connects gtfs_dataset_key to organization
    for all the dates we have and dedupe.
    Keep unique combinations of operator names (schedule_gtfs_dataset_name)
    and organization_name (Airtable).
    """
    df = (
        pd.concat(
            [
                pd.read_parquet(
                    f"{SCHED_GCS}crosswalk/gtfs_key_organization_{date}.parquet", columns=["name", "organization_name"]
                )
                for date in date_list
            ],
            axis=0,
            ignore_index=True,
        )
        .drop_duplicates()
        .sort_values(["organization_name", "name"])
        .reset_index(drop=True)
    )

    return df


def find_operators_with_multiple_organization_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Count the number of organization_names per operator,
    then filter to those with multiple names.
    Manually figure out what are the names we want.
    """
    operators_with_multiple_orgs = (
        df.groupby(["name"]).agg({"organization_name": "nunique"}).reset_index().query("organization_name > 1")
    ).name.unique()

    df2 = df[df.name.isin(operators_with_multiple_orgs)].reset_index(drop=True)

    return df2


def operators_keep_existing_organization_names(
    df: pd.DataFrame, preferred_organization_name_dict: dict = PORTFOLIO_ORGANIZATION_NAMES
):
    schedule_feeds_to_fix = list(preferred_organization_name_dict.keys())
    schedule_feeds_ok = df[~df.name.isin(schedule_feeds_to_fix)][["name", "organization_name"]].drop_duplicates()

    existing_orgs_dict = {
        gtfs_dataset_name: organization_name
        for gtfs_dataset_name, organization_name in zip(schedule_feeds_ok.name, schedule_feeds_ok.organization_name)
    }

    combined_name_dict = {**preferred_organization_name_dict, **existing_orgs_dict}

    return combined_name_dict


if __name__ == "__main__":
    df = operators_and_organization_df(rt_dates.all_dates)

    operators_to_fix = find_operators_with_multiple_organization_names(df)

    # Look at these in a notebook and decide the names
    # Decisions canonized in PORTFOLIO_ORGANIZATION_NAMES dict

    # Now combine it with the rest of the organization names
    # Save entire thing as a yaml
    DISPLAY_DICT = operators_keep_existing_organization_names(df, PORTFOLIO_ORGANIZATION_NAMES)

    output = pyaml.dump(DISPLAY_DICT)

    with open(EXPORT_YAML_NAME, "w") as f:
        f.write(output)

    print(f"create {EXPORT_YAML_NAME}")
