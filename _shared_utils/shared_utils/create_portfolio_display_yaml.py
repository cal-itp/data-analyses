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
    # expanded this based on manual check
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
    "SLO Peak Transit Schedule": "San Luis Obispo Regional Transit Authority",  # added this
    "TART, North Lake Tahoe Schedule": "Tahoe Truckee Area Regional Transportation, North Lake Tahoe",  # combined this
    "Tehama Schedule": "Tehama County",  # or Susanville Indian Rancheria
    "UCSC Schedule": "UCSC and City of Santa Cruz Beach Shuttle",
    "Santa Cruz Schedule": "UCSC and City of Santa Cruz Beach Shuttle",
    "VCTC GMV Schedule": "Ventura County (VCTC, Gold Coast, Cities of Camarillo, Moorpark, Ojai, Simi Valley, Thousand Oaks)",
    "VCTC Flex": "Ventura County (VCTC, Gold Coast, Cities of Camarillo, Moorpark, Ojai, Simi Valley, Thousand Oaks)",  # added this
}

# These have different feeds, schedule_gtfs_dataset_name,
# but likely would represent the same info (same n_routes)
duplicated_feed_info = [
    "Basin Transit GMV Schedule",  # dupe with Morongo Basin Schedule
    "Cerritos on Wheels Schedule"  # dupe wtih Cerritos on Wheels Website Schedule (different n_routes, but website has more routes)
    "LAX Shuttles Schedule",  # dupe with LAX Flyaway Bus Schedule
    "Lawndale Beat GMV Schedule",  # dupe with Lawndale Schedule
    "Merced GMV Schedule",  # dupe with Merced Schedule
    "Mountain Transit GMV Schedule",  # dupe with Mountain Transit Schedule
    "Roseville Transit GMV Schedule",  # dupe with Roseville Schedule
    "South San Francisco Schedule",  # dupe with Bay Area 511 South San Francisco Shuttle
    "Tahoe Transportation District GMV Schedule",  # dupe with Tahoe Transportation District Schedule
    "Victor Valley GMV Schedule",  # dupe with Victor Valley Schedule
]


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
    # Get all the operators and organization combinations across all dates
    df = operators_and_organization_df(rt_dates.all_dates)

    # Drop where we've found manual duplications
    df = df[~df.name.isin(duplicated_feed_info)].reset_index(drop=True)

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
