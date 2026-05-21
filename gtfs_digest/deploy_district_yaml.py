"""
Create the yamls for district / legislative GTFS Digest.

Since these yamls do not use sections, we can
generate them similarly using Makefile commands.

Try out typer to make CLI a little easier to use, since
this only takes 1 argument with 2 possible values.
Base it off of this tutorial:
https://typer.tiangolo.com/tutorial/options/required/
"""

from functools import cache
from pathlib import Path

import typer
from calitp_data_analysis.gcs_pandas import GCSPandas
from calitp_portfolio.models import load_site
from calitp_portfolio.mutations import generate_parts_flat
from update_vars import GTFS_DATA_DICT, SHARED_GCS, file_name


@cache
def gcs_pandas():
    return GCSPandas()


DISTRICT_SITE = Path(__file__).parent / "district_digest.yml"
LEG_DISTRICT_SITE = Path(__file__).parent / "legislative_district_digest.yml"

app = typer.Typer()


@app.command()
def overwrite_yaml(name: str = typer.Argument(default=None)):
    """
    Create yamls for either district or legislative district
    GTFS digest.
    """
    if name is None:
        raise ValueError("digest_type can be 'district', 'legislative_district'")

    elif name == "district":

        FILEPATH_URL = f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/{GTFS_DATA_DICT.gtfs_digest_rollup.crosswalk}_{file_name}.parquet"

        df = (
            gcs_pandas()
            .read_parquet(
                FILEPATH_URL,
                columns=[
                    "caltrans_district",
                ],
            )
            .sort_values(["caltrans_district"])
            .reset_index(drop=True)
            .drop_duplicates()
        )

        site = load_site(DISTRICT_SITE)
        site = generate_parts_flat(site, param_key="district", values=sorted(list(df.caltrans_district)))
        site.write_yaml(DISTRICT_SITE)

    elif name == "legislative_district":

        df = (
            gcs_pandas()
            .read_parquet(
                f"{SHARED_GCS}crosswalk_transit_operators_legislative_districts.parquet",
                columns=["legislative_district"],
            )
            .drop_duplicates()
        )

        site = load_site(LEG_DISTRICT_SITE)
        site = generate_parts_flat(site, param_key="district", values=sorted(list(df.legislative_district)))
        site.write_yaml(LEG_DISTRICT_SITE)

    return


if __name__ == "__main__":
    app()
