"""
Deploy portfolio yaml based on ntd id to rtpa crosswalk, all reporter types.

Since the names of RTPAs change so much depending on the crosswalk
we use, let's just generate the yaml.

This Yaml structure is structured by RTPA names, instead of CT districts.
RTPA names will become the navigation panel on the portfolio site
"""

import sys

sys.path.append("../")  # up one level
sys.path.append("../monthly_ridership_report")
from pathlib import Path  # noqa: E402

from calitp_data_analysis.gcs_pandas import GCSPandas  # noqa: E402
from calitp_portfolio.models import load_site  # noqa: E402
from calitp_portfolio.mutations import generate_parts_flat  # noqa: E402
from update_vars import GCS_FILE_PATH  # noqa: E402

PORTFOLIO_SITE_YAML = Path(__file__).parent / "new_transit_metrics.yml"

if __name__ == "__main__":

    df = (
        GCSPandas()
        .read_parquet(
            # f"{GCS_FILE_PATH}ntd_id_rtpa_crosswalk_all_reporter_types.parquet",
            f"{GCS_FILE_PATH}raw_transit_performance_metrics_data.parquet",
            columns=["RTPA"],
        )
        .drop_duplicates()
        .sort_values("RTPA")
        .reset_index(drop=True)
    )

    site = load_site(PORTFOLIO_SITE_YAML)
    site = generate_parts_flat(site, param_key="rtpa", values=list(df.RTPA))
    site.write_yaml(PORTFOLIO_SITE_YAML)
