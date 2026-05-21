"""
Creates site .yml with chapters for each unique RTPA in the report data,
then places the new .yml in the portfolio/sites directory

Yaml structure is not nested by district, it is just all RTPAs
in the navigation panel.
"""

import sys

sys.path.append("../monthly_ridership_report")  # for update_vars
from pathlib import Path  # noqa: E402

from calitp_data_analysis.gcs_pandas import GCSPandas  # noqa: E402
from calitp_portfolio.models import load_site  # noqa: E402
from calitp_portfolio.mutations import generate_parts_flat  # noqa: E402
from update_vars import GCS_FILE_PATH  # noqa: E402

PORTFOLIO_SITE_YAML = Path(__file__).parent / "ntd_annual_ridership_report.yml"

# read in rtpa data from dim_orgs
if __name__ == "__main__":
    df = (
        GCSPandas()
        .read_parquet(f"{GCS_FILE_PATH}annual_ridership_report_data.parquet")["rtpa_name"]
        .sort_values()
        .drop_duplicates()
        .to_frame()
    )

    site = load_site(PORTFOLIO_SITE_YAML)
    site = generate_parts_flat(site, param_key="rtpa", values=list(df.rtpa_name))
    site.write_yaml(PORTFOLIO_SITE_YAML)
