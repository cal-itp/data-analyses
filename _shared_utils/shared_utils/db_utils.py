import os

from sqlalchemy import create_engine

CALITP_BQ_MAX_BYTES = os.environ.get("CALITP_BQ_MAX_BYTES", 5_000_000_000)
CALITP_BQ_LOCATION = os.environ.get("CALITP_BQ_LOCATION", "us-west2")


def get_engine(max_bytes=None, project="cal-itp-data-infra", dataset=None):
    # TODO: update calitp_data_analysis.sql.get_engine to accept dataset arg
    max_bytes = CALITP_BQ_MAX_BYTES if max_bytes is None else max_bytes

    cred_path = os.environ.get("CALITP_SERVICE_KEY_PATH")

    # Note that we should be able to add location as a uri parameter, but
    # it is not being picked up, so passing as a separate argument for now.
    return create_engine(
        f"bigquery://{project}/{dataset if dataset else ''}?maximum_bytes_billed={max_bytes}",  # noqa: E231
        location=CALITP_BQ_LOCATION,
        credentials_path=cred_path,
    )
