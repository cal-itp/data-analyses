"""
Create catalog for `rt_trips_{itp_id}_*` parquets available.
See which dates are available for each operator.

v1 warehouse.
"""
import gcsfs
import yaml

fs = gcsfs.GCSFileSystem()


def grab_itp_id_and_date(filename: str):
    file_cleaned = filename.split("rt_trips/")[1]

    itp_id = file_cleaned.split("_")[0]
    date = file_cleaned.split("_")[1].split(".parquet")[0]

    return int(itp_id), date


if __name__ == "__main__":
    GCS_RT_TRIPS = "gs://calitp-analytics-data/data-analyses/rt_delay/rt_trips/"

    YAML_FILE = "rt_trips_catalog.yaml"

    fs_list = fs.ls(GCS_RT_TRIPS)
    files_to_parse = [f for f in fs_list if ".parquet" in f]

    # First, save out a list of ITP IDs used
    # Don't save as dict yet, because sharing the same key (itp_id)
    # will only allow 1 value to be stored
    included_itp_ids = []

    for f in files_to_parse:
        itp_id, _ = grab_itp_id_and_date(f)
        included_itp_ids.append(itp_id)

    # Find the set, so each ITP ID appears just once
    # Assume that over time, more ITP IDs are added
    ITP_IDS = list(set(included_itp_ids))

    # For each ITP ID present, look for the dates available
    # ITP IDs are allowed to vary in the number of dates with data available
    results_dict = {}

    for itp_id in sorted(ITP_IDS):
        operator_files = [f for f in files_to_parse if str(itp_id) in f]

        # Create a list that stores what dates are available for that ITP ID
        operator_dates = []

        for f in operator_files:
            _, date = grab_itp_id_and_date(f)
            operator_dates.append(date)

        results_dict[itp_id] = sorted(list(set(operator_dates)))

    # Save as a yaml
    with open(YAML_FILE, "w") as file:
        doc = yaml.dump(results_dict, file, indent=1)
