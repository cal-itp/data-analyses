from pathlib import Path

from omegaconf import OmegaConf

PREDICTIONS_GCS = "gs://calitp-analytics-data/data-analyses/rt_predictions/"
VP_GCS = "gs://calitp-analytics-data/data-analyses/rt_vehicle_positions/"


def get_catalog(catalog_name="rt_msa_catalog") -> Path:
    """
    Grab GTFS RT MSA catalog (uses OmegaConf yaml parser).
    """
    catalog_path = Path.cwd().joinpath(f"{catalog_name}.yml")

    return OmegaConf.load(catalog_path)


RT_MSA_DICT = get_catalog("rt_msa_catalog")
