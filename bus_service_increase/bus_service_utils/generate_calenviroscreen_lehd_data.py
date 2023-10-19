"""
Download LEHD data from Urban Institute data catalog

Combine with CalEnviroscreen data, 
clean both CalEnviroScreen and LEHD, generate
needed columns, and upload to GCS.
"""
from bus_service_utils import calenviroscreen_lehd_utils


if __name__ == "__main__":
    
    LEHD_DATE_DOWNLOAD = "2021/04/19/"

    #calenviroscreen_lehd_utils.download_lehd_data(
    #    LEHD_DATE_DOWNLOAD, LEHD_DATASETS)
    
    calenviroscreen_lehd_utils.generate_calenviroscreen_lehd_data(
        calenviroscreen_quartile_groups = 3,
        lehd_datasets = calenviroscreen_lehd_utils.LEHD_DATASETS,
        GCS = True
    )