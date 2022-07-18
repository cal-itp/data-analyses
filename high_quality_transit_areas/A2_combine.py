"""
Combine rail, BRT, and ferry stops data.

Before running, check BRT stop maps in notebooks,
and filter out certain stop_ids.

Export combined rail_brt_ferry data into GCS.
"""
import pandas as pd

import A1_rail_ferry_brt as rail_ferry_brt
import B1_bus_corridors as bus_corridors
from shared_utils import utils
#from utilities import GCS_FILE_PATH


metro_street_running =[
    '141012', '13805', '5397', '13803',
    '13804', '5396', '13802', '5395', '5410', '5411', '13817',
    '12304', '5408', '3821', '2603', '3153', '3124', '378', '65300039',
    '65300038', '15820', '13460', '4994', '1813', '2378', '5049',
    '4652', '2377', '4675', '5040', '65300042', '3674', '15713',
    '13561', '5378', '13560', '70500012', '5377', '15612',
    '12416', '11917', '12415', '8704'
]

van_ness_ids = [
    '8096', '8097', '18095', '18098', '8094', '8099', '18093', '8100',
    '18092', '18101', '18102', '18091', '18103', '8090', '18104', '18089',
    '18105', '18088']


if __name__ == "__main__":
    analysis_date = rail_ferry_brt.analysis_date
    # Rail
    rail_stops = rail_ferry_brt.grab_rail_data(analysis_date)

    # BRT
    # LA Metro
    metro_brt_stops = rail_ferry_brt.grab_operator_brt(182, analysis_date)
    metro_brt_stops = rail_ferry_brt.additional_brt_filtering_out_stops(
        metro_brt_stops, 182, metro_street_running)
    
    # AC Transit
    act_brt_stops = rail_ferry_brt.grab_operator_brt(4, analysis_date)
    
    # SF Muni
    muni_brt_stops = rail_ferry_brt.grab_operator_brt(282, analysis_date)

    muni_brt_stops = rail_ferry_brt.additional_brt_filtering_out_stops(
        muni_brt_stops, 282, van_ness_ids)

    # Ferry
    ferry_stops = rail_ferry_brt.grab_ferry_data(analysis_date)
    
    # Concatenate datasets
    rail_brt_ferry = pd.concat([
        rail_stops, 
        metro_brt_stops, act_brt_stops, muni_brt_stops, 
        ferry_stops
    ], axis=0, ignore_index=True)
    
    
    # Export to GCS
    utils.geoparquet_gcs_export(rail_brt_ferry, 
                                f'{bus_corridors.TEST_GCS_FILE_PATH}', 
                                'rail_brt_ferry')

    