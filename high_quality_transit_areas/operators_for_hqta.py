"""
Do a check of the operators to run, 
what is missing if we put in this
explicit list vs running the tbl.gtfs_schedule.agency query
"""
import pandas as pd

from calitp.tables import tbl


ITP_IDS_IN_GCS = [
    101, 102, 103, 105, 106, 108, 10, 110,
    112, 116, 117, 118, 11, 120, 
    121, 122, 123, 126, 127, 129,
    135, 137, 13, 142, 146, 148, 14,
    152, 154, 159, 15, 
    162, 165, 167, 168, 169, 16, 
    170, 171, 172, 173, 174, 176, 177, 178, 179, 17,
    181, 182, 183, 186, 187, 188, 18, 190, 
    192, 194, 198, 199, 
    201, 203, 204, 206, 207, 208, 
    210, 212, 213, 214, 217, 218, 21, 
    220, 221, 226, 228, 231, 232, 235,
    238, 239, 23, 243, 246, 247, 24, 
    251, 254, 257, 259, 260, 
    261, 263, 264, 265, 269, 
    270, 271, 273, 274, 278, 279,
    280, 281, 282, 284, 287, 289, 
    290, 293, 294, 295, 296, 298, 29, 
    300, 301, 305, 308, 30, 
    310, 312, 314, 315, 320, 323, 324, 327, 329, 
    331, 334, 336, 337, 338, 339, 33, 
    341, 343, 344, 346, 349, 34, 
    350, 351, 356, 35, 360, 361, 365,
    366, 367, 368, 36, 372, 374, 376, 37,
    380, 381, 386, 389, 394, 
    41, 42, 45, 473, 474,
    482, 483, 484, 48, 49, 4, 
    50, 54, 56, 61, 6, 
    70, 71, 75, 76, 77, 79, 
    81, 82, 83, 86, 87, 
    91, 95, 98, 99
]

ITP_IDS = (tbl.gtfs_schedule.agency()
           >> distinct(_.calitp_itp_id)
           >> filter(_.calitp_itp_id != 200)
           >> collect()
).calitp_itp_id.tolist()


IN_GCS_NOT_BQ = set(ITP_IDS_IN_GCS).difference(set(ITP_IDS))
IN_BQ_NOT_GCS = set(ITP_IDS).difference(set(ITP_IDS_IN_GCS))

print(f"ITP IDs in GCS, not in warehouse: {IN_GCS_NOT_BQ}")
print(f"ITP IDs in warehouse, not in GCS: {IN_BQ_NOT_GCS}")