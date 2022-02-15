# Queries in common for accessibility
import pandas as pd

from calitp.tables import tbl
from siuba import *

# Stop query
stops = (tbl.gtfs_schedule.stops()
      # unique on itp_id and url_number
      # but there are handful of agencies where same stop has multiple url's
         >> select(_.calitp_itp_id, _.calitp_url_number, 
                   _.stop_id, _.stop_lat, _.stop_lon, 
                   _.wheelchair_boarding, 
                  )
         >> distinct()
)

# Trip query
trips = (tbl.gtfs_schedule.trips()
         >> select(_.calitp_itp_id, _.calitp_url_number, 
                   _.route_id, _.trip_id, 
                   _.wheelchair_accessible)
         >> arrange(_.calitp_itp_id)
         >> distinct()
)



