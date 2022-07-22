"""
Find the bus stops that fall where bus corridors intersect 
and also the stops that fall within the HQ transit corr 
(but may not be stops with highest trip count).


These are hqta_types: 
* major_transit_stop: where 2 HQ bus corridors intersect
* major_stop_bus: the bus stop within the above intersection that does have the highest trip count
* hq_corridor_bus: stops along the HQ transit corr (may not be highest trip count)

From combine_and_visualize.ipynb
"""
import dask.dataframe as dd
import dask_geopandas
import geopandas as gpd
import pandas as pd

from calitp.tables import tbl
from siuba import *

import C1_prep_for_clipping as C1
from A1_rail_ferry_brt import analysis_date
from shared_utils import geography_utils, utils
from utilities import catalog_filepath

stop_cols = ["calitp_itp_id", "stop_id"]

def query_all_stops(analysis_date):
    
    all_stops = (tbl.views.gtfs_schedule_fact_daily_feed_stops()
                 >> filter(_.date == analysis_date)
                 >> filter(_.calitp_extracted_at < analysis_date)
                 >> filter(_.calitp_deleted_at > analysis_date)
                 >> select(_.stop_key)
                 >> inner_join(_, 
                               tbl.views.gtfs_schedule_dim_stops(), 
                               on = 'stop_key')
                 >> select(_.stop_id, _.stop_lat, _.stop_lon, _.calitp_itp_id)
                 >> filter(_.calitp_itp_id != 200)
                 >> distinct(_keep_all=True)
                 >> collect()
                )
    
    tbl_stops = (geography_utils.create_point_geometry(
            all_stops, 
            crs=geography_utils.CA_NAD83Albers
        ).drop(columns = ["stop_lon", "stop_lat"])
    )
    
    return tbl_stops


# Keep the stop_ids in these intersections
# These are stop_ids with highest trips
# Tag these as hqta_type == major_stop_bus?
def create_major_stop_bus(stops_in_intersections, tbl_stops):

    major_stop_bus = (stops_in_intersections
                      [stop_cols + ["hqta_segment_id", "hq_transit_corr"]]
                      .drop_duplicates()
                      .reset_index(drop=True)
                     )

    # this overwrites hqta_type from major_transit_stop to major_stop_bus
    # what are the differences between these 2?
    major_stop_bus = major_stop_bus.assign(
        hqta_type = "major_stop_bus"
    )

    # Add the point geom for this stop
    major_stop_bus2 = dd.merge(tbl_stops, 
                               major_stop_bus,
                               on = stop_cols,
                               how = "inner",
                              )
    
    return major_stop_bus2    


# Is this how major_transit_stop should be constructed? Or the method above? 
# In both cases, major_transit_stop is overwritten with major_stop_bus
def sjoin_all_stops_to_clipped_intersections(clipped_intersections, tbl_stops):
    stops_at_intersection = (dask_geopandas.sjoin(
                                tbl_stops,
                                clipped_intersections,
                                how = "inner"
                            ).drop_duplicates(
                                 subset=["calitp_itp_id_left", "stop_id", "calitp_itp_id_right"])
                             .reset_index(drop=True)
                            )
    
    stops_at_intersection2 = (stops_at_intersection.assign(
                                hqta_type = "major_stop_bus",
                                ).rename(columns = 
                                         {"calitp_itp_id_left": "calitp_itp_id_primary", 
                                          "calitp_itp_id_right": "calitp_itp_id_secondary",
                                         })
                              [["calitp_itp_id_primary", "calitp_itp_id_secondary", 
                                "stop_id", "geometry", "hqta_type"]]
    )
                              
    return stops_at_intersection2


# hqta_type = hq_corridor_bus
# These are bus stops that lie within the HQ corridor, but 
# are not the stops that have the highest trip count
# they may also be stops that don't meet the HQ corridor threshold, but
# are stops that physically reside in the corridor.
def create_stops_along_corridors(tbl_stops):
    
    bus_corridors = C1.prep_bus_corridors()
    
    stops_in_hq_corr = (dask_geopandas.sjoin(
                                tbl_stops, 
                                bus_corridors[["geometry"]],
                                how = "inner", 
                                predicate = "intersects"
                            ).drop(columns = "index_right")
                            .drop_duplicates(subset=stop_cols)
                            .reset_index(drop=True)
                           )
    
    stops_in_hq_corr2 = (stops_in_hq_corr.assign(
                                hqta_type = "hq_corridor_bus",
                            )[stop_cols + ["hqta_type", "geometry"]]
                             .rename(columns = {"calitp_itp_id": "calitp_itp_id_primary"})
                            )
    
    return stops_in_hq_corr2
    
    
    
if __name__=="__main__":

    clipped = C3.process_clipped_intersections()
    stops_in_bus_intersections = C3.merged_clipped_geom_to_highest_trip_stop(clipped)   
    #one_intersection_per_row = C3.explode_geometries(stops_in_bus_intersections)
    
    #Figure out what's the unit for `hqta_type == major_transit_stop`.
    #That's what should be staged at end of C3.

    tbl_stops = query_all_stops(analysis_date)
    tbl_stops_gddf = dask_geopandas.from_geopandas(tbl_stops, npartitions=1)
    
    major_stop_bus = create_major_stop_bus(stops_in_bus_intersections, tbl_stops_gddf)


    # what is the geometry mean for this?
    # why find centroid?
    # Do we want the point geom of the stop? A buffer around the stop? The clipped geom?
    # Here, start with point geom of the stop
    # Is this what we want for major_transit_stop? Take the clipped geom and add all the stops in there?
    major_transit_stop = sjoin_all_stops_to_clipped_intersections(clipped, tbl_stops_gddf)
    
    
    stops_in_hq_corridor = create_stops_along_corridors(tbl_stops_gddf)
