"""
Create the bus-related hqta_types

These are hqta_types: 
* major_stop_bus: the bus stop within the above intersection does not necessarily have
the highest trip count
* hq_corridor_bus: stops along the HQ transit corr (may not be highest trip count)

From combine_and_visualize.ipynb
"""
import dask.dataframe as dd
import dask_geopandas
import datetime as dt
import geopandas as gpd
import pandas as pd

from calitp.tables import tbl
from siuba import *

import C1_prep_for_clipping as prep_clip
import C3_clean_clipped_intersections as clean_clip
from A1_rail_ferry_brt import analysis_date
from B1_bus_corridors import TEST_GCS_FILE_PATH
from shared_utils import utils, geography_utils
from utilities import catalog_filepath

# Input files
ALL_BUS = catalog_filepath("all_bus")

segment_cols = ["calitp_itp_id", "hqta_segment_id"]
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
                 >> select(_.calitp_itp_id, _.stop_id, _.stop_lat, _.stop_lon)
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


def merge_clipped_with_hqta_segments():   
    # Clean up the clipped df and remove large shapes
    clipped_df = clean_clip.process_clipped_intersections()
    
    # For hqta_segment level data, only 1 stop is attached to each segment
    # It's the stop with the highest trip count
    hqta_segment = dask_geopandas.read_parquet(ALL_BUS)
    
    keep_cols = segment_cols + ["stop_id", "hq_transit_corr"]

    stops_in_bus_intersections = dd.merge(
        # Put clipped geometries on the left for merge
        # Keep the clipped geom, don't want the full hqta_segment geom
        clipped_df,
        hqta_segment[keep_cols],
        on = segment_cols,
        how = "inner"
    )
    
    # Change to gdf because gdf.explode only works with geopandas, not dask gdf
    return stops_in_bus_intersections.compute()


def explode_bus_intersections(gdf):
    # Explode so that the multipolygon becomes multiple rows of polygons
    # Each polygon contains one clipped area
    # Explode works with dask dataframe, but not dask gdf
    keep_cols = segment_cols + ["stop_id", "hq_transit_corr", "geometry"]
    
    one_intersection_per_row = gdf[keep_cols].explode(ignore_index=True)
    
    return one_intersection_per_row


def only_major_bus_stops(all_stops, bus_intersections):
    all_stops_for_major_operators = dd.merge(
        all_stops,
        bus_intersections[["calitp_itp_id"]].drop_duplicates(),
        on = "calitp_itp_id",
        how = "inner"
    )
    
    return all_stops_for_major_operators


def create_major_stop_bus(all_stops, bus_intersections):
    # Narrow down all stops to only include stops from operators
    # that have at least 1 stop that is in freq_bus_stops
    major_stops = only_major_bus_stops(all_stops, bus_intersections)

    major_bus_stops_in_intersections = (
        dask_geopandas.sjoin(
            major_stops,
            bus_intersections[["calitp_itp_id", "geometry"]],
            how = "inner",
            predicate = "within"
        ).drop(columns = "index_right")
        .drop_duplicates(
            subset=["calitp_itp_id_left", "stop_id", "calitp_itp_id_right"])
    ).reset_index(drop=True)
    # why get centroid? drawing buffer around that, but
    # centroid may be different than the bus stop's point geom
    # which geometry should we be left with? both options are point geom
    # should it reflect the bus stop or the centroid of the intersection?
    
    stops_in_intersection = (major_bus_stops_in_intersections.assign(
                                hqta_type = "major_stop_bus",
                                ).rename(columns = 
                                         {"calitp_itp_id_left": "calitp_itp_id_primary", 
                                          "calitp_itp_id_right": "calitp_itp_id_secondary",
                                         })
                              [["calitp_itp_id_primary", "calitp_itp_id_secondary", 
                                "stop_id", "geometry", "hqta_type"]]
    )
    
    return stops_in_intersection


# hqta_type = hq_corridor_bus
# These are bus stops that lie within the HQ corridor, but 
# are not the stops that have the highest trip count
# they may also be stops that don't meet the HQ corridor threshold, but
# are stops that physically reside in the corridor.
def create_stops_along_corridors(tbl_stops):
    
    bus_corridors = prep_clip.prep_bus_corridors()
    
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


if __name__ == "__main__":
    
    start = dt.datetime.now()

    freq_bus_stops_in_intersections = merge_clipped_with_hqta_segments()

    # This exploded geometry is what will be used in spatial join on all stops
    # to see which stops fall in this
    bus_intersections = explode_bus_intersections(freq_bus_stops_in_intersections)
    
    #all_stops = query_all_stops(analysis_date)
    all_stops = dask_geopandas.read_parquet("./data/all_stops.parquet")
    
    major_stop_bus = create_major_stop_bus(all_stops, bus_intersections)

    stops_in_hq_corr = create_stops_along_corridors(all_stops)
    
    # Export locally
    # Remove in the following script after appending
    major_stop_bus.compute().to_parquet("./data/major_stop_bus.parquet")
    stops_in_hq_corr.compute().to_parquet("./data/stops_in_hq_corr.parquet")
    
    end = dt.datetime.now()
    print(f"execution time: {end-start}")