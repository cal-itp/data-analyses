"""
Use output from `cut_road_segments.py` and condense this
into 1 km cutoff points for roads.

Store results once.
"""
import datetime
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils

from segment_speed_utils import vp_transform
from segment_speed_utils.project_vars import GTFS_DATA_DICT, PROJECT_CRS, SHARED_GCS
from shared_utils import catalog_utils

catalog = catalog_utils.get_catalog("shared_data_catalog")

def spatial_join_shn_caltrans_districts(
    gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Attach caltrans_district to Census TIGER roads linearid.
    """
    districts = catalog.caltrans_districts.read()[
        ["DISTRICT", "geometry"]
    ].to_crs(PROJECT_CRS).rename(
        columns = {"DISTRICT": "caltrans_district"}
    )
    
    # inner join gets us more rows than original SHN because
    # some linearids intersect with multiple districts
    # this is ok, but it makes doing a left join much more time-consuming
    crosswalk = gpd.sjoin(
        gdf,
        districts,
        how = "inner",
        predicate = "intersects"
    )[["linearid", "caltrans_district"]].drop_duplicates()
    
    return crosswalk


if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    road_segment_cols = [*GTFS_DATA_DICT.shared_data.road_segment_cols]
    segment_type = "onekm"
    
    shn = gpd.read_parquet(
        f"{SHARED_GCS}segmented_roads_{segment_type}_2020.parquet",
        columns = road_segment_cols + ["destination"],
    ).to_crs(PROJECT_CRS).pipe(
        vp_transform.condense_point_geom_to_line,
        group_cols = ["linearid", "fullname"],
        geom_col = "destination",
        array_cols = ["segment_sequence"],
        sort_cols = ["linearid", "fullname", "segment_sequence"]
    ).rename(
        columns = {"destination": "geometry"}
    ).set_geometry(
        "geometry"
    ).set_crs(
        PROJECT_CRS
    )
    
    shn_with_district = spatial_join_shn_caltrans_districts(shn)
    
    shn2 = pd.merge(
        shn,
        shn_with_district,
        on = "linearid",
        how = "left"
    )
    
    utils.geoparquet_gcs_export(
        shn2,
        SHARED_GCS,
        f"condensed_shn_{segment_type}"
    )
    
    end = datetime.datetime.now()
    print(f"condense {segment_type} SHN segments: {end - start}")
    
    #condense onekm SHN segments: 0:00:33.344454    
    #add district column 0:01:26.688158