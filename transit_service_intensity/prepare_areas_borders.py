# import intake
import uuid
from functools import cache

import geopandas as gpd
import google.auth
import pandas as pd
from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
from calitp_data_analysis.gcs_pandas import GCSPandas
from calitp_data_analysis.geography_utils import CA_NAD83Albers_m
from segment_speed_utils import helpers
from update_vars import (
    ANALYSIS_DATE,
    BORDER_BUFFER_METERS,
    GCS_PATH,
    GEOM_INPUT_PATH,
    GEOM_SUBFOLDER,
)

credentials, _ = google.auth.default()


@cache
def gcs_pandas():
    return GCSPandas()


@cache
def gcs_geopandas():
    return GCSGeoPandas()


# def read_census_tracts(crs: str = geography_utils.CA_NAD83Albers_m) -> gpd.GeoDataFrame:
#     census_tracts = (
#         catalog.calenviroscreen_lehd_by_tract(geopandas_kwargs={"storage_options": {"token": credentials}})
#         .read()
#         .to_crs(crs)[["Tract", "pop_sq_mi", "Population", "geometry"]]
#     ).rename(columns={"Tract": "tract", "Population": "population"})
#     return census_tracts


def intersection_hash(row, id_col="tract"):
    """
    Get unique hash of intersection zones.
    No need to keep both t1 x t2 and t2 x t1
    """
    t1 = int(row[f"{id_col}_1"][2:])  # drop state code
    t2 = int(row[f"{id_col}_2"][2:])
    row_areas = [t1, t2]
    row_areas.sort()  # modifies inplace
    return hash(tuple(row_areas))


def find_borders(
    areas_gdf: gpd.GeoDataFrame, border_buffer: int = BORDER_BUFFER_METERS, id_col: str = "tract"
) -> gpd.GeoDataFrame:
    """ """
    areas_gdf = areas_gdf.copy()
    areas_gdf.geometry = areas_gdf.buffer(border_buffer)
    borders = gpd.overlay(areas_gdf, areas_gdf)
    borders = borders[borders[f"{id_col}_1"] != borders[f"{id_col}_2"]]
    # for dropping mirrored borders
    borders["intersection_hash"] = borders.apply(intersection_hash, axis=1, id_col=id_col)
    borders = borders.drop_duplicates(subset=["intersection_hash"])
    # for more elegant tracking
    borders["intersection_id"] = [str(uuid.uuid4()) for _ in range(borders.shape[0])]
    return borders


def find_shapes_in_areas_borders(shape_stops, areas, borders, id_col="tract"):
    """
    sjoin stops to areas and border zones by GTFS shape.
    create tsi_segment_id equal to area id_col if a single area
    or intersection_id if a border zone
    """
    shape_stops_areas_borders = pd.concat([areas, borders]).sjoin(shape_stops).drop(columns="index_right")

    shape_stops_areas_borders = shape_stops_areas_borders.assign(
        tsi_segment_id=shape_stops_areas_borders[f"{id_col}"]
        .combine_first(shape_stops_areas_borders.intersection_id)
        .astype(str)
    )
    return shape_stops_areas_borders


if __name__ == "__main__":

    print(f"prepare_areas_borders {ANALYSIS_DATE}")
    # analysis_geoms = read_census_tracts(ANALYSIS_DATE)
    analysis_geoms = gcs_geopandas().read_parquet(GEOM_INPUT_PATH)
    assert analysis_geoms.crs == CA_NAD83Albers_m
    shapes = helpers.import_scheduled_shapes(ANALYSIS_DATE)
    borders = find_borders(areas_gdf=analysis_geoms, id_col="uace20")
    st = helpers.import_scheduled_stop_times(
        analysis_date=ANALYSIS_DATE, columns=["feed_key", "trip_id", "stop_id"], get_pandas=True
    )
    trips = helpers.import_scheduled_trips(ANALYSIS_DATE, columns=["shape_array_key", "trip_id", "feed_key"])
    stops = helpers.import_scheduled_stops(ANALYSIS_DATE, columns=["feed_key", "stop_id", "geometry"])

    shape_stops = (
        stops.merge(st, on=["feed_key", "stop_id"])
        .merge(trips, on=["feed_key", "trip_id"])
        .drop_duplicates(subset=["feed_key", "shape_array_key", "stop_id"])
        .dropna()
    )

    gcs_geopandas().geo_data_frame_to_parquet(borders, f"{GCS_PATH}{GEOM_SUBFOLDER}borders_{ANALYSIS_DATE}.parquet")
    shape_stops_areas_borders = find_shapes_in_areas_borders(shape_stops, analysis_geoms, borders, id_col="uace20")
    keep_cols = ["shape_array_key", "tsi_segment_id"]
    shape_stops_areas_borders = shape_stops_areas_borders[keep_cols].drop_duplicates()
    gcs_pandas().data_frame_to_parquet(
        shape_stops_areas_borders, f"{GCS_PATH}{GEOM_SUBFOLDER}shape_stops_areas_borders_{ANALYSIS_DATE}.parquet"
    )
