from functools import cache

import geopandas as gpd
import pandas as pd
import shapely
from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
from segment_speed_utils import helpers
from tqdm import tqdm
from update_vars import (
    ANALYSIS_DATE,
    BORDER_BUFFER_METERS,
    GCS_PATH,
    GEOM_ID_COL,
    GEOM_INPUT_PATH,
    GEOM_SUBFOLDER,
)


@cache
def gcs_geopandas():
    return GCSGeoPandas()


tqdm.pandas(desc=f"TSI Segments Progress {ANALYSIS_DATE}")


def overlay_to_borders(
    shape_gdf: gpd.GeoDataFrame, border_gdf: gpd.GeoDataFrame, sensitivity_dist: int = BORDER_BUFFER_METERS * 4
):
    """
    Find where shapes overlap border zones, keep if >= sensitivity distance.
    Screen out shapes briefly crossing border, keep shapes running along border.
    """
    overlaid = shape_gdf.overlay(border_gdf, how="intersection")
    overlaid["length"] = overlaid.geometry.length
    overlaid = overlaid[overlaid.length >= sensitivity_dist]
    return overlaid


def overlay_to_areas(shape_gdf_no_border: gpd.GeoDataFrame, areas_gdf: gpd.GeoDataFrame, id_col: str = GEOM_ID_COL):
    """
    simple overlay with non-border analysis areas
    """
    areas_gdf = areas_gdf[[id_col, "geometry"]]
    return shape_gdf_no_border.overlay(areas_gdf, how="intersection")


def overlay_areas_borders(
    shape_gdf: gpd.GeoDataFrame,
    areas_gdf: gpd.GeoDataFrame,
    border_gdf: gpd.GeoDataFrame,
    sensitivity_dist: int = BORDER_BUFFER_METERS * 4,
    id_col=GEOM_ID_COL,
):
    """
    overlay gtfs shapes to tsi analysis areas and border zones
    """
    border_gdf = border_gdf.drop(columns=["intersection_hash"])
    # try:
    border_overlaid = overlay_to_borders(shape_gdf, border_gdf, sensitivity_dist)
    not_border = shape_gdf.overlay(
        border_overlaid, how="difference"
    )  # strip shape part in border zone to avoid double-counting distance
    areas_overlaid = overlay_to_areas(not_border, areas_gdf, id_col=id_col)
    areas_and_borders = pd.concat([areas_overlaid, border_overlaid]).explode(index_parts=False).reset_index(drop=True)
    areas_and_borders = areas_and_borders.assign(
        border=~areas_and_borders[f"{id_col}_2"].isna(),
        # point where gtfs shape crosses into a tsi analysis area
        start=areas_and_borders.geometry.apply(lambda x: shapely.Point(x.coords[0])),
        tsi_segment_id=areas_and_borders[id_col].combine_first(areas_and_borders.intersection_id).astype(str),
        tsi_segment_meters=areas_and_borders.geometry.length,
    )
    areas_and_borders = areas_and_borders[areas_and_borders.tsi_segment_meters >= sensitivity_dist]
    return areas_and_borders
    # except Exception as e:
    #     print(f'{shape_gdf}, {e}')


if __name__ == "__main__":

    print(f"define_tsi_segments {ANALYSIS_DATE}")
    analysis_geoms = gcs_geopandas().read_parquet(GEOM_INPUT_PATH)
    shapes = helpers.import_scheduled_shapes(ANALYSIS_DATE)
    borders = gcs_geopandas().read_parquet(f"{GCS_PATH}{GEOM_SUBFOLDER}borders_{ANALYSIS_DATE}.parquet")

    trip_cols = [
        "gtfs_dataset_key",
        "name",
        "trip_id",
        "shape_id",
        "shape_array_key",
        "route_id",
        "route_key",
        "direction_id",
        "route_short_name",
        "trip_instance_key",
        "feed_key",
    ]

    trips = helpers.import_scheduled_trips(ANALYSIS_DATE, columns=trip_cols).dropna(subset=["shape_id"])

    # grouping is slow, but may be necessary depending on input geometries and available memory
    # tsi_segs = (
    #     shapes.groupby("shape_array_key")
    #     .progress_apply(overlay_areas_borders, areas_gdf=analysis_geoms, border_gdf=borders)
    #     .reset_index(drop=True)
    # )

    tsi_segs = overlay_areas_borders(shape_gdf=shapes, areas_gdf=analysis_geoms, border_gdf=borders, id_col=GEOM_ID_COL)
    path = f"{GCS_PATH}{GEOM_SUBFOLDER}tsi_segs_{ANALYSIS_DATE}.parquet"
    gcs_geopandas().geo_data_frame_to_parquet(tsi_segs, path)
