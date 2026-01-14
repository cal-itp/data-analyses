import geopandas as gpd
import pandas as pd
import shapely
from segment_speed_utils import helpers
from tqdm import tqdm
from update_vars import ANALYSIS_DATE, BORDER_BUFFER_METERS
from utils import read_census_tracts

tqdm.pandas(desc=f"TSI Segments Progress {ANALYSIS_DATE}")


def overlay_to_borders(
    shape_gdf: gpd.GeoDataFrame, border_gdf: gpd.GeoDataFrame, sensitivity_dist: int = BORDER_BUFFER_METERS * 4
):
    """ """
    overlaid = shape_gdf.overlay(border_gdf, how="intersection")
    overlaid["length"] = overlaid.geometry.length
    overlaid = overlaid[overlaid.length >= sensitivity_dist]
    return overlaid


def overlay_to_areas(shape_gdf_no_border: gpd.GeoDataFrame, areas_gdf: gpd.GeoDataFrame, id_col: str = "tract"):
    """ """
    areas_gdf = areas_gdf[[id_col, "geometry"]]
    return shape_gdf_no_border.overlay(areas_gdf, how="intersection")


def overlay_areas_borders(
    shape_gdf: gpd.GeoDataFrame,
    areas_gdf: gpd.GeoDataFrame,
    border_gdf: gpd.GeoDataFrame,
    sensitivity_dist: int = BORDER_BUFFER_METERS * 4,
    id_col="tract",
):
    """ """
    border_gdf = border_gdf.drop(columns=["intersection_hash"])
    # try:
    border_overlaid = overlay_to_borders(shape_gdf, border_gdf, sensitivity_dist)
    not_border = shape_gdf.overlay(border_overlaid, how="difference")
    areas_overlaid = overlay_to_areas(not_border, areas_gdf, id_col=id_col)
    areas_and_borders = pd.concat([areas_overlaid, border_overlaid]).explode(index_parts=False).reset_index(drop=True)
    areas_and_borders = areas_and_borders.assign(
        border=~areas_and_borders[f"{id_col}_2"].isna(),
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
    tracts = read_census_tracts(ANALYSIS_DATE)
    shapes = helpers.import_scheduled_shapes(ANALYSIS_DATE)
    borders = gpd.read_parquet(f"borders_{ANALYSIS_DATE}.parquet")

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

    tsi_segs = (
        shapes.groupby("shape_array_key")
        .progress_apply(overlay_areas_borders, areas_gdf=tracts, border_gdf=borders)
        .reset_index(drop=True)
    )
    tsi_segs.to_parquet(f"tsi_segments_{ANALYSIS_DATE}.parquet")
