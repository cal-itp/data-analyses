import geopandas as gpd


def explode_segments(
    gdf: gpd.GeoDataFrame, group_cols: list, segment_col: str = "segment_geometry"
) -> gpd.GeoDataFrame:
    """
    Explode the column that is used to store segments, which is a list.
    Take the list and create a row for each element in the list.
    We'll do a rough rank so we can order the segments.
    """
    gdf_exploded = gdf.explode(segment_col).reset_index(drop=True)

    gdf_exploded["temp_index"] = gdf_exploded.index

    gdf_exploded = gdf_exploded.assign(
        segment_sequence=(
            gdf_exploded.groupby(group_cols, observed=True, group_keys=False).temp_index.transform("rank")
            - 1
            # there are NaNs, but since they're a single segment, just use 0
        )
        .fillna(0)
        .astype("int16")
    )

    # Drop the original line geometry, use the segment geometry only
    gdf_exploded2 = (
        gdf_exploded.drop(columns=["geometry", "temp_index"])
        .rename(columns={segment_col: "geometry"})
        .set_geometry("geometry")
        .set_crs(gdf_exploded.crs)
        .sort_values(group_cols + ["segment_sequence"])
        .reset_index(drop=True)
    )

    return gdf_exploded2
