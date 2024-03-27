"""
"""
import datetime
import geopandas as gpd
import pandas as pd
import shapely

from dask import delayed, compute

from segment_speed_utils import helpers, neighbor, segment_calcs, wrangle_shapes
from segment_speed_utils.project_vars import SEGMENT_GCS, SHARED_GCS, PROJECT_CRS
import interpolate_stop_arrival

road_id_cols = ["linearid", "mtfcc", "primary_direction"]
segment_identifier_cols = road_id_cols + ["segment_sequence"]
segment_identifier_cols2 = ['linearid', 'mtfcc', 
                            'stop_primary_direction', 'segment_sequence']
test_shapes = [
    'e3435a4b882913d92a12563910f7193d',
    'dfae5639b824ed6ad87ed753575f5381',
    '626724031caabc3abcf3f183f4c9c718',
    'b63ae7a27ecb677ac93c0373245ea21b',
    '85a03eed08934ed37f204e9aae6cbd36',
    '20af512ed1ada757a3b49beb36b623ad',
    'a1999da4d09bc81548fe3e2b0fb458b4',
    '1e7115aaac1fcb58c6509c7a90d9741c',
    '3d437ea82b56e9827d15527ce41c716a',
    '40469843deb94fbf61ef5f38bb76a137',
    '04353cab33c0b31d8e9575ace6f9f6da',
    'f165d1399f8880fc0011eb75b740ba24',
    'ad15c10f6bc86dd6d3c02bfe6daf7ad9',
    '71b06c07dabcecf71ddc5a8f3638ccf1',
    '4e2b8555bc9936d711c8748694d67a3e',
    '2e00363dba250fae30b7c14596f18907',
    '2fd717cc5df1495434b8170e294137a6',
    '60057141822cc9d9ac4795a83b2f25f1',
    '5c0a268639c22fc58c8c842741cf68e3',
    '67bc9cc93630ea8a22783cd59d11d46b',
    '81e5d878737a777ffea18b0c08f58118',
    'a91351f71fc4d2c0b457e1701a3ade64',
    '67af448db16c21d09812f58cf065aac5',
    'bd66e7d4ffae3bc36888cfddaf5e2e44',
    'b9b803a342d42f72bbe25042f7328385'
]

def get_shape_road_crosswalk(
    analysis_date: str, **kwargs
) -> pd.DataFrame:

    # shapes to road crosswalk
    shape_road_crosswalk = pd.read_parquet(
        f"{SEGMENT_GCS}roads_staging/"
        f"shape_road_crosswalk_{analysis_date}.parquet",
    )
    
    # link shapes to trip
    shape_to_trip = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key"],
        **kwargs
        #filters = [[("shape_array_key", "==", one_shape)]]
    )

    shape_road_crosswalk = shape_road_crosswalk.merge(
        shape_to_trip,
        on = "shape_array_key",
        how = "inner"
    )
    
    return shape_road_crosswalk


def make_road_stops_long(
    shape_road_crosswalk: pd.DataFrame
) -> gpd.GeoDataFrame:
    
    road_segments = gpd.read_parquet(
        f"{SHARED_GCS}road_segments/",
        columns = segment_identifier_cols + ["geometry"],
    )
    
    road_segments2 = pd.merge(
        road_segments,
        shape_road_crosswalk,
        on = ["linearid", "mtfcc", "segment_sequence"], 
        how = "inner"
    )
    
    # Beginning of road segment (1st coord)
    road_segments0 = road_segments2.assign(
        geometry = road_segments2.apply(
            lambda x: shapely.Point(x.geometry.coords[0]), 
            axis=1),
    ).assign(stop_type=0)

    # End of road segment (last coord)
    road_segments1 = road_segments2.assign(
        geometry = road_segments2.apply(
            lambda x: shapely.Point(x.geometry.coords[-1]), 
            axis=1),
    ).assign(stop_type=1)

    # Make long, similar to how stop_times is set up
    # For roads, each segment has beginning and end stop
    # We want to interpolate arrival times for both 
    # to calculate speed
    road_segments_long = pd.concat(
        [road_segments0, road_segments1], 
        axis=0
    ).sort_values(
        ["linearid", "segment_sequence", "stop_type"]
    ).rename(
        columns = {"primary_direction": "stop_primary_direction"}
    ).reset_index(drop=True)
    
    return road_segments_long

def merge_nn_with_shape(results2):

    results2 = results2.assign(
        stop_geometry = results2.stop_geometry.to_crs(PROJECT_CRS),
        vp_coords_trio = results2.vp_coords_trio.to_crs(PROJECT_CRS)
    )
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS
    ).dropna(subset="geometry")

    gdf = pd.merge(
        results2,
        shapes.rename(columns = {"geometry": "shape_geometry"}),
        on = "shape_array_key",
        how = "inner"
    )
    
    stop_meters_series = []
    stop_arrival_series = []
    
    for row in gdf.itertuples():

        stop_meters, interpolated_arrival = interpolate_stop_arrival.project_points_onto_shape(
            getattr(row, "stop_geometry"),
            getattr(row, "vp_coords_trio"),
            getattr(row, "shape_geometry"),
            getattr(row, "location_timestamp_local_trio")
        )

        stop_meters_series.append(stop_meters)
        stop_arrival_series.append(interpolated_arrival)

    results2 = gdf.assign(
        stop_meters = stop_meters_series,
        arrival_time = stop_arrival_series,
    )[segment_identifier_cols2 + [
        "trip_instance_key", "shape_array_key", 
        "stop_type",
         "stop_meters", "arrival_time"]
     ].sort_values(
        segment_identifier_cols2 + ["trip_instance_key", "stop_type", ]
    ).reset_index(drop=True)
    
    return results2


def quick_calculate_speeds(results2):
    grouped_df = results2.groupby(segment_identifier_cols2 + 
                                   ["trip_instance_key"])

    min_arrival = grouped_df.agg({"arrival_time": "min"}).reset_index()
    max_arrival = grouped_df.agg({"arrival_time": "max"}).reset_index()
    
    # If min/max arrival are the same, remove
    # The same trio of vp is attached to the road segment's
    # beginning and end
    min_max_arrival = pd.merge(
        min_arrival,
        max_arrival,
        on = segment_identifier_cols2 + ["trip_instance_key"]
    ).query('arrival_time_x != arrival_time_y')
    
    results3 = pd.merge(
        results2,
        min_max_arrival[segment_identifier_cols2 + ["trip_instance_key"]],
        on = segment_identifier_cols2 + ["trip_instance_key"],
        how = "inner"
    )
    
    results3 = segment_calcs.convert_timestamp_to_seconds(
        results3, ["arrival_time"]
    ).sort_values(
        segment_identifier_cols2 + ["trip_instance_key"]
    ).reset_index(drop=True)
    
    trip_cols = segment_identifier_cols2 + ["trip_instance_key"]
    
    results3 = results3.assign(
        subseq_arrival_time_sec = (results3.groupby(trip_cols, 
                                             observed=True, group_keys=False)
                                  .arrival_time_sec
                                  .shift(-1)
                                 ),
        subseq_stop_meters = (results3.groupby(trip_cols, 
                                        observed=True, group_keys=False)
                             .stop_meters
                             .shift(-1)
                            )
    )
    
    speed = results3.assign(
        meters_elapsed = results3.subseq_stop_meters - results3.stop_meters, 
        sec_elapsed = results3.subseq_arrival_time_sec - results3.arrival_time_sec,
    ).pipe(
        segment_calcs.derive_speed, 
        ("stop_meters", "subseq_stop_meters"), 
        ("arrival_time_sec", "subseq_arrival_time_sec")
    )
    
    return speed


if __name__ == "__main__":
    
    #from segment_speed_utils.project_vars import analysis_date
    from shared_utils import rt_dates
    analysis_date = rt_dates.DATES["oct2023"]

    start = datetime.datetime.now()
    
    shape_road_crosswalk = get_shape_road_crosswalk(
        analysis_date, 
        filters = [[("shape_array_key", "in", test_shapes)]]
    )
    
    road_segments_long = make_road_stops_long(shape_road_crosswalk)
    
    gdf = neighbor.merge_stop_vp_for_nearest_neighbor(
        road_segments_long, 
        analysis_date
    )
    
    results = neighbor.add_nearest_neighbor_result(gdf, analysis_date)
    #results = compute(results)[0]
    
    #utils.geoparquet_gcs_export(
    #    results,
    #    SEGMENT_GCS,
    #    f"roads_staging/nearest_{analysis_date}"
    #)
    
    results2 = delayed(merge_nn_with_shape)(results)
    #results2 = compute(results2)[0]
    
    #utils.geoparquet_gcs_export(
    #    results2,
    #    SEGMENT_GCS,
    #    f"roads_staging/interp_{analysis_date}"
    #)
    
    speeds = delayed(quick_calculate_speeds)(results2)
    
    time1 = datetime.datetime.now()
    print(f"delayed dfs: {time1 - start}")
    
    speeds = compute(speeds)[0]
    
    speeds.to_parquet(
        f"{SEGMENT_GCS}roads_staging/"
        f"test_speeds_{analysis_date}.parquet")
    
    end = datetime.datetime.now()
    print(f"test 25 shapes: {end - start}")