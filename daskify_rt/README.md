# GTFS RT Speeds by Segment Pipeline

Work related to using `dask` to produce speeds by segments from GTFS RT vehicle positions data.

* Data [catalog](https://github.com/cal-itp/data-analyses/blob/main/daskify_rt/catalog.yml)
* [dask utils](https://github.com/cal-itp/data-analyses/blob/main/daskify_rt/dask_utils.py)
* GCS folder: `dask_test`
* GitHub Epics
    * [Initial daskifying and exploration](https://github.com/cal-itp/data-analyses/issues/592)

| warehouse         | gcs                                                       | github                                                  |
|-------------------|-----------------------------------------------------------|---------------------------------------------------------|
| schedule          | gtfs_schedule (make dir when we start)                                       | gtfs_schedule (existing as gtfs_schedule_calendar_dates, need to rename)                |
| rt_segment_speeds | rt_segment_speeds (existing as dask_test, need to rename) | rt_segment_speeds (existing daskify_rt, need to rename) |
| rt_vs_schedule    | rt_vs_schedule                                            | rt_scheduled_v_ran (existing)                           |
| rt_predictions    | rt_predictions                                            | rt_predictions (make dir when we start)                 |


## Steps
1. concat vehicle positions for a single day - change df to gdf (`A1`)
1. cut route segments, make crosswalk for trips table (with `route_id` and `direction_id` to be linked to a `route_direction_identifier` used for segments (`A2`)
1. spatial join vehicle positions by route-direction to the segments (use delayed after looping within routes for operator to assemble operator parquets) (`A3`)
1. pare down vehicle positions to just enter/exit within a segment + placeholder for dropping unusable trips. should exclude trips with too little info or `trip_id is None` at the start, then pare down
1. do linear referencing within segments to get `distance_elapsed` and `time_elapsed`, and calculate speeds. save both partitioned parquets and individual operator parquets for now