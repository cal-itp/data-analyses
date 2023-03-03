# GTFS RT Speeds by Segment Pipeline

Work related to using `dask` to produce speeds by segments from GTFS RT vehicle positions data.

* Data [catalog](https://github.com/cal-itp/data-analyses/blob/main/rt_segment_speeds/catalog.yml)
* `segment_speed_utils`: install with `pip install -r requirements.txt`
* GCS folder: `rt_segment_speeds`
* scripts for creating tables for exploration: `scripts/`
    * stages for data processing split up by `A_`, `B_`
    * scripts run consecutively within each stage `A1`, `A2`, ...
* exploratory notebooks: prefix with `00_`, `01_`, `02_`, etc.
* [Exploratory epic](https://github.com/cal-itp/data-analyses/issues/592)

| warehouse         | gcs               | github             |
|-------------------|-------------------|--------------------|
| schedule          | gtfs_schedule     | gtfs_schedule      |
| rt_segment_speeds | rt_segment_speeds | rt_segment_speeds  |
| rt_vs_schedule    | rt_vs_schedule    | rt_scheduled_v_ran |
| rt_predictions    | rt_predictions    | rt_predictions     | 


## Scripts
1. cut segments (route segments or stop segments)
1. spatial join vehicle positions to the segments  (`A3`)
1. pare down vehicle positions to just enter/exit within a segment + placeholder for dropping unusable trips. should exclude trips with too little info or `trip_id is None` at the start, then pare down (`A4`)
1. do linear referencing within segments to get `distance_elapsed` and `time_elapsed`, and calculate speeds. save as partitioned parquets (`A5`)