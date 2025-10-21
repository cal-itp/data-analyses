# Notes for 2025 MTC Collaboration

## 10/21/2025 Data Sharing

### Google Cloud Storage

Data is available in a public GCS bucket (link provided separately). GCS offers [multiple ways to download](https://cloud.google.com/storage/docs/access-public-data#console).

The bucket currently contains data at two grains for each of six dates (September 23 - 25 and October 14 - 16).

* `speeds_{date}.parquet`: [Trip-level speeds](https://github.com/cal-itp/data-analyses/blob/12055b148d52a6fb4e63e6ed2bd72563bdcbb8d2/_shared_utils/shared_utils/gtfs_analytics_data.yml#L107) from before aggregation step. One row per trip and segment. Does not include geometries.

* `speeds_shape_timeofday_speedmap_segments_{date}.parquet`: [Aggregated speeds](https://github.com/cal-itp/data-analyses/blob/12055b148d52a6fb4e63e6ed2bd72563bdcbb8d2/_shared_utils/shared_utils/gtfs_analytics_data.yml#L115). One row for each GTFS shape, segment, and time of day. Includes geometries.
    * See [here](https://github.com/cal-itp/data-analyses/blob/12055b148d52a6fb4e63e6ed2bd72563bdcbb8d2/rt_segment_speeds/scripts/speeds_by_time_of_day.py#L77-L156) for aggregation step, which drops trip speed values that are either null or above 80 mph.
    
## Relationship to Open Data and California Transit Speed Maps Site

`speeds_shape_timeofday_speedmap_segments_{date}.parquet` is the same data that we publish on the [Open Data Portal](https://gis.data.ca.gov/datasets/4937eeb59fdb4e56ae75e64688c7f2c0_0/about), but includes some additional or differently named columns. Our Open Data Portal dataset does have ESRI [metadata](https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHrailroad/Speeds_by_Stop_Segments/FeatureServer/0/metadata) available.

* `analysis_name`: same as `agency` in ESRI metadata
* `source_record_id`: same as `org_id` in ESRI metadata
* `shape_id`: GTFS `shape_id`
* `n_trips_sch`: number of trips we _expected_ to observe per GTFS Schedule data.
* `trips_hr_sch`: frequency in time period based on scheduled service levels
* `segment_id`: formatted as {GTFS stop_id}-{GTFS stop_id}-#
    * \# is 1 for the first segment between those stops. If higher numbers are present, they indicate additional 1km segments between widely spaced stops.

Our [California Transit Speed Maps](https://analysis.dds.dot.ca.gov/rt/README.html) site is also built on the same data, but with each map filtered to an individual operator and time period. Also, geometries are [transformed into arrow polygons](https://github.com/cal-itp/data-analyses/blob/12055b148d52a6fb4e63e6ed2bd72563bdcbb8d2/_shared_utils/shared_utils/rt_utils.py#L319-L376) for display.

The trip-level `speeds_{date}.parquet` file should enable more detailed analysis, including by using aggregation periods other than our defaults.