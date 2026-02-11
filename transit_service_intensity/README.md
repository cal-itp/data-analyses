# Transit Service Intensity Scripts

First, select analysis date in `update_vars` and stage desired analysis geometry according to pattern in GCS bucket (UZAs are already staged).

## Script Sequence

* `prepare_tracts_borders.py`
* `define_tsi_segments.py`
* `time_distance_in_segments.py`
    * This currently takes a long time to run.
    *
* `borders_stops_aggregation.py`
