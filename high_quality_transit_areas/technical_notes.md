# HQ Transit Corridors/Major Transit Stops Technical Notes

## Parameter Definitions

Mostly located in [update_vars.py](update_vars.py).

* `HQ_TRANSIT_THRESHOLD`, `MS_TRANSIT_THRESHOLD`: Statutory definition, see [README.md](README.md)
* `AM_PEAK`, `PM_PEAK`: Reflect most common MPO methodologies, not defined in statute
* `HQTA_SEGMENT_LENGTH`, `SEGMENT_BUFFER_METERS`: Our definitions for splitting corridors into analysis segments and matching them to stops
* `CORRIDOR_BUFFER_METERS`: Statutory definition
* `SHARED_STOP_THRESHOLD`: Our threshold for finding similar routes (routes sharing at least this many stops with another in the same direction). Reflects the 96th percentile. 

## Script Sequence and Notes

Defined via [Makefile](Makefile)

## `create_aggregate_stop_frequencies.py`

This script finds similar (collinear) routes.

### Initial steps

Get frequencies at each stop for defined peak periods, with one version only looking at the single most frequent routes per stop and another version looking at all routes per stop.

Find stops in the multi-route version that qualify at a higher threshold than they do in the single-route version. These are the stops (and thus routes and feeds) that we want to check for collinearity.

### Detailed collinearity evaluation

1. Get a list of unique feeds where at least one route_directions pair qualifies to evaluate.
1. Get stop_times filtered to that feed, and filter that to stops that only qualify with multiple routes, and route directions that pair with at least one other route_direction. Do not consider pairs between the same route in one direction and the same route in the opposite direction.
1. After that filtering, check again if stop_times includes the minimum frequency to qualify at each stop. Exclude stops where it doesn't.
1. Then... evaluate which route_directions can be aggregated at each remaining stop. From the full list of route_directions (sorted by frequency) serving the stop, use `list(itertools.combinations(this_stop_route_dirs, 2))` to get each unique pair of route_directions. Check each of those unique pairs to see if it meets the `SHARED_STOP_THRESHOLD`. If they all do, keep all stop_times entries for that stop, different route_directions can be aggregated together at that stop. If any do not, remove the least frequent route_direction and try again, until a subset passes (only keep stop_times for that subset) or until all are eliminated. Currently implemented recursively as below:

    ```
    attempting ['103_1', '101_1', '102_1', '104_1']... subsetting...
    attempting ['103_1', '101_1', '102_1']... subsetting...
    attempting ['103_1', '101_1']... matched!

    attempting ['103_1', '101_0', '101_1', '103_0']... subsetting...
    attempting ['103_1', '101_0', '101_1']... subsetting...
    attempting ['103_1', '101_0']... subsetting...
    exhausted!
    ```

1. With that filtered stop_times, recalculate stop-level frequencies as before. Only keep stops meeting the minimum frequency threshold for a major stop or HQ corridor.
1. Finally, once again apply the `SHARED_STOP_THRESHOLD` after aggregation (by ensuring at least one route_dir at each stop has >= `SHARED_STOP_THRESHOLD` frequent stops). Exclude stops that don't meet this criteria.

#### edge cases:

[AC Transit 45](https://www.actransit.org/sites/default/files/timetable_files/45-2023_12_03.pdf) _Opposite directions share a same-direction loop._ __Solved__ by preventing the same route from being compared with itself in the opposite direction.

[SDMTS 944/945](https://www.sdmts.com/sites/default/files/routes/pdf/944.pdf) _Shared frequent stops are few, and these routes are isolated._ __Solved__ by once again applying the `SHARED_STOP_THRESHOLD` after aggregation (by ensuring at least one route_dir at each stop has >= `SHARED_STOP_THRESHOLD` frequent stops). Complex typology including a loop route, each pair of [944, 945, 945A(946)] has >= threshold... but not actually in the same spots!

### Export

Export stop-level frequencies that are a composite of single-route results, and multi-route results passing the collinearity evaluation. These will be the stop-level frequencies used in subsequent steps.

## `sjoin_stops_to_segments.py`

* Formerly, this evaluates segments from all routes against all stops. Since it is a spatial join, this meant that infrequent routes running perpendicular to frequent routes often grabbed the cross street stop from the frequent route, creating an erroneous, isolated frequent segment and often an erroneous major transit stop.
* Now, we first filter segments to only segments from routes with at least one stop-level frequency meeting the standard. This screens out entirely infrequent routes and vastly reduces the risk of false positives.
* We have also reduced the segment to stop buffer (`SEGMENT_BUFFER_METERS`) to the extent possible.
