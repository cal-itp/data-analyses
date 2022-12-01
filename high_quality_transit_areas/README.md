# High Quality Transit Areas Analysis

## Open Data Portal 
1. [HQTA Areas](https://gis.data.ca.gov/datasets/863e61eacbf3463ab239beb3cee4a2c3_0)
1. [HQTA Stops](https://gis.data.ca.gov/datasets/f6c30480f0e84be699383192c099a6a4_0)
1. [CA Transit Routes](https://gis.data.ca.gov/datasets/dd7cb74665a14859a59b8c31d3bc5a3e_0)
1. [CA Transit Stops](https://gis.data.ca.gov/datasets/900992cc94ab49dbbb906d8f147c2a72_0)

## High Quality Transit Areas Relevant Statutes

[PRC 21155](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=21155.&lawCode=PRC)
* Major transit stop definition: _A major transit stop is as defined in Section 21064.3, except that, for purposes of this section, it also includes major transit stops that are included in the applicable regional transportation plan_
* High-quality transit corridor definition: _For purposes of this section, a high-quality transit corridor means a corridor with fixed route bus service with service intervals no longer than 15 minutes during peak commute hours._
    * Unable to locate definition of "peak commute hours"

[PRC 21064.3](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=21064.3.&lawCode=PRC)
* _Major transit stop means a site containing any of the following:
(a) An existing rail or bus rapid transit station.
(b) A ferry terminal served by either a bus or rail transit service.
(c) The intersection of two or more major bus routes with a frequency of service interval of 15 minutes or less during the morning and afternoon peak commute periods._
    * "Intersection" may not be sufficiently well-defined for this analysis

[PRC 21060.2](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=PRC&sectionNum=21060.2.&highlight=true&keyword=bus%20rapid%20transit)
* _(a) “Bus rapid transit” means a public mass transit service provided by a public agency or by a public-private partnership that includes all of the following features:
(1) Full-time dedicated bus lanes or operation in a separate right-of-way dedicated for public transportation with a frequency of service interval of 15 minutes or less during the morning and afternoon peak commute periods.
(2) Transit signal priority.
(3) All-door boarding.
(4) Fare collection system that promotes efficiency.
(5) Defined stations._
    * Unlikely to determine if a service qualifies as BRT under this definition using GTFS alone
    
## Workflow

HQTA data is updated at a monthly frequency for the open data portal. Check the [Makefile](./Makefile) for the most up-to-date order to run the scripts. Run the `Makefile` commands from within the `high_quality_transit_areas` directory.

### Document Monthly Variable

1. Pick a Wednesday in the of the month and put this in [rt_dates](../_shared_utils/shared_utils/rt_dates.py). Update the `DATES` and `METADATA_EDITION` variables.
1. Update this month's `analysis_date` in [update_vars](./update_vars.py)

### Download Data

In terminal: `make download_hqta_data`

1. [Download data](./download_data.py) and cache parquets in GCS
1. [Create JSONs](./operators_for_hqta.py) storing a dictionary with all the operators that have cached files for all 4 datasets. These are the [valid operators](./valid_hqta_operators.json). 
    * It's possible in downloading the `routes`, `trips`, `stops`, and `stop_times`, one of the files is not able to be successfully downloaded, but the other 3 are. 
    * Also, it's possible that files with no rows are downloaded.
    * A check for complete information (all 4 files are present and are non-empty)
    * These valid operators are the ones that continue on in the HQTA workflow.
1. [Compile and cache datasets for all operators](./compile_operators_data.py)
    * To add `route_id` based on `stop_id`, it requires `trips` and `stop_times` intermediary tables. Need it compiled across operators, because we don't know which `stop_id` values are present by the end.
1. [Download rail, ferry, brt data](./A2_combine_stops.py)
    * Sanity check: [check 1: downloads](./check1_downloads.ipynb)

### Bus Corridor Intersections

In terminal: `make grab_corridors_and_clip`

1. [Draw bus corridors, from routes to HQTA segments](./B1_create_hqta_segments.py)
    * Across all operators, find the longest shapes in each direction. Use a symmetric difference to grab the components that make up the route network.
    * Cut route into HQTA segments. Every segment is 1,250 m. 
    * Add in route direction.
1. [Combine operator HQTA areas across operators](./B2_sjoin_stops_to_segments.py)
    * Attach number of stop arrivals that occur in the AM and PM and find the max
    * Do spatial join of stops to HQTA segments. Where multiple stops are present, keep the stop with the highest number of trips.
1. [Use pairwise table to store which segments intersect](./C1_prep_pairwise_intersections.py) 
    * Find which routes actually do intersect, and store that in a pairwise table.
1. [Find where corridors intersect](./C2_get_intersections.py)
1. [Create datasets for each of the hqta types](./C3_create_bus_hqta_types.py)
    * `major_stop_bus`: the bus stop within the above intersection does not necessarily have
the highest trip count
    * `hq_corridor_bus`: stops along the HQ transit corr (may not be highest trip count)
    * Sanity check: [check 2: hq corridors](./check2_hq_corridors.ipynb)

### Export Data

In terminal: `make export_data`

1. [Compile and export HQTA areas as points](./D1_assemble_hqta_points.py)
    * Sanity check: [check 3: hqta points](./check3_hqta_points.ipynb)
1. [Compile and export HQTA areas as polygons](./D2_assemble_hqta_polygons.py)