# High Quality Transit Areas Analysis

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

### Download Data
In terminal: `make download_hqta_data`

1. [Download data](./download_data.py) and cache parquets in GCS
1. [Create JSONs](./operators_for_hqta.py) storing a dictionary with [all the operators](./hqta_operators.json) that have cached files, and [valid operators](./valid_hqta_operators.json). 
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

1. [Draw bus corridors, from routes to HQTA segments](./B1_bus_corridors.py)
    * Every segment is 1,250 m. 
    * Attach number of stop arrivals that occur in the AM and PM and find the max
1. [Combine operator HQTA areas across operators](./B2_combine_operator_corridors.py)
1. [Prep for clipping](./C1_prep_for_clipping.py) 
    * Find which routes actually do intersect, and store that in a pairwise table.
    * Only these valid routes go on to do the clipping
1. [Find where corridors intersect by clipping](./C2_clip_bus_intersections.py)
1. [Create datasets for each of the hqta types](./C4_create_bus_hqta_types.py)
    * `major_stop_bus`: the bus stop within the above intersection does not necessarily have
the highest trip count
    * `hq_corridor_bus`: stops along the HQ transit corr (may not be highest trip count)
    * Sanity check: [check 2: hq corridors](./check2_hq_corridors.ipynb)

### Export Data

In terminal: `make export_data`

1. [Compile and export HQTA areas as points](./D1_assemble_hqta_points.py)
    * Sanity check: [check 3: hqta points](./check3_hqta_points.ipynb)
1. [Compile and export HQTA areas as polygons](./D2_assemble_hqta_polygons.py)