# High Quality Transit Areas Analysis

## Open Data Portal 

These datasets are updated **monthly**. We usually use a Wednesday in the middle of the month. The datasets reflect the a snapshot in time based on published GTFS schedule data.

1. [HQTA Areas](https://gis.data.ca.gov/datasets/863e61eacbf3463ab239beb3cee4a2c3_0) -- start here for any policy or planning analysis related to high quality transit.
1. [HQTA Stops](https://gis.data.ca.gov/datasets/f6c30480f0e84be699383192c099a6a4_0) -- use this to understand the transit stops that go into defining high quality transit areas.
1. [CA Transit Routes](https://gis.data.ca.gov/datasets/dd7cb74665a14859a59b8c31d3bc5a3e_0)
1. [CA Transit Stops](https://gis.data.ca.gov/datasets/900992cc94ab49dbbb906d8f147c2a72_0)

## Understanding the Open Data

Per statute, High Quality Transit Areas are defined as the _areas surrounding_ high quality transit corridors and major transit stops. Therefore, if you're looking for our most authoratative data on where HQTAs are within California, please start with the HQTA Areas (polygon) dataset.

Major transit stop is a more restrictive definition that high quality transit corridor. A high quality corridor only requires that there be service in that corridor meeting the frequency standard. It need not be one route alone meeting the frequency standard – therefore we cut corridors into segments and make the high quality corridor determination based on the stop with the most trips in that segment (even if those trips are on different routes). It is possible and valid for a high quality corridor to contain no major transit stops at all.

On the other hand, major transit stops exist at the intersection of two high-quality transit corridors as described above, and also at any rail station, most ferry terminals, and select BRT stations.

Using the HQTA Areas dataset, it's possible to determine if an area qualifies because it is a high quality corridor, major transit stop, or both. This is useful for certain kinds of analyses, since some statutes and programs only reference major transit stops and don't include high quality corridors.

We provide the HQTA Stops dataset as a convienience for certain kinds of analysis where it would be helpful to know actual stop locations. Note that the `hq_corridor_bus` type includes all stops in a high quality corridor. Since we make the high quality corridor determination at the corridor segment level and only then find the stops within that corridor, not every `hq_corridor_stop` is guaranteed to have frequent service (though they could all have frequent service, and at least one stop every 1,250 meters will have frequent service).

### Additional Details
* The AM peak period is defined as 12:00 AM - 11:59 AM. The PM peak period is defined as 12:00 PM - 11:59 PM.
    * Frequencies are calculated for each stop per hour. The highest frequency in the period before noon is counted for AM peak frequency; the highest frequency in the period after noon is counted for PM peak frequency.
* Corridors are cut for each operator-route-direction every 1,250 meters.
   * Stops have a 50 meter buffer drawn around them and are spatially joined to corridors.
   * For each 1,250 meter corridor, the highest frequency is counted (if there are multiple stops).
*  `peak_trips`: the number of trips per hour are calculated separately for the AM peak and PM peak. This column should be interpreted as: *at least x trips per hour in the peak period*. 
    * If these values differ, the *lesser* value is reported. 
    * Ex: 5 AM peak trips, 4 PM peak trips would show a value of `peak_trips = 4`.
* `hqta_type = hq_corridor_bus`
    * `hqta_details` is `corridor_frequent_stop` if the stop has at least 4 peak trips, `corridor_other_stop` otherwise.
* `hqta_type = major_stop_bus`
    * Major stop bus is designation of stops appearing at the intersection of 2 frequent corridors. In our analysis, corridors have an operator and route associated to track it.
    * If the intersection was between 2 different operators (different agency names), `hqta_details = intersection_2_bus_routes_different_operators`.
    * If the intersection was between routes of the same operator, `hqta_details = intersection_2_bus_routes_same_operator`
* A half-mile buffered dataset is provided for high quality transit areas. We recommend using the polygon dataset. To get the stops (points) that create these half mile polygons, we do also provide high quality transit stops.

[![hqta_methodology_mermaid](https://mermaid.ink/img/pako:eNqFkk1rwzAMhv-K8Ai5NNBLGWQw6FduY7CWnQxFjZXVkNhBVthK6X9fmqxry0h3sYX0SNjvq4PKvSGVqiRJtBMrJaXwWpODBQpCAvMprBldsAIr8XUAdAbefCMUtOuaouigHYB1VlLoQoBYdlRRnEK8xUDx6Dr7jmxxW1KIf_G2VLOtkPdzX3o-9T0sJ9kkm55bL8SavuRCjcfjv8jMsyEegkrraKgWKPfO3L4jyx6XsytGiMXeIEVRxH35eLra4xhF2mlXlP4z3yELrGdPPSFsWxGT5Bly3Eiv7IY7PX-IsMOaBpA7M8LJnfOIzql_gI3YigYoNVIVcYXWtKvRmaRVZ55WaRsaKrApRav2py2KjfjV3uUqFW5opJraoNDC4gdjdU6SseL5pd-2bumO3-XAyjk?type=png)](https://mermaid.live/edit#pako:eNqFkk1rwzAMhv-K8Ai5NNBLGWQw6FduY7CWnQxFjZXVkNhBVthK6X9fmqxry0h3sYX0SNjvq4PKvSGVqiRJtBMrJaXwWpODBQpCAvMprBldsAIr8XUAdAbefCMUtOuaouigHYB1VlLoQoBYdlRRnEK8xUDx6Dr7jmxxW1KIf_G2VLOtkPdzX3o-9T0sJ9kkm55bL8SavuRCjcfjv8jMsyEegkrraKgWKPfO3L4jyx6XsytGiMXeIEVRxH35eLra4xhF2mlXlP4z3yELrGdPPSFsWxGT5Bly3Eiv7IY7PX-IsMOaBpA7M8LJnfOIzql_gI3YigYoNVIVcYXWtKvRmaRVZ55WaRsaKrApRav2py2KjfjV3uUqFW5opJraoNDC4gdjdU6SseL5pd-2bumO3-XAyjk)

## High Quality Transit Areas Relevant Statutes

[PRC 21155](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=21155.&lawCode=PRC)
* Major transit stop definition: _A major transit stop is as defined in Section 21064.3, except that, for purposes of this section, it also includes major transit stops that are included in the applicable regional transportation plan_
* High-quality transit corridor definition: _For purposes of this section, a high-quality transit corridor means a corridor with fixed route bus service with service intervals no longer than 15 minutes during peak commute hours._
    * Statute does not define "peak commute hours", given that peaks may vary locally we look for any hour in the morning plus any hour in the afternoon/evening.

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

### HQTA Analysis

Prior to running the HQTA workflow, make sure this month's tables are already downloaded. Check `rt_dates` and the GCS bucket to see if `trips`, `stops`, `shapes`, and `stop_times` are already downloaded. 

If not, within the `gtfs_funnel` directory, run `make download_gtfs_data` in the terminal.

In terminal: `make hqta_data` to run through entire workflow.

1. [Compile rail, ferry, brt data](./A1_rail_ferry_brt_stops.py)
    * Sanity check: [check 1: downloads](./check1_downloads.ipynb)
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
    * `major_stop_bus`: the bus stop within the above intersection does not necessarily have the highest trip count
    * `hq_corridor_bus`: stops along the HQ transit corr (may not be highest trip count)
    * Sanity check: [check 2: hq corridors](./check2_hq_corridors.ipynb)
1. [Compile and export HQTA areas as points](./D1_assemble_hqta_points.py)
    * Sanity check: [check 3: hqta points](./check3_hqta_points.ipynb)
1. [Compile and export HQTA areas as polygons](./D2_assemble_hqta_polygons.py)
