# Competitive Transit Corridors near the State Highway Network

**Motivation**: Identify the most competitive bus routes near the State Highway Network (SHN).

## Important Definitions

The Methodology section explains why these definitions are needed. 

**Routes on SHN**: routes where at least 20% of the bus route takes place on the SHN (within a 50 ft buffer).

**Routes Intersecting SHN**: routes where at least 35% of the bus route falls within 0.5 mile of the SHN. 

**Routes Near SHN**: the sum of routes on the SHN and intersecting the SHN. In some way, these routes are impacted by the SHN, either through traffic conditions or simply because they pass through freeway on-ramps and underpasses.

**Competitive routes**: routes where at least 50% of the trips take no longer than 1.5x a car. A multiplier of 1 means that the bus and the car take the same amount of time. A multiplier of 1.5 means that the bus takes 50% longer than the car.


Caltrans perspective:
* If Caltrans were to sponsor an express bus, which highway corridors have [no transit routes within 2 miles](https://docs.calitp.org/data-analyses/bus_service_increase/img/highways-no-parallel-routes.html)?
* Which highway corridors have [no competitive transit](https://docs.calitp.org/data-analyses/bus_service_increase/img/highways-uncompetitive-routes.html) (looser definition of within 2x of car travel) within 2 miles?

Transit operator perspective:
* Of the bus routes near the SHN, which ones are competitive against car travel and should be targeted for future service improvements? 
* Operators with GTFS Real-Time feeds: explore these competitive bus routes and [see where the bottlenecks are](https://analysis.calitp.org/rt/README.html).


## Data

* GTFS schedule data - trips on a typical weekday, 5/4/22.
* State highway network
* [Data catalog](https://github.com/cal-itp/data-analyses/blob/main/bus_service_increase/catalog.yml)
* [Data cleaning scripts](https://github.com/cal-itp/data-analyses/blob/main/bus_service_increase/README_analysis.md) 

## Methodology
### Identifying Competitive Routes 

The idea is to take routes near and intersecting the SHN and narrow in on a set of competitive routes. An operator would not be able to improve bus service along all its routes, but could more readily improve service frequency on the routes most competitive with car travel.

A faster trip (25th percentile) for the bus route is selected and a comparison is made to car travel (since each Google Directions API request costs money). The car is constrained to following every 3rd, 4th, or 5th bus stop as waypoints (depending on the route length), and travels at the same departure hour on the same day as the bus.

Relative to this car travel time, all the trips for a bus route is compared, and a ratio, the `bus_multiplier` is calculated. Actual bus service hours are available in GTFS schedule data. But, each bus route is associated with only one car travel time (Google Directions API). A ratio of 1 means that the bus trip takes the same amount of time as a car; a ratio of 2 means that the bus trip takes twice as long as the car. 

**Competitive routes**: routes where at least 50% of the trips take no longer than 1.5x than a car. 

**For each route group, the operator must have at least 2 routes, and up to 15 routes are recommended.** 

### Notes

* Google Directions API results vary by time-of-day, but not by day-of-week or month/seasons. Nevertheless, the car travels on the same day at the same departure hour as the bus, with the same origin, destination, and waypoints, for the best one-to-one comparison.
* A single request takes in origin, destination, and up to 25 waypoints. If the bus route is short enough, every 3rd bus stop is used as a waypoint. Every 4th or every 5th is used for longer routes, and 25 waypoints is always maxed out for these cases.
* The `duration_in_traffic` result is used. During light traffic hours, Google Direction API's `duration` and `duration_in_traffic` would not differ by much, but during a heavy traffic hours, `duration_in_traffic` better captures the traffic conditions for the bus.