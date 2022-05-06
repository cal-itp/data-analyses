# Parallel Corridors Analysis

**Motivation**: Identify the bus routes that are parallel to the State Highway Network (SHN). 

**Parallel routes**: routes where at least 30% of the bus route falls within 1 mile of the SHN *and* cover at least 10% of the highway segment's length.

In CA, nearly ___ % of bus routes are parallel.

Caltrans:
* If Caltrans were to sponsor an express bus, which highway corridors have no parallel transit routes?

Transit operators:
* Of the parallel bus routes, which ones are competitive against car travel and targeted for future service improvements? 
* Which highway corridors have somme parallel routes but few competitive routes?


## Data

* GTFS schedule data - trips on a typical weekday, Jan 6, 2022.
* State highway network

## Methodology

The idea is to take parallel routes and focus in on a set of competitive routes. An operator would not be able to improve bus service along all its parallel routes, but could more readily improve service frequency on the routes most competitive with car travel.

The fastest trip for the bus route is selected and a comparison is made to car travel (since each Google Directions API request costs money). The car is constrained to following every 3rd, 4th, or 5th bus stop as waypoints (depending on the route length), and travels at the same departure hour on the same day as the bus.

Relative to this car travel time, all the trips for a bus route is compared, and a ratio, the `bus_multiplier` is calculated. Actual bus service hours are available in GTFS schedule data. But, each bus route is associated with only one car travel time (Google Directions API). A ratio of 1 means that the bus trip takes the same amount of time as a car; a ratio of 2 means that the bus trip takes twice as long as the car. 

**Competitive routes**: routes where at least 75% of the trips take no longer than 2x a car. 

Within an operator, there is additional variability in the type of routes it serves. For a short route to take no more than an hour to make a trip, staying within the 2x car travel time means the route is completed within 2 hours. For a long route that takes 2 hours, staying within the 2x car travel time means the route is completed within 4 hours. 

Bus riders would not accept such a high discrepancy for travel time. Therefore, a new metric, `bus_difference` is calculated, showing the difference, in minutes, between bus and car travel time for that bus trip. 

The `bus_multiplier` metric is then paired with a `bus_difference` metric. At least 25% of the bus route's trips must be within this `bus_difference` threshold. For a short route, at least 25% of the bus route's trips cannot take longer than an additional 20 min compared to a car. These thresholds are used to show an operator its most viable competitive routes, and up to 15 routes are recommended for each route group.
* Short (< 1 hr): +20 min for bus
* Medium (1-1.5 hrs): +30 min
* Long (> 1.5 hrs): +40 min

Notes:

* Google Directions API results vary by time-of-day, but not by day-of-week or month/seasons. Nevertheless, the car travels on the same day at the same departure hour as the bus, with the same origin, destination, and waypoints, for the best one-to-one comparison.
* A single request takes in origin, destination, and up to 25 waypoints. If the bus route is short enough, every 3rd bus stop is used as a waypoint. Every 4th or every 5th is used for longer routes, and 25 waypoints is always maxed out for these cases.
* The `duration_in_traffic` result is used. During light traffic hours, Google Direction API's `duration` and `duration_in_traffic` would not differ by much, but during a heavy traffic hours, `duration_in_traffic` better captures the traffic conditions for the bus.