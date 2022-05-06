# Parallel Corridors Analysis

Motivation: find where bus routes are considered "parallel" to the State Highway Network (SHN)

## Data

GTFS schedule data 

THIS_DATE_USED for trips Jan 6, 2022

## Methodology

**Parallel routes**: routes where at least 30% of the bus route falls within 1 mile of the SHN *and* cover at least 10% of the highway segment's length.

**Competitive routes**: routes where at least 75% of the trips take no longer than 2x a car. 

The idea is to take parallel routes and focus in on a set of competitive routes. An operator would not be able to improve bus service along all its parallel routes, but could more readily improve service frequency on the routes most competitive with car travel.

The fastest trip for the bus route is selected and a comparison is made to car travel (since each Google Directions API request costs money). The car is constrained to following every 3rd, 4th, or 5th bus stop as waypoints (depending on the route length), and travels at the same departure hour on the same day as the bus.

Relative to this car travel time, all the trips for a bus route is compared, and a ratio, the `bus_multiplier` is calculated. A ratio of 1 means that the bus trip takes the same amount of time as a car; a ratio of 2 means that the bus trip takes twice as long as the car. 

Notes:

* Google Directions API results vary by time-of-day, but not by day-of-week or month/seasons. Nevertheless, the car travels on the same day at the same departure hour as the bus, with the same origin, destination, and waypoints, for the best one-to-one comparison.
* A single request takes in origin, destination, and up to 25 waypoints. If the bus route is short enough, every 3rd bus stop is used as a waypoint. Every 4th or every 5th is used for longer routes, and 25 waypoints is always maxed out for these cases.
* The `duration_in_traffic` result is used. During light traffic hours, Google Direction API's `duration` and `duration_in_traffic` would not differ by much, but during a heavy traffic hours, `duration_in_traffic` better captures the traffic conditions for the bus.