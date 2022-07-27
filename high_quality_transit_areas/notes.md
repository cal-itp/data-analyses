# To Do

## Route Network
* 1 shape is really fast, but it's known to miss one-way streets, buses traveling in DTLA, by direction
* Dissolving results in different ordering of points
* Need to figure out how to correctly order points, otherwise, HQTA segments will be very choppy, and the result is spotty/gaps, instead of plotting the route as 1 line
* Even picking top 5 shape_ids by length for each route results in this
* `gpd.simplify(tolerance=10)` didn't really help, but probably did simplify the array of points in the multipolygon, but it didn't result in less choppy segments.

## Intersections
* Draw buffer, then find all the stops that fall into that intersection
* Once exploded, tag as: `hqta_type = major_transit_stop`
* What is the difference in this than above? Are these the actual stops that met the qualification with > 4 AM / PM trips? `hqta_type = major_stop_bus`

## Stops Along Corridor
* These may not be the stops that have the highest trips, but they are stops that fall in a HQ corridor
* `hqta_type = hq_corridor_bus`

# Combine all the points (do all these in D scripts)
## Rail / Ferry / BRT
* These are the ones that came from A1, A2, most straightforward case
* `hqta_type = major_stop_rail / major_stop_brt, major_stop_ferry`

## Other Bus Types
* `hq_corridor_bus` (done)
* `major_transit_stop` vs `major_stop_bus` are these distinct or the same?

# Combine all the polygons
