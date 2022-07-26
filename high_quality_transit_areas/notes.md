# To Do

## Route Network
* 1 shape is really fast, but it's known to miss one-way streets, buses traveling in DTLA, by direction
* Dissolving results in different ordering of points
* Need to figure out how to correctly order points, otherwise, HQTA segments will be very choppy, and the result is spotty/gaps, instead of plotting the route as 1 line
* Even picking top 5 shape_ids by length for each route results in this
* `gpd.simplify(tolerance=10)` didn't really help, but probably did simplify the array of points in the multipolygon, but it didn't result in less choppy segments.

## Pairwise
* clipping C2 now takes 3 min
* Right now, add table, which works a lot faster
* TODO: fix the clipping, break apart by north-south vs east-west? Otherwise, the masking df used for the clip will keep 

## Intersections
* Draw buffer, then find all the stops that fall into that intersection
* Once exploded, tag as: `hqta_type = major_transit_stop`
* What is the difference in this than above? Are these the actual stops that met the qualification with > 4 AM / PM trips? `hqta_type = major_stop_bus`

## Stops Along Corridor
* These may not be the stops that have the highest trips, but they are stops that fall in a HQ corridor
* `hqta_type = hq_corridor_bus`


# A1 / A2 scripts
* These are the ones that came from A1, A2, most straightforward case
* `hqta_type = major_stop_rail / major_stop_brt, major_stop_ferry`