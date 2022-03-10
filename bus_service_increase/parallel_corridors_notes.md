## Highways

Figure out how to deal with highways.

So far, dissolved up to direction. Got rid of little segments that maybe occur with highway interchanges. Keep direction so far, need a way to aggregate EB/WB and NB/SB, because we want to treat it as one highway, but be able to select for various directions.

## Spatial Operation

* https://groups.google.com/g/geopandas/c/H_qzH2T5cCE
* https://geopandas.org/en/stable/docs/user_guide/set_operations.html
* https://gis.stackexchange.com/questions/332167/length-of-intersections-from-a-linestring-and-a-grid-shapefile-by-using-python-g

Don't use spatial join. Want to calculate *how much* of a route overlaps with a highway...to determine whether it's paralle or intersecting the highway.


## Criteria

* Bus route should be over a certain percentage, to be parallel bus route. >50%.
* It should also run a reasonable distance, some percentage, of the highway.
<br>Ex: Line 94 runs along 134, but it's only a small portion of the 134, whereas it might be a bigger portion of the 94.

Line 33: 
* https://moovitapp.com/index/en/public_transit-line-33-Los_Angeles_CA-302-1177-469126-0
* This seems correct, it's parallel to the 10 freeway

Line 605:
* https://moovitapp.com/index/en/public_transit-line-605-Los_Angeles_CA-302-1177-612551-0
* This one seems to intersect it, more perpendicular to the 10, but a large part falls within that 1 mile buffer

### Calculate orientation

* https://gis.stackexchange.com/questions/416316/compute-east-west-or-north-south-orientation-of-polylines-sf-linestring-in-r

### Sorting Routes into Parallel vs Intersecting Lines
* For each route, see what % intersects with what highways, so the highway it intersects with the most is the one you link the transit route to?
* Each highway is a corridor, and what routes are parallel to it? Iff not parallel, it's intersecting.
* For 10 fwy, have a group of transit lines that are parallel and ones that are intersecting.
* For each operator, show what % of lines is parallel, what % is intersecting.

### Motivation
No route should take more than twice as long than car route
1. Most efficient car route...how long that takes, origin to destination
1. How long does it take to drive the existing transit route. Start with the route...see how long it takes now, highlight areas of delay, but also show the minimum trip time (if bus went its top avg speed throughout).

Line 33 is good local bus, goes along Venice Blvd
Could you provide better end-to-end service for ppl who go between DTLA and Venice, by using 10 fwy.



[osrm](https://github.com/vaclavdekanovsky/data-analysis-in-examples/blob/master/Maps/Driving%20Distance/Driving%20Distance%20between%20two%20places.ipynb) -- can't be installed in Hub
[osmx part 1](https://towardsdatascience.com/driving-distance-between-two-or-more-places-in-python-89779d691def) -- nodes for bus stops snapping to same one
[osmx part 2](https://towardsdatascience.com/how-to-calculate-travel-time-for-any-location-in-the-world-56ce639511f)
[valhalla](https://github.com/valhalla/valhalla)
[valhalla demos](https://github.com/valhalla/demos)
[kuan's blog on calculating travel time](http://kuanbutts.com/2020/09/12/raptor-simple-example/)
[google-api-python-client](https://github.com/googleapis/google-api-python-client/blob/main/docs/batch.md)
[google python package](https://github.com/googlemaps/google-maps-services-python)