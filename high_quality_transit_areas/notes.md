# To Do

## Route Network
* 1 shape is really fast, but it's known to miss one-way streets, buses traveling in DTLA, by direction
* Dissolving results in different ordering of points
* Need to figure out how to correctly order points, otherwise, HQTA segments will be very choppy, and the result is spotty/gaps, instead of plotting the route as 1 line
* Even picking top 5 shape_ids by length for each route results in this
* `gpd.simplify(tolerance=10)` didn't really help, but probably did simplify the array of points in the multipolygon, but it didn't result in less choppy segments.
* Possibly just taking longest `shape_id` from both directions directly from `routelines` when it's imported could work, and change `route_id` to `shape_id` downstream

# Combine all the points / polygons
* Done, except writing to `geojsonl`, `geojson` in its GCS sub-folder. Right now, still writes out geoparquet.

# Organization
* Clean up all the scripts, retire old functions, move `dask_utils` into `utilities`? 

# Logs
* Create logs to check that certain things run successfully with INFO
* Make sure hitting BigQuery query limit shows an error (it runs successfully in a script)
* https://www.machinelearningplus.com/python/python-logging-guide/
* https://github.com/Delgan/loguru