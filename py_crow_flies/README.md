# Hackathon 2023 - Speeding up bike accessibility script

[GH Epic](https://github.com/cal-itp/data-analyses/issues/698)

Henry McKay's measuring bike accessibility [script here](https://github.com/hhmckay/Ideal-Access-Metric/blob/main/CrowsFlyWeighted.R) takes 2-3 weeks to run. Let's speed it up.

## Summary of current script
* take an origin point of interest (POI) and draw a 20 mile buffer
* find where the origin intersects with other destinations
   * destinations: subset of origins who have non-zero opportunities (`grid_code > 0`)
* calculate distance between origin and destination (distance as the crow flies / straight line between 2 points)
* apply decay function to weigh opportunities closer more and opportunities further less
   * the opportunities an origin that intersects with itself would be counted in full. The exponential decay function currently returns the full value when the travel time parameter, or distance is 0
* sum up the weighted opportunities by origin
* set the origins with no opportunities to zero (do not leave as missing value)

## Guidelines
* One week to work on this individually (3/27/23 - 4/4/23)
* Bonus points for completing it in R
* Optional participation
* Make a PR and link this epic

## Resources
* GCS bucket: `gs://calitp-analytics-data/data-analyses/py_crow_flies/`
* 4 geoparquet files contained the points of interests (POIs) by region. these have point geometries.