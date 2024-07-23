# Methodology

## Data
* Download the data used in [GTFS Digest](https://console.cloud.google.com/storage/browser/calitp-publish-data-analysis) from our public Google Cloud Storage bucket. 
* Match the [readable column names](https://github.com/cal-itp/data-analyses/blob/main/gtfs_digest/readable.yml) to the table names.

## Route Typology
**NACTO Guidelines:** Transit [Route Types](https://nacto.org/publication/transit-street-design-guide/introduction/service-context/transit-route-types/) and 
[Frequency and Volume](https://nacto.org/publication/transit-street-design-guide/introduction/service-context/transit-frequency-volume/)

### Route Typology Methodology
Our methodology combines looking within GTFS `trips` and NACTO guidelines. 

#### NACTO Typology Implementation
The NACTO guidelines use a combination of transit characteristics that can be derived from GTFS (stop spacing, frequency), characteristics that are outside of GTFS (ridership), and street design characteristics (service area descriptors). Of the various descriptors, we focus on two characteristics that can be derived from GTFS:
* `stop_frequency` (number of stops per mile across all operators)
* `transit_frequency` (peak frequency across all operators)
   * *For decisions about street space and time allocation, the combined frequency of all routes is more significant than the frequency of any given route. Frequency is discussed here in the context of standard buses during peak periods.*

We sampled four dates, one in each quarter in the past year and found the combined peak period frequency of **all** transit routes that traveled along a two mile road segment.
* Transit routes (the most common shape / path) is selected for each route and spatially joined to two mile road segments.
* Any NACTO route classification over 10% of the transit route's path is kept, up to two types. 

An initial pass of classifying transit routes this way was insufficient. 
* Express routes were unable to be classified based on `stop_frequency` and `transit_frequency`.
* Rapid routes were severely undercounted, even for routes with names that are designated "rapid".
* Rail routes were also excluded from the classification.

#### GTFS and NACTO Implementation
For a more complete classification, we first looked within GTFS, specifically at the `route_short_name` and `route_long_name` the operators used. Since transit operators use these names to communicate with the public about what the route is supposed to be, we used that as a primary source.
* Any route with words like `express` or `limited` was categorized as `express`.
* Any route with `rapid` was categorized as `rapid`.
* Any route with `route_type` being rail was categorized as `rail`.
* All other routes were categorized as `local`.

Routes were then matched to their NACTO classification (potentially 2 options, with each option being at least 10% of the path). 
* `Local` routes had the opportunity to be categorized as `downtown_local`, `local`, or `coverage` (different local types depending on the road's peak service frequency). 

At this point, a route can be categorized with several typologies (held in dummy variables). 
* A rank is assigned for each typology, and we use the most generous designation and highest rank for the route.
* Typologies are ranked in this order: `coverage` (1), `local`, `downtown_local`, `express`, `rapid`, and `rail` (6)
* Recognizing that routes are nearly always classified with multiple typologies, we designate the routes according to their primary typology in GTFS Digest.

## GTFS Schedule and RT Pipeline
* More about the work for GTFS digest can be found [here](https://github.com/cal-itp/data-analyses/tree/main/gtfs_digest/).
* More about our pipeline can be found in this [diagram](https://github.com/cal-itp/data-analyses/tree/main/gtfs_funnel/README.md).

## Common Definitions
Official definitions of GTFS Schedule Data terms are [here](https://gtfs.org/schedule/reference/) and definitions of GTFS Realtime Data are [here](https://gtfs.org/realtime/reference/).<br>
* <b>All Day</b>: All of the trips that ran in a full 24 hour day.
* <b>AM Peak</b>: Defined as 7-9:59AM, when there is an anticipated surge of riders.
* <b>Average Scheduled Minutes</b>: The time an operator schedules for a trip to arrive from its origin to its destination.
* <b>Cardinal Direction</b>: Looking at the GPS coordinates of a trip, we determine which direction (Eastbound, Westbound, Northbound, or Southbound) the majority of the stops are heading towards.
* <b>Direction ID</b>: Per [gtfs.org](https://gtfs.org/schedule/reference/), this <i>indicates the direction of travel for a trip.</i>.
* <b>Journey</b>: Used interchangeably with <i>trip</i> in this analysis (see below).
* <b>Offpeak</b>: Summing all the trips that do not run during the AM or PM peak.
* <b>PM Peak</b>: Defined as 3-7:59PM, when there is an anticipated surge of riders.
* <b>Peak</b>: Summing all the of trips that run during the two peak AM and PM periods.
* <b>Route-Direction Frequency</b>: Taking the total trips for a route going one direction by the time period and dividing this total by how many hours are associated with the time period. For example, Route A runs 6 trips per hour going Southbound during the AM Peak. The frequency for this route-direction combination during the AM Peak would be 10 minutes.
* <b>Route-Direction</b>: A route travels two different directions. We analyze and present realtime data for a route and its two distinct directions.
* <b>Scheduled Service Hours</b>: The total number in hours of public transit service an operator is scheduled to provide. Due to the size of the data, we download a full week's worth of data twice a year in April and October for this metric.
* <b>Scheduled Shape (Path)</b>: Operators provide geographic data of where a route is scheduled to travel.
* <b>Spatial Accuracy</b>: We draw a buffer of 35 meters around the scheduled shape (path). We use this buffered shape to calculate the percentage of vehicle positions that fall in a reasonable distance of the scheduled route to understand the accuracy of the spatial data that is gathered.
* <b>Timeliness</b>: Subtracting the actual duration of a trip by its scheduled duration, we can categorize if a trip is early, late, or on-time. A trip is considered on-time if it arrives at its final destination within 5 minutes of its scheduled arrival time.
* <b>Trip</b>: A trip in <b>this context</b> is one instance of a route traveling in one particular direction from a set origin to a set destination, at a set time.
* <b>Vehicle Positions</b>: The coordinates of a bus's location collected by a GPS for GTFS data. The ideal benchmark is for a trip to collect 2 or more vehicle positions per minute across its total duration.

