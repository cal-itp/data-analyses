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
Official definitions of GTFS Schedule Data terms can be found [here](https://gtfs.org/schedule/reference/) and definitions of GTFS Realtime Data are located [here](https://gtfs.org/realtime/reference/).
* Average Scheduled Minutes: The time an operator schedules for a trip to arrive from its origin to its destination. 
* Scheduled Service Hours: The total number of hours of public transit service an operator is scheduled to provide. We split this data on a weekday, Saturday, and Sunday level. Sunday service often coincides with holiday service.
* Timeliness: Subtracting the actual duration of a trip by its scheduled duration, we can categorize if a trip is early, late, or on time. A trip is considered on time if it arrives 5 minutes later or earlier than the scheduled times.  
* All Day: All of the trips that run in a full 24 hour day. 
* AM Peak: Defined as 7-9:59AM when there is an anticipated surge of riders.
* PM Peak: Defined as 3-7:59PM when there is an anticipated surge of riders.
* Peak: Summing all the trips that run during the two peak periods. 
* Offpeak: Summing all the trips that do not run during the AM or PM peak.
* Cardinal Direction: Looking at the GPS coordinates of a trip, we determine which direction (Eastbound, Westbound, Northbound, or Southbound) the majority of the stops are pointing to. 
* Direction ID: Per GTFS.org/schedule, <i>indicates the direction of travel for a trip.</i>. 
* Vehicle Positions: The coordinates of a bus's location collected by a GPS for GTFS data. Ideally, a vehicle position should be captured at least twice per minute because this reflects a higher density of data collection.
* Scheduled Shape (Path): Operators provide geographic data of where a route is scheduled to travel.
* Spatial Accuracy: We draw a buffer of 35 meters around the scheduled shape. We use this buffered shape to calculate the percentage of vehicle positions that fall in a reasonable distance of the scheduled route. This allows us to gauge the accuracy of the geographic component of the collected GTFS data. 
* Trip: A trip in <b>this portfolio</b> is one instance of a route traveling in one particular direction from a set origin to a set destination, at a set time. For example, one trip of Route A goes Eastbound and the other trip of Route A goes Westbound. Route A is scheduled to go Eastbound every 30 minutes during the AM Peak, so Trip 1 leaves at 7AM and the last trip, Trip 6 leaves at 9:30AM. 
* Route-Direction Frequency: Taking the total trips for a route going one direction by the time period (peak and offpeak) and dividing by how many hours are associated with the time period. For example, there are 100 trips in a day for  Route B. The peak time period is considered 7-9:59AM and 3-7:59PM, a total of about 8 hours. If 40 of those trips are scheduled during these peak periods, we would divide 40 by 8. A bus heading the first direction passes by five times per hour, so a bus comes by about every 12 minutes. 
* Route-Direction: A route travels two different directions. We analyze and present realtime data on the route on the direction level. You will see that many of the charts are split between Cardinal Direction or Direction ID's of 0/1 respectively. 
* Journey: used interchangeably with <i>trip</i>. 

## Common Questions

**Why is time-series table sampling single days?**

GTFS provides us with extremely detailed information, such as the time a bus is scheduled to arrive at a stop, and the GPS coordinates of a bus at a given timestamp. When working with granular data like this, a single day statewide can be a very large table.

For context, on our sampled date in January 2024 there were 100k+ trips and 3.6 million+ stop arrivals, and that's just scheduled data. Our vehicle positions table, after deduplicating in our warehouse, had 15 million+ rows. On top of that, each operator can have a quartet of GTFS data (1 schedule table + 3 real-time tables).

Getting our pipeline right is fairly complex for a single day. Our warehouse has a set of internal keys to ensure we're matching trip for trip across quartets. If you factor in the fact that operators can update their GTFS feeds at any time in the month, there are a lot of things that are changing!

We do have monthly aggregations on our roadmap, but for now, we're building out our own time-series tables of processed data, and working through the kinks of being able to track the same route over time (as feeds get updated, identifiers change, etc). We will be starting with schedule data to figure out how to produce monthly aggregations in a scalable way.

**How does GTFS Digest fit into SB 125 performance metrics?**

[SB 125](https://calsta.ca.gov/subject-areas/sb125-transit-program) and the creation of the Transit Transformation Task Force has a section on creating performance metrics for transit operators statewide. Dive into the [legislative bill](https://legiscan.com/CA/text/SB125/id/2831757).

The Caltrans Division of Data & Digital Services has been ingesting and collecting GTFS data in our warehouse since 2021. Our own internal effort has been to create data pipelines so that the rich and comprehensive data we collect can be processed and made available for public consumption. 

There overlaps with the goals of SB 125. There are a set of performance metrics that could be of interest to the task force, the public, and us! However, GTFS Digest is a **GTFS** digest, which means its primary focus is on metrics that can be derived purely from GTFS, and to do it statewide so we can understand transit operator performance. We based a lot of our metrics on the papers by [Professor Gregory Newmark](https://www.morgan.edu/sap/gregory-newmark) that gave us a roadmap of metrics that could be derived solely from GTFS that would create comparisons of transit operators regardless of size, service area and density. 

GTFS Digest will continue to evolve as we dive into our own warehouse!