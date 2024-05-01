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