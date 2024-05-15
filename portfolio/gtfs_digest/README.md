# GTFS Digest
This portfolio houses performance metrics from GTFS schedule and vehicle positions time-series data for all transit operators by route.

To download our processed full data that powers this portfolio, please navigate to the folder titled `gtfs_digest` [here](https://console.cloud.google.com/storage/browser/calitp-publish-data-analysis). You will find the most recent datasets in `.parquet, .csv,.geojson` formats. Match the [readable column names](https://github.com/cal-itp/data-analyses/blob/main/gtfs_digest/readable.yml) to the table names. The data pulled from the Federal Transit Administration's National Transit Data is located [here](https://www.transit.dot.gov/ntd/data-product/2022-annual-database-agency-information). 
## Common Questions
<b>To read about the methodology, please visit [here](https://github.com/cal-itp/data-analyses/blob/main/gtfs_digest/methodology.md).</b></br>
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
