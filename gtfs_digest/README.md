# General Transit Feed Specification (GTFS) Digest
The goal of this website is to give you an overview of transit operators that produce GTFS schedule and/or real-time data either on the individual operator, Caltrans district, or legislative district level.

We use data from the [National Transit Database](https://www.transit.dot.gov/ntd), [National Association of City Transportation Official’s Transit Route Types](https://nacto.org/publication/transit-street-design-guide/introduction/service-context/transit-route-types/), and [GTFS feeds](https://gtfs.org/) to deliver key insights. You can find details such as the types of routes and the total scheduled hours of public transit service for which an operator runs.

For operators who produce real-time data, we also calculate additional performance metrics for all their routes. Examples include displaying the number of on-time, early, and late trips, the average speed, and the headway for a route.

GTFS Digest will continue to evolve as we dive into our own data warehouse!

## Definitions and Methodology
To read about the methodology behind and the definitions of terms used throughout our work, please visit [here](https://github.com/cal-itp/data-analyses/blob/main/gtfs_digest/methodology.md).

## Frequently Asked Questions
**Why are the time-series tables sampling single days?**
GTFS provides us with extremely detailed information, such as the time a bus is scheduled to arrive at a stop, and the GPS coordinates of a bus at a given timestamp. When working with granular data like this, a single day statewide can be a very large table.

For context, on our sampled date in January 2024 there were 100k+ trips and 3.6 million+ stop arrivals, and that's just scheduled data. Our vehicle positions table genereated in real-time, after deduplicating in our warehouse, had 15 million+ rows. On top of that, each operator can have a quartet of GTFS data (1 schedule table + 3 real-time tables).

Getting our pipeline right is fairly complex for a single day. Our warehouse has a set of internal keys to ensure we're matching trip for trip across quartets. If you factor in the fact that operators can update their GTFS feeds at any time in the month, there are a lot of things that are changing!

We do have monthly aggregations on our roadmap, but for now, we're building out our own time-series tables of processed data, and working through the kinks of being able to track the same route over time (as feeds get updated, identifiers change, etc). We will be starting with schedule data to figure out how to produce monthly aggregations in a scalable way.

## Data Sources
Per [GTFS.org](https://gtfs.org/documentation/overview/), GTFS contains both realtime and schedule components.
* Realtime data consists of *"...is composed of a collection of simple files, mostly text files (.txt) that are contained in a single ZIP file. Each file describes a particular aspect of transit information such as stops, routes, trips, etc. At its most basic form, a GTFS Schedule dataset is composed of 7 files: agency.txt, routes.txt, trips.txt, stops.txt, stop_times.txt, calendar.txt and calendar_dates.txt"*
* Schedule data consists of *"...allows public transportation agencies to provide up-to-date information about current arrival and departure times, service alerts, and vehicle position, allowing users to smoothly plan their trips. The specification currently supports the following types of information:*

    - *Trip updates - delays, cancellations, changed routes*
    - *Service alerts - stop moved, unforeseen events affecting a station, route or the entire network*
    - *Vehicle positions - information about the vehicles including location and congestion level"*

The GTFS Digest is comprised of four major datasets. The processing of the datasets is detailed below.
**To come**
To download all of the processed data that powers this portfolio, please navigate to the folder titled `gtfs_digest` [here](https://console.cloud.google.com/storage/browser/calitp-publish-data-analysis). You will find the most recent datasets in `.parquet, .csv,.geojson` formats. The data pulled from the Federal Transit Administration's National Transit Data is located [here](https://www.transit.dot.gov/ntd/data-product/2022-annual-database-agency-information).

## Who We Are
This website was created by the [California Department of Transportation](https://dot.ca.gov/)'s Division of Data and Digital Services. We are a group of data analysts and scientists who analyze transportation data, such as General Transit Feed Specification (GTFS) data, or data from funding programs such as the Active Transportation Program. Our goal is to transform messy and indecipherable original datasets into usable, customer-friendly products to better the transportation landscape. For more of our work, visit our [portfolio](https://analysis.calitp.org/).

<img src="https://raw.githubusercontent.com/cal-itp/data-analyses/main/portfolio/templates/assets/Calitp_logo_MAIN.png" alt="Alt text" width="200" height="100"> <img src="https://raw.githubusercontent.com/cal-itp/data-analyses/main/portfolio/templates/assets/CT_logo_Wht_outline.gif" alt="Alt text" width="129" height="100">

<br>Caltrans®, the California Department of Transportation® and the Caltrans logo are registered service marks of the California Department of Transportation and may not be copied, distributed, displayed, reproduced or transmitted in any form without prior written permission from the California Department of Transportation.
