# General Transit Feed Specification (GTFS) Digest
The goal of this website is to give you an overview of transit operators that produce GTFS schedule and/or real-time data. We use data from the [National Transit Database](https://www.transit.dot.gov/ntd), [National Association of City Transportation Officials's Transit Route Types](https://nacto.org/publication/transit-street-design-guide/introduction/service-context/transit-route-types/), and [GTFS feeds](https://gtfs.org/) to deliver key insights. You can find details such as the types of routes and the total scheduled hours of public transit service for which an operator runs.

For operators who produce real-time data, we also calculate additional performance metrics for all of their routes. Examples include displaying the number of on-time, early, and late trips, the average speed, and the headway for a route.

GTFS Digest will continue to evolve as we dive into our own data warehouse!

## Definitions and Methodology
To read about the methodology behind and the definitions of terms used throughout our work, please visit [here](https://github.com/cal-itp/data-analyses/blob/main/gtfs_digest/methodology.md).

## Frequently Asked Questions
**Why are the time-series tables sampling single days?**
GTFS provides us with extremely detailed information, such as the time a bus is scheduled to arrive at a stop, and the GPS coordinates of a bus at a given timestamp. When working with granular data like this, a single day statewide can be a very large table.

For context, on our sampled date in January 2024 there were 100k+ trips and 3.6 million+ stop arrivals, and that's just scheduled data. Our vehicle positions table genereated in real-time, after deduplicating in our warehouse, had 15 million+ rows. On top of that, each operator can have a quartet of GTFS data (1 schedule table + 3 real-time tables).

Getting our pipeline right is fairly complex for a single day. Our warehouse has a set of internal keys to ensure we're matching trip for trip across quartets. If you factor in the fact that operators can update their GTFS feeds at any time in the month, there are a lot of things that are changing!

We do have monthly aggregations on our roadmap, but for now, we're building out our own time-series tables of processed data, and working through the kinks of being able to track the same route over time (as feeds get updated, identifiers change, etc). We will be starting with schedule data to figure out how to produce monthly aggregations in a scalable way.

**How does GTFS Digest fit into SB 125 performance metrics?**

[SB 125](https://calsta.ca.gov/subject-areas/sb125-transit-program) and the creation of the Transit Transformation Task Force has a section on creating performance metrics for transit operators statewide. Dive into the [legislative bill](https://legiscan.com/CA/text/SB125/id/2831757).

The Caltrans Division of Data & Digital Services has been ingesting and collecting GTFS data in our warehouse since 2021. Our own internal effort has been to create data pipelines so that the rich and comprehensive data we collect can be processed and made available for public consumption. 

There are overlaps with the goals of SB 125. There are a set of performance metrics that could be of interest to the task force, the public, and us! However, GTFS Digest is a **GTFS** digest, which means its primary focus is on metrics that can be derived purely from GTFS, and to do it statewide so we can understand transit operator performance. We based a lot of our metrics on the papers by [Professor Gregory Newmark](https://www.morgan.edu/sap/gregory-newmark) that gave us a roadmap of metrics that could be derived solely from GTFS that would create comparisons of transit operators regardless of size, service area and density. 

## Data Sources
The GTFS Digest is comprised of four major datasets. The processing of the datasets is detailed below.
[![mermaid_diagram1](https://mermaid.ink/img/pako:eNqlVmtv4jgU_StWVhVUCxQooSUjrTSQgf0w3UdhdqUto8pNnMQaY0e2M21a9b_vtU2ApLAzu_sFEvvch-8598YvXiRi4gVet9tdc001IwFarOZLFNKUKI3W3O6cnb2sOUKUUx0g-4hQS2dkQ1oBaj1gRVqdw9U_sKT4gRHV2sFhK5d0g2U5E0xIY_fDB3_uz99XpnvEijzpParf77-FTIWMiTwFYpSTU3uKRILH9Tzm86sP0wOMJlLTGiRJkpbbfjV_8PN6drbma54w8RhlWGr08dYBVPGQSpxnrpC_CakTwahAnzRlVJdoXvBIU8GVg88Hd6lO1H1sK35xD_mZ3cF9AXjVy8vPQRDECep2f9oaDNuhIAo9oRJhHqNnpDPKU4W0OD9AXd4tJKYcDZz9O5Os2Vgcjzc8GW_xXfEWjXiVLQAWo-3WcJeK2SPgyz016vaJA7VKQywIA8EIsuYKPRAIHLuVvUgr1hCKGFYqJAmC9BPKWLBV2BGEiiTNdYWy_B9BOWIdaD6bjvof9qD3g7tfcyKxFtKWZUnkVxoRFGJN3Dnh6GiPtzbD9mHtN0Sm5F5svdzHWGOo_zkYb9M74uGyvdQ4-qJsFYyFIlohkaCdm1wKyBgIq1uaHCMplHrEDMwfqc7QL6vQ-kAJHKIBZ8yGUNtjAYwYyoGESGwI0nRjNyms2sQb9jfmbC7L4RuwzVmLlMC27J3vBNc87LjdXgmNGboVhQn_UKJVmUNTpuW5sbL8vNsbTQ846dYYgVfr4_OpWNP_Tc30ODU6wxrByNEg4bqBgUmTFFIZzuF4hgWzWIfVGTA02t7AMqbPsOA8cLwhqtOwBKpwoYjjEQAO21IWjSLMjcB5SpD4SqQlqXfQ0btzjdrtj4Lb78GPaJnBQDOPtpx7Fpq1sAWa-u32Dc6NPC0cLYiwPQ5COErgbE9g909CvlwssS5kjMuLZQFHLrs_i0Ke5HD2Txx-i77ZZXsO9VdRRuKCERgzAISSwrBT1bhQjl_ojbrt0U5x6jYtE0liJIgP-6DZL6uM8G2ww26BV5ymkqTgIDb6hyIgXeaks2v4zpFGz6BMJjLkAH-mg_bnqvI0GHXQfI1yAOshpqxEy51l1VB1pKHEadcwBvmdEsXMiWLm_3vPlQy-5Xr8H1xbYR1VY3igRqdflcMn3NAQUum-m923c_-IuMJjyvyeoRLWhsqbYeyU1eD_UECd08OibpVQjpnFmECRMYAFFFfnBB1urVPCoSaMlXUHGcGxyekRTCFsTOAStYF7WBXUiraRaZ4DVbuP0ukPQQhi_L3A9vq0myWGvFtqrgqnVBE6VYR-3ZpgZivUAJvvYCQYg_OCbIz37YnfaANuLtvblPmr7jDr3XjfO3ar7t3reMD7BtMYLtz2Vrz27G157QXwGJMEF0yvPbhaAhQXWixLHnmBlgXpeJBJmnlBgpmCtyI3Eggphmm6qSAkpqDUG3ejtxf7jpdj7gUv3pMXXPV7o7E_Gk6Gw_7A9y87XukFg-Gkdzm4Glz7o8l4MvCvXjvesxDgctC7nkyu-0N_7PtXo_FobH39ZfdMuNe_AaRWsro?type=png)](https://mermaid.live/edit#pako:eNqlVmtv4jgU_StWVhVUCxQooSUjrTSQgf0w3UdhdqUto8pNnMQaY0e2M21a9b_vtU2ApLAzu_sFEvvch-8598YvXiRi4gVet9tdc001IwFarOZLFNKUKI3W3O6cnb2sOUKUUx0g-4hQS2dkQ1oBaj1gRVqdw9U_sKT4gRHV2sFhK5d0g2U5E0xIY_fDB3_uz99XpnvEijzpParf77-FTIWMiTwFYpSTU3uKRILH9Tzm86sP0wOMJlLTGiRJkpbbfjV_8PN6drbma54w8RhlWGr08dYBVPGQSpxnrpC_CakTwahAnzRlVJdoXvBIU8GVg88Hd6lO1H1sK35xD_mZ3cF9AXjVy8vPQRDECep2f9oaDNuhIAo9oRJhHqNnpDPKU4W0OD9AXd4tJKYcDZz9O5Os2Vgcjzc8GW_xXfEWjXiVLQAWo-3WcJeK2SPgyz016vaJA7VKQywIA8EIsuYKPRAIHLuVvUgr1hCKGFYqJAmC9BPKWLBV2BGEiiTNdYWy_B9BOWIdaD6bjvof9qD3g7tfcyKxFtKWZUnkVxoRFGJN3Dnh6GiPtzbD9mHtN0Sm5F5svdzHWGOo_zkYb9M74uGyvdQ4-qJsFYyFIlohkaCdm1wKyBgIq1uaHCMplHrEDMwfqc7QL6vQ-kAJHKIBZ8yGUNtjAYwYyoGESGwI0nRjNyms2sQb9jfmbC7L4RuwzVmLlMC27J3vBNc87LjdXgmNGboVhQn_UKJVmUNTpuW5sbL8vNsbTQ846dYYgVfr4_OpWNP_Tc30ODU6wxrByNEg4bqBgUmTFFIZzuF4hgWzWIfVGTA02t7AMqbPsOA8cLwhqtOwBKpwoYjjEQAO21IWjSLMjcB5SpD4SqQlqXfQ0btzjdrtj4Lb78GPaJnBQDOPtpx7Fpq1sAWa-u32Dc6NPC0cLYiwPQ5COErgbE9g909CvlwssS5kjMuLZQFHLrs_i0Ke5HD2Txx-i77ZZXsO9VdRRuKCERgzAISSwrBT1bhQjl_ojbrt0U5x6jYtE0liJIgP-6DZL6uM8G2ww26BV5ymkqTgIDb6hyIgXeaks2v4zpFGz6BMJjLkAH-mg_bnqvI0GHXQfI1yAOshpqxEy51l1VB1pKHEadcwBvmdEsXMiWLm_3vPlQy-5Xr8H1xbYR1VY3igRqdflcMn3NAQUum-m923c_-IuMJjyvyeoRLWhsqbYeyU1eD_UECd08OibpVQjpnFmECRMYAFFFfnBB1urVPCoSaMlXUHGcGxyekRTCFsTOAStYF7WBXUiraRaZ4DVbuP0ukPQQhi_L3A9vq0myWGvFtqrgqnVBE6VYR-3ZpgZivUAJvvYCQYg_OCbIz37YnfaANuLtvblPmr7jDr3XjfO3ar7t3reMD7BtMYLtz2Vrz27G157QXwGJMEF0yvPbhaAhQXWixLHnmBlgXpeJBJmnlBgpmCtyI3Eggphmm6qSAkpqDUG3ejtxf7jpdj7gUv3pMXXPV7o7E_Gk6Gw_7A9y87XukFg-Gkdzm4Glz7o8l4MvCvXjvesxDgctC7nkyu-0N_7PtXo_FobH39ZfdMuNe_AaRWsro)

To download all of the processed data that powers this portfolio, please navigate to the folder titled `gtfs_digest` [here](https://console.cloud.google.com/storage/browser/calitp-publish-data-analysis). You will find the most recent datasets in `.parquet, .csv,.geojson` formats. Match the [readable column names](https://github.com/cal-itp/data-analyses/blob/main/gtfs_digest/readable.yml) to the table names. The data pulled from the Federal Transit Administration's National Transit Data is located [here](https://www.transit.dot.gov/ntd/data-product/2022-annual-database-agency-information). 

## Who We Are
This website was created by the [California Department of Transportation](https://dot.ca.gov/)'s Division of Data and Digital Services. We are a group of data analysts and scientists who analyze transportation data, such as General Transit Feed Specification (GTFS) data, or data from funding programs such as the Active Transportation Program. Our goal is to transform messy and indecipherable original datasets into usable, customer-friendly products to better the transportation landscape. For more of our work, visit our [portfolio](https://analysis.calitp.org/).

<img src="https://raw.githubusercontent.com/cal-itp/data-analyses/main/portfolio/Calitp_logo_MAIN.png" alt="Alt text" width="200" height="100"> <img src="https://raw.githubusercontent.com/cal-itp/data-analyses/main/portfolio/CT_logo_Wht_outline.gif" alt="Alt text" width="129" height="100">

<br>Caltrans®, the California Department of Transportation® and the Caltrans logo are registered service marks of the California Department of Transportation and may not be copied, distributed, displayed, reproduced or transmitted in any form without prior written permission from the California Department of Transportation.