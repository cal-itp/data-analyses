# High Quality Transit Areas Analysis

## High Quality Transit Areas Relevant Statutes

[PRC 21155](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=21155.&lawCode=PRC)
* Major transit stop definition: _A major transit stop is as defined in Section 21064.3, except that, for purposes of this section, it also includes major transit stops that are included in the applicable regional transportation plan_
* High-quality transit corridor definition: _For purposes of this section, a high-quality transit corridor means a corridor with fixed route bus service with service intervals no longer than 15 minutes during peak commute hours._
    * Unable to locate definition of "peak commute hours"

[PRC 21064.3](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=21064.3.&lawCode=PRC)
* _Major transit stop means a site containing any of the following:
(a) An existing rail or bus rapid transit station.
(b) A ferry terminal served by either a bus or rail transit service.
(c) The intersection of two or more major bus routes with a frequency of service interval of 15 minutes or less during the morning and afternoon peak commute periods._
    * "Intersection" may not be sufficiently well-defined for this analysis

[PRC 21060.2](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=PRC&sectionNum=21060.2.&highlight=true&keyword=bus%20rapid%20transit)
* _(a) “Bus rapid transit” means a public mass transit service provided by a public agency or by a public-private partnership that includes all of the following features:
(1) Full-time dedicated bus lanes or operation in a separate right-of-way dedicated for public transportation with a frequency of service interval of 15 minutes or less during the morning and afternoon peak commute periods.
(2) Transit signal priority.
(3) All-door boarding.
(4) Fare collection system that promotes efficiency.
(5) Defined stations._
    * Unlikely to determine if a service qualifies as BRT under this definition using GTFS alone
    
## Workflow

HQTA data is updated at a monthly frequency for the open data portal.

1. [Download data](./download_data.py) and cache parquets in GCS
1. [Create JSONs](./operators_for_hqta.py) storing a dictionary with [all the operators](./hqta_operators.json) that have cached files, and [valid operators](./valid_hqta_operators.json). 
    * It's possible in downloading the `routes`, `trips`, `stops`, and `stop_times`, one of the files is not able to be successfully downloaded, but the other 3 are. 
    * Also, it's possible that files with no rows are downloaded.
    * A check for complete information (all 4 files are present and are non-empty)
    * These valid operators are the ones that continue on in the HQTA workflow.
1. 