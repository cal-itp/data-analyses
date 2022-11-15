# Dask Outline

## Business Conditions
* Substantial geospatial processing done (Python!)
* GTFS data for all CA transit operators (big!)
* monthly updates to the CA open data portal (frequent!)
* v1: get it there
* rewrites / refactoring: get it there faster and sustainably

### Statutes
List all statutes: 
* [PRC 21060.2](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=PRC&sectionNum=21060.2.&highlight=true&keyword=bus%20rapid%20transit)
* [PRC 21155](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=21155.&lawCode=PRC)
* [PRC 21064.3](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=21064.3.&lawCode=PRC)

For this presentation, focus on 2 definitions, because that's where the bottlenecks in the code processing time is.
* High-quality transit corridor: _A corridor with fixed route bus service with service intervals no longer than 15 minutes during peak commute hours._ [PRC 21155](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=21155.&lawCode=PRC)
* _Major transit stop:
(c) The intersection of two or more major bus routes with a frequency of service interval of 15 minutes or less during the morning and afternoon peak commute periods._
[PRC 21064.3](https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?sectionNum=21064.3.&lawCode=PRC)

## Back to the Basics
* Dask forced me to consider which tasks were truly sequential and which ones could be parallelized.
    * No apparent memory issue because we sidesteppped it with looping, but then imposed a sequential framework when there need not be one
* Use Dask to address the memory issue, and remove loops where possible.
   * `stop_times`: v1: bring in each operator's stop_times table in with pandas, clean / parse timestamps to grab departure hour, then aggregate to AM/PM peak. 
   * v2: clean / parse each operator's stop_times table with dask, aggregate to AM/PM peak.
   * v3: clean / parse all opeators with dask, aggregate.

### HQ Transit Corridors
* **corridor** --> segments (cut bus route into 1,250 m long segments, then buffer it by 50 m) --> `shapes` and `routes` tables
* **service intervals no longer than 15 minutes** --> at least 4 trips per hour --> aggregate `stop_times`,  spatially join `stops` to segments
* **peak commute hours** --> be more flexible and take the maximum trips in the AM (before 12pm) and PM (12pm-after) --> `stop_times` aggregated by stops and find the `max_am_trips` and `max_pm_trips`

### Bus Corridor Intersections
* Geography plays a role! Bay Area transit operators and SoCal transit operators are not going to overlap
* When we say "intersection", we where orthogonal routes cross, not when routes traveling in the same direction have an area of intersection
* We can use both ideas to greatly simplify the code
* v1: clip each `shape_id` against all other ones (but clipping is computationally expensive) and remove geometries that are too large that would indicate same direction routes overlapping....step takes a couple hours.
* v2: factor in geography and route direction: create pairwise table with spatial join where you compare segments _across_ and _within_ operators. only save orthogonal intersections. the pairwise table narrows down only valid clips....step takes 45 min
* v3: factor in route direction earlier: no more looping _across_ and _within_ operators...just compare north-south routes against east-west routes...step takes 1.5 min (local dask, without connecting to dask cloud cluster)

## References
* [TDS: Good Data Scientists Write Good Code](https://towardsdatascience.com/good-data-scientists-write-good-code-28352a826d1f)
   * make it work, make it right, make it fast
   * code quality comic!
* [TDS: Does Your Code Smell](https://towardsdatascience.com/does-your-code-smell-acb9f24bbb46)
* [TDS: modularity, readability, speed](https://towardsdatascience.com/3-key-components-of-a-well-written-data-model-c426b1c1a293)