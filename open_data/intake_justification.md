# Open Data Intake 

Document links, justification, for why datasets need to be made public. Submit this information every time a ticket is open to update the existing layers.

## Intranet Links
1. [Geospatial open data request](https://sv03tmcpo.ct.dot.ca.gov/portal/apps/sites/#/geep/pages/open-data-request)
1. [Open support ticket for GIS](https://sv03tmcpo.ct.dot.ca.gov/portal/apps/sites/#/geep/pages/support-request)

### High Quality Transit Areas

High Quality Transit Areas, as described in Public Resources Code 21155, 21064.3, 21060.2, relies on the intersection of frequent transit service. These are subject to the transit schedules available in the General Transit Feed Specification (GTFS). The California Integrated Travel Project within Caltrans ingests GTFS data daily for all operators in the state, standardizes, and processes this data for storage in its data warehouse. 

Capturing where these HQTAs are is one expected spatial product from GTFS data. Given that GTFS data is always being updated by operators, whether it is to increase or reduce service, 
this HQTA dataset also reflects the most recent boundaries given operator's latest scheduled trips.

### Transit Routes / Stops

The General Transit Feed Specification (GTFS) provides transit schedules, including transit route and stop information, in text files. The California Integrated Travel Project within Caltrans ingests GTFS data daily for all operators in the state, standardizes, and processes this data for storage in its data warehouse.  

This dataset compiles all the route and stop information for all CA transit operators and provides it in a geospatial format. Transit routes are shown with their line geometry, and transit stops are shown with their point geometry. Given that GTFS data is always being updated by operators, whether it is to increase or reduce service, CA transit routes / transit stops datasets provide monthly snapshots for operators' latest schedules. 