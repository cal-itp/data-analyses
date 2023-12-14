# Open Data Intake 

Document links, justification, for why datasets need to be made public. Submit this information every time a ticket is open to update the existing layers.

## Intranet Links
1. [Geospatial open data request](https://forms.office.com/Pages/ResponsePage.aspx?id=ZAobYkAXzEONiEVA00h1VuRQZHWRcbdNm496kj4opnZUNUo1NjRNRFpIOVRBMVFFTFJDM1JKNkY0SC4u)
    * District: `Sacramento HQ`
    * Division / Program: `Rail and Mass Transportation`
    * Update or New: `Update existing service`
    * Enterprise Geodatabase Name: `HQ`
    * Feature Class Name: `HQ.ca_hq_transit_stops, HQ.ca_hq_transit_areas, HQ.ca_transit_stops, HQ.ca_transit_routes, HQ.speeds_by_stop_segments, HQ.speeds_by_route_time_of_day`
    
1. [Open support ticket for GIS](https://forms.office.com/Pages/ResponsePage.aspx?id=ZAobYkAXzEONiEVA00h1VuRQZHWRcbdNm496kj4opnZUNkI2T0hGWElZMUcwTDRUOUNCU0VWMTUxWi4u)

## Intake Justification
### High Quality Transit Areas

High Quality Transit Areas, as described in Public Resources Code 21155, 21064.3, 21060.2, relies on the intersection of frequent transit service and is based on the General Transit Feed Specification (GTFS) data. This HQTA dataset provides four categories of high quality transit: rail, ferry, BRT, and the intersection of frequent bus corridors. 

### Transit Routes / Stops / Speeds

The General Transit Feed Specification (GTFS) provides transit schedules, including transit route and stop information, in text files. The California Integrated Travel Project within Caltrans ingests GTFS data daily for all operators in the state, standardizes, and processes this data for storage in its data warehouse. This dataset compiles all the route and stop information for all CA transit operators and provides it in a geospatial format. It also compiles all the GTFS real-time vehicle positions data, processes it into a usable format as speeds.
