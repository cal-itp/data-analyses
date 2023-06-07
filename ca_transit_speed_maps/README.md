# Introduction

Supporting transit is integral to Caltrans' vision of delivering a connected, equitable, and more sustainable state transportation network as well as the Department’s work to combat the climate crisis.

To help realize that vision, the California Integrated Travel Program (Cal-ITP) is developing analyses alongside our partners such as the Caltrans Division of Research, Innovation and System Information (DRISI) in order to provide Caltrans staff and external stakeholders with actionable data to prioritize transit projects.

Transit in California is overwhelmingly provided by buses. However, speeds have been declining for years, and now average below 10mph across much of California – and often much lower during peak times in urban areas. These slow speeds translate to countless hours wasted for transit riders and jeopardize the ability of California’s transit agencies to provide frequent, reliable service.

Our maps already include routes and operators accounting for about 80% of bus riders in the state, including operators in every Caltrans district. Cal-ITP continues to work daily to onboard and support new agencies in providing quality data that improves the transit rider experience and powers useful and timely analysis.

# About This Speedmap Site

These maps, tables, and charts provide an overview of typical weekday transit vehicle speeds in California. They are based on actual, archived positions data from each vehicle. They are generally accurate for their intended purpose of identifying slower parts of bus routes that would be candidates for projects to speed up buses, but some erroneous data may be present. Feel free to contact Cal-ITP with any data questions.

Maps are organized by Caltrans district, with a separate page for each transit operator within the district. It's easiest to navigate the site using the menu on the left side of the page. Click the dropdown icon for each district to show links to each operator within the district. Alternatively, you can navigate through all pages in order using the "Next" and "Previous" links at the bottom of each page.

## New: Speed Variation Maps and Download Links!

![speedmap header with download link in top right circled in red](https://github.com/cal-itp/data-analyses/blob/065adb2838482619c039791f6f3090b7efaa9e82/ca_transit_speed_maps/img/download.png?raw=true)

We've updated our map rendering technology for better performance and flexibility.

Speed variation maps are now displayed for each time of day, just like speed maps. Segments with high variation in speeds make it difficult for transit operators to set accurate schedules, and can cause inconsistent service for riders. Identifying where speeds are inconsistent provides another useful data point to target transit priority measures in addition to actual speeds.

Each map now comes with a link to download its geospatial data in a gzip-compressed GeoJSON format (.geojson.gz). Most file compression software, including 7zip on Windows, will decompress these files. Once decompressed, most GIS software including ESRI ArcGIS Pro and QGIS can import the GeoJSON.

## Data Sources

* General Transit Feed Specification (GTFS)
    * GTFS Schedule (static for each operator, includes timetable and route geometry information)
    * Archived GTFS-Realtime (GTFS-RT) vehicle positions data (vehicle locations updated about every 20 seconds)

## Methodology

Segment speed is estimated using the time and distance between vehicle positions reports, with distance being measured linearly along the corresponding transit route. These maps show speeds along segments, which are calculated by interpolating the two nearest position reports for each trip in order to estimate speed along each segment, then taking the 20th percentile of speeds for that segment in each period (morning peak, afternoon peak, and midday). Generally, segments are constructed from one stop to the next, however, if the distance between stops is large we add interpolated segments every kilometer to provide additional resolution for rural and express services.

## Ongoing Work

The Cal-ITP team is working to share broader speed and delay data using the California Open Data Portal.

### Questions or Feedback? Please email hello@calitp.org