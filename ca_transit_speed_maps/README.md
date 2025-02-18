# California Transit Speed Maps

Supporting transit is integral to Caltrans' vision of delivering a connected, equitable, and more sustainable state transportation network as well as the Department’s work to combat the climate crisis.

To help realize that vision, the California Integrated Travel Program (Cal-ITP) is developing analyses alongside our partners such as the Caltrans Division of Research, Innovation and System Information (DRISI) in order to provide Caltrans staff and external stakeholders with actionable data to prioritize transit projects.

Transit in California is overwhelmingly provided by buses. However, speeds have been declining for years, and now average below 10mph across much of California – and often much lower during peak times in urban areas. These slow speeds translate to countless hours wasted for transit riders and jeopardize the ability of California’s transit agencies to provide frequent, reliable service.

We also measure speed variation. Low speeds delay transit, but variation is equally problematic since it leads to an inconsistent experience for transit riders and complicates scheduling of transit services.

Our maps already include routes and operators accounting for about 80% of bus riders in the state, including operators in every Caltrans district. Cal-ITP continues to work daily to onboard and support new agencies in providing quality data that improves the transit rider experience and powers useful and timely analysis.

This site is updated at least quarterly.

Select source code can be found at:

[https://github.com/cal-itp/data-analyses/tree/main/ca_transit_speed_maps](https://github.com/cal-itp/data-analyses/tree/main/ca_transit_speed_maps)

[https://github.com/cal-itp/data-analyses/tree/main/rt_delay/rt_analysis](https://github.com/cal-itp/data-analyses/tree/main/rt_delay/rt_analysis)

[https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/rt_utils.py](https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/rt_utils.py)


## Time Period Definitions

* AM Peak: 07:00-10:00
* Midday: 10:00-15:00
* PM Peak: 15:00-20:00
* Evening: 20:00-24:00
* Owl: 00:00-04:00
* Early AM: 04:00-07:00

## Methodology

Segment speed is estimated using the time and distance between vehicle positions reports, with distance being measured linearly along the corresponding transit route. These maps show speeds along segments, which are calculated by interpolating the two nearest position reports for each trip in order to estimate speed along each segment, then taking the 20th percentile of speeds for that segment in each time period. This site shows data for a specific day, usually a Wednesday (and not a holiday or other date we believe could be atypical).

Generally, segments are constructed from one stop to the next, however, if the distance between stops is large we add interpolated segments every kilometer to provide additional resolution for rural and express services.

We use the ratio between 80th and 20th percentile speeds in a segment to measure speed variation.

## What's New?

We're aligining with segment speeds published via the [open data portal!](https://gis.data.ca.gov/datasets/4937eeb59fdb4e56ae75e64688c7f2c0_0/explore)

While the datasets don't fully match just yet, this site is now based on the same underlying data. This should help us update it more often, enhance accuracy for complex route shapes, and include as many transit operators as possible.

The site also includes a more informative speed and variation by time of day chart at the bottom of the page.

Route segments with no data are now shown as thin, dark gray or black segments on the maps. These may be due to the route not running at that time of day, or a data availability or processing issue.
    
## Data Sources
Archived GTFS-Realtime Vehicle Positions data, plus corresponding GTFS Schedule data.

Each map includes a link to download its geospatial data in a gzip-compressed GeoJSON format (.geojson.gz). Most file compression software, including 7zip on Windows, will decompress these files. Once decompressed, most GIS software including ESRI ArcGIS Pro and QGIS can import the GeoJSON.

## Ongoing Work

The Cal-ITP team is working to transition the site to a rolling average of recent dates, instead of a single day.

Questions or feedback? Please email hello@calitp.org

## Who We Are

This website was created by the [California Department of Transportation](https://dot.ca.gov/)'s Division of Data and Digital Services. We are a group of data analysts and scientists who analyze transportation data, such as General Transit Feed Specification (GTFS) data, or data from funding programs such as the Active Transportation Program. Our goal is to transform messy and indecipherable original datasets into usable, customer-friendly products to better the transportation landscape. For more of our work, visit our [portfolio](https://analysis.calitp.org/).

<img src="https://raw.githubusercontent.com/cal-itp/data-analyses/main/portfolio/Calitp_logo_MAIN.png" alt="Alt text" width="200" height="100"> <img src="https://raw.githubusercontent.com/cal-itp/data-analyses/main/portfolio/CT_logo_Wht_outline.gif" alt="Alt text" width="129" height="100">

<br>Caltrans®, the California Department of Transportation® and the Caltrans logo are registered service marks of the California Department of Transportation and may not be copied, distributed, displayed, reproduced or transmitted in any form without prior written permission from the California Department of Transportation.
