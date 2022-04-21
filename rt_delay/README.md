# Introduction

These maps, tables, and charts provide an overview of typical weekday transit vehicle speeds in California. They are based on actual, archived positions data from each vehicle. They are generally accurate for their intended purpose of identifying slower parts of bus routes that would be candidates for projects to speed up buses, but some erroneous data may be present. Feel free to contact Cal-ITP with any data questions.

Maps are organized by Caltrans district, with a seperate page for each transit operator within the district.

## Data Sources

* General Transit Feed Specification (GTFS)
    * GTFS Schedule (static for each operator, includes timetable and route geometry information)
    * Archived GTFS-Realtime (GTFS-RT) vehicle positions data (vehicle locations updated about every 20 seconds)

## Methodology

Segment speed is estimated using the time and distance between vehicle positions reports, with distance being measured linearly along the corresponding transit route. These maps show stop-to-stop speeds, which are calculated by interpolating the two nearest position reports for each trip in order to estimate speed between two stops, then taking the 20th percentile of speeds for that stop segment in each period (morning peak, afternoon peak, and midday)

Delay is estimated by comparing the _time_ each transit vehicle actually arrived at a stop to the scheduled arrival time.

## Ongoing Work

The Cal-ITP team is working to share our derived datasets, including this vehicle speed and delay data, using the California Open Data Portal. In the interim, data is available upon request.

### Questions or Feedback? Please email hello@calitp.org