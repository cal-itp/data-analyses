# NTD Monthly Ridership by RTPA

Provide CalSTA with NTD Monthly Ridership by each regional transportation planning authority (RTPA).

This report shows general ridership trends by transit agency, mode, and type of service. Reported unlinked passenger trips are reported, as well as the change from the prior year. For example, July 2023's change would be the change in July 2023's reported values against July 2022's reported values.

## Datasets
1. NTD monthly data: https://www.transit.dot.gov/ntd/data-product/monthly-module-adjusted-data-release. 
2. [RTPA list](https://gis.data.ca.gov/datasets/CAEnergy::regional-transportation-planning-agencies/explore?appid=cf412a17daaa47bca93c6d6b7e77aff0&edit=true)
3. Download our processed full data [here](https://console.cloud.google.com/storage/browser/calitp-publish-data-analysis).


# NTD ID Changes 2021_2022 Crosswalk

NTD IDs have changed for some agencies from 2021 to 2022 i.e. NTD Id is  no longer unique at the year level. 

For the NTDs ID that have changed we have mapped to the historical data. 

## Datasets
1. NTD 2021_2022 raw data: (gs://calitp-analytics-data/data-analyses/ntd/ntd_2021_2022.csv)
2. Data that required manual crosswalk: (gs://calitp-analytics-data/data-analyses/ntd/manual.csv)
3. Download our final data that has crosswalk as well as additional rem
