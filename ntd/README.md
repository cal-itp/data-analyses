# Monthly NTD Ridership by RTPA

Provide CalSTA with NTD Monthly Ridership by each RTPA. 

Per the [SB125 Final Guildelines](https://calsta.ca.gov/-/media/calsta-media/documents/sb125-final-guidelines-a11y.pdf)
>Caltrans will provide all RTPAs with a summary report each month that meets the requirements of this statutory provision, drawn from the data reported to the National Transit Database. The data will be drawn from the NTD at: [Complete Monthly Ridership (with adjustments and estimates) | FTA (dot.gov)](https://www.transit.dot.gov/ntd/data-product/monthly-module-adjusted-data-release). RTPAs are required to post a link to this report and data in a manner easily accessed by the public, so that ridership trends within their region can be easily reviewed.

This report shows general ridership trends by transit agency, mode, and type of service for California RTPAs from 2018 to present. Unlinked passenger trips are reported, as well as the change from the prior year. For example, July 2023's change would be the change in July 2023's reported values against July 2022's reported values.

## Definitions
- **FTA**: Federal Transit Admisistration.
- **NTD**: National Transit Database. A reporting system that collects public transportation financial and operating information.
- **RTPA**: Regional Transportation Planning Authority.
- **UZA**: Urbanized Areas. An urbanized area is an incorporated area with a population of 50,000 or more that is designated as such by the U.S. Department of Commerce, Bureau of the Census.
- **MODE**: A system for carrying transit passengers described by specific right-of-way (ROW), technology and operational features. Examples: Bus, Cable Car, Light Rail.
- **TOS**: Describes how public transportation services are provided by the transit agency: directly operated (DO) or purchased transportation (PT) services.

## Methodology
Ridership data is ingested via the `FTA Complete Monthly Ridership` report, per the SB125 guidelines. Then filtered for agencies residing in California UZAs. These California Agencies are grouped by RTPAs, then aggregated by agencies, mode and TOS. The processed data for each RTPA is saved to a public respository, see datasets below.


## Frequently Asked Questions
**Q: Which agencies/transit operators are in this report? Why are some agencies missing from an RTPA?**

Per the [NTD Complete Monthly Ridership Report](https://www.transit.dot.gov/ntd/data-product/monthly-module-adjusted-data-release) webpage:
>File Summary: Contains monthly-updated service information reported by urban Full Reporters.

Each RTPA has their own tab. Within each tab are the Urban full reporters, that submit monthly ridership data to NTD. This report tracks data from 2018 to present. If an agency is not a monthly reporter, or has not reported data since 2018, they will not appear in the report. Previous monthly reporters may appear in the report under certain conditions. 


**Q: Where can I download my RTPA's data?**

Data from this report can be downloaded from the Cal-ITP public data repository, see `Fully Processed Data Download` below. A Google Account is required to access the repoisitory. Once logged in, navigate to `ntd_monthly_ridership`, click the year-month you want to download, then click `download`.

The data is a zipped folder of all RTPA data for the year-month.


**Q: How can my RTPA/Agency meet the requirements of the SB125 Guidelines regarding how "to make publicly available a summary of ridership data"**

Per the [SB125 Final Guildelines](https://calsta.ca.gov/-/media/calsta-media/documents/sb125-final-guidelines-a11y.pdf):
>RTPAs are required to post a link to this report and data in a manner easily accessed by the public, so that ridership trends within their region can be easily reviewed

Hyperlinking this report on your RTPA's/Agency's webpage is a common method to meeting this requirement.

## Datasets / Data Sources
- [NTD Complete Monthly Ridership Report](https://www.transit.dot.gov/ntd/data-product/monthly-module-adjusted-data-release) 
- [California RTPA list](https://gis.data.ca.gov/datasets/CAEnergy::regional-transportation-planning-agencies/explore?appid=cf412a17daaa47bca93c6d6b7e77aff0&edit=true)
- [Fully Processed Data Download](https://console.cloud.google.com/storage/browser/calitp-publish-data-analysis)



## Who We Are
This website was created by the [California Department of Transportation](https://dot.ca.gov/)'s Division of Data and Digital Services. We are a group of data analysts and scientists who analyze transportation data, such as General Transit Feed Specification (GTFS) data, or data from funding programs such as the Active Transportation Program. Our goal is to transform messy and indecipherable original datasets into usable, customer-friendly products to better the transportation landscape. For more of our work, visit our [portfolio](https://analysis.calitp.org/).

<img src="https://raw.githubusercontent.com/cal-itp/data-analyses/main/portfolio/Calitp_logo_MAIN.png" alt="Alt text" width="200" height="100"> <img src="https://raw.githubusercontent.com/cal-itp/data-analyses/main/portfolio/CT_logo_Wht_outline.gif" alt="Alt text" width="129" height="100">

<br>Caltrans®, the California Department of Transportation® and the Caltrans logo are registered service marks of the California Department of Transportation and may not be copied, distributed, displayed, reproduced or transmitted in any form without prior written permission from the California Department of Transportation.