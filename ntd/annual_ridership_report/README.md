# NTD Annual Ridership by RTPA


Provide CalSTA with NTD Annual Ridership by each regional transportation planning authority (RTPA)

Per the [SB125 Final Guildelines](https://calsta.ca.gov/-/media/calsta-media/documents/sb125-final-guidelines-a11y.pdf)
>Caltrans will provide all RTPAs with a summary report each month that meets the requirements of this statutory provision... For RTPAs with transit operators who do not report monthly data to the NTD, Caltrans will include the most recent annual ridership numbers provided to the NTD. RTPAs may publish


This report shows general ridership trends by Annual NTD Reporter for California RTPAs. Unlinked passenger trips are reported, as well as the change from the prior year. For example, July 2023's change would be the change in July 2023's reported values against July 2022's reported values.

## Definitions
- **FTA**: Federal Transit Administration.
- **Annual NTD Reporter**: Transit agencies that are required  to report yearly to the NTD, includes rural, urban and reduced reporters.
- **NTD**: National Transit Database. A reporting system that collects public transportation financial and operating information.
- **RTPA**: Regional Transportation Planning Authority.
- **UZA**: Urbanized Areas. An urbanized area is an incorporated area with a population of 50,000 or more that is designated as such by the U.S. Department of Commerce, Bureau of the Census.


## Methodology
Ridership data extract from NTD  via the [Service Data - Annual Data Tables](https://www.transit.dot.gov/ntd/ntd-data?field_data_categories_target_id%5B2551%5D=2551&field_product_type_target_id=1016&year=all&combine=). Then filtered for Reporters residing in California UZAs. These California Reporters are grouped by RTPAs, then aggregated mode and TOS. The processed data for each RTPA is saved to a public repository, see datasets below.


## Frequently Asked Questions
**Q: Which Annual NTD Reporters are in this report? Why are some Reporters missing from an RTPA?**

Transit operators/agencies that submit annual reports to NTD are included in this report. Reporters that were previously active reporters, but are currently not, may appear. This may result in Reporters showing zero or partial ridership data in the report. 

If a Reporter, type of service, mode, or any combination of, is not a annual reporter or has not reported data since 2018, they will not appear in the report.

Examples: 
- **Reporter A** is an annual reporter from 2019-2022, then became inactive and did not report for 2023. Reporter A's ridership data will be displayed for 2019-2022 only.
- **Reporter B** is an annual from 2000-2017, then became inactive and did not report for 2018. Reporter B will be named in the report, but will not display ridership data.
- **Reporter C** was an inactive reporter form 2015-2020, then became an active full reporter for 2021. Reporter C's ridership data will be displayed for 2021-present.  


**Q: Where can I download my RTPA's data?**

Data from this report can be downloaded from the Cal-ITP public data repository, see `Fully Processed Data Download` below. A Google Account is required to access the repository. Once logged in, navigate to `.....`, click the year-month you want to download, then click `download`.

The data is a zipped folder of all RTPA data for the year-month.


**Q: How can my RTPA/Agency meet the requirements of the SB125 Guidelines regarding how "to make publicly available a summary of ridership data"**

Per the [SB125 Final Guildelines](https://calsta.ca.gov/-/media/calsta-media/documents/sb125-final-guidelines-a11y.pdf):
>RTPAs are required to post a link to this report and data in a manner easily accessed by the public, so that ridership trends within their region can be easily reviewed

Hyperlinking this report on your RTPA's/Agency's webpage is a common method to meeting this requirement.

## Datasets / Data Sources
- [NTD Annual Service data](https://www.transit.dot.gov/ntd/data-product/2022-annual-database-service 
- [California RTPA list](https://gis.data.ca.gov/datasets/CAEnergy::regional-transportation-planning-agencies/explore?appid=cf412a17daaa47bca93c6d6b7e77aff0&edit=true)
- [Fully Processed Data Download](https://console.cloud.google.com/storage/browser/calitp-publish-data-analysis)



## Who We Are
This website was created by the [California Department of Transportation](https://dot.ca.gov/)'s Division of Data and Digital Services. We are a group of data analysts and scientists who analyze transportation data, such as General Transit Feed Specification (GTFS) data, or data from funding programs such as the Active Transportation Program. Our goal is to transform messy and indecipherable original datasets into usable, customer-friendly products to better the transportation landscape. For more of our work, visit our [portfolio](https://analysis.calitp.org/).

<img src="https://raw.githubusercontent.com/cal-itp/data-analyses/main/portfolio/Calitp_logo_MAIN.png" alt="Alt text" width="200" height="100"> <img src="https://raw.githubusercontent.com/cal-itp/data-analyses/main/portfolio/CT_logo_Wht_outline.gif" alt="Alt text" width="129" height="100">

<br>Caltrans®, the California Department of Transportation® and the Caltrans logo are registered service marks of the California Department of Transportation and may not be copied, distributed, displayed, reproduced or transmitted in any form without prior written permission from the California Department of Transportation.