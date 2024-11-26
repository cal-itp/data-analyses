# AHSC Ridership Prediction Dashboard

The goal of this tool is to provide predictions to estimate increases in annual bus ridership that correspond to planned increases in scheduled bus services. We use General Transit Feed Specification (GTFS) schedule data, American Community Survey (ACS) data and Ridership data from selected agencies to feed our statistical model. The dashboard summarizes the additional annual riders across each stop that a route goes by. This tool is available to assist mainly AHSC, CARB grant applicants along with other agencies. 

This tool currently does not support the addition of new routes or routing changes, but it will continue to evolve as more data becomes available. Feedback is always welcome to help improve its functionality. If you have additional questions, email hello@calitp.org.


## Definitions and Methodology

### <ins>Data Used</ins>
#### GTFS-Schedule

The GTFS Schedule serves as an input to our statistical model, which includes key information such as timetable and frequency by time of day and day of week, as well as the spatial locations of transit routes. Cal-ITP collects GTFS Schedule data from the feeds of over 180 transit agencies, using information from the `stops`, `routes`, `trips`, and `stop_times` tables. For each representative weekday (Wednesday), Saturday, and Sunday, all trips—regardless of route—that stop at a given stop are counted. This measure of service is used to model ridership patterns, with the data also capturing the number of routes that stop at each location.



#### Ridership Data for Model Training
The model relies on a limited amount of stop-level ridership data, which is essential for its development. Currently, it uses data from three transit agencies:

* LA Metro (March 2022)
* Santa Barbara MTD (July 2021 – June 2022)
* Monterey-Salinas Transit (September 2021 – August 2022)

To estimate the relationship between service and ridership, this model uses stop-level ridership figures (weekday, Saturday, and Sunday boardings) from these agencies. Data from LA Metro was annualized based on the ratio between the March 2022 NTD ridership report and the annual ridership figure from the National Transit Database (NTD). Additionally, LA Metro's BRT routes were excluded from the analysis due to their significantly higher ridership compared to regular bus service. A small number of stops with riders could not be matched to stops in the collected GTFS data.

Cal-ITP is actively seeking to add stop-level ridership data from other agencies for future iterations of this model.



#### Ridership Data for Prediction Calibration
Because the predicted annual ridership values from the statistical model are based on ridership data from medium-to-large agencies, this estimate uses NTD ridership data to “rescale” the stop-level estimates. This analysis gets the sum of unlinked passenger trips (UPT) on any bus modality from October 2021 to September 2022 for each California agency that reports to NTD. The rescaling process is described below.


#### ACS Data

In addition to bus service levels, this statistical model also controls for the effect of some demographic and socioeconomic factors that correlate with ridership in academic literature. From 2018 American Community Survey census tract-level tables, we calculate population density, the percent of population that are not US citizens, the percent of youth (age 0-24), the percent of population that are seniors (age 65+), the percent of the working population without access to a car, and the percent of population (for whom poverty status can be determined) living below the poverty line. 

To aggregate socioeconomic and demographic variables to each stop, census tracts whose boundaries intersect a quarter-mile radius buffer circle around a bus stop are associated with that stop. This distance roughly corresponds with 10 minutes walking distance. A stop can be associated with multiple tracts, and a tract can be associated with multiple stops. The counts for the relevant numerators (e.g. population, for population density) and denominators (e.g. land area, for population density) are summed for a given stop neighborhood, and the percentage variables are created after that.



#### Job Density Data

This statistical model also controls for job density, which is derived from the Urban Institute’s summary files for Longitudinal Employer-Household Dynamics (LEHD) Origin-Destination Employment Statistics. The analysis uses Workplace Area Characteristic (WAC) tables to sum federal and non-federal jobs for each census tract. Additional documentation is available on the Urban Institute’s [website](https://datacatalog.urban.org/dataset/longitudinal-employer-household-dynamics-origin-destination-employment-statistics-lodes). These figures are aggregated in the same way as the ACS data, above.



### <ins>Methodology</ins>

#### Statistical Modeling
In order to estimate the effect of bus service on bus ridership at each stop, this analysis employs the following log-linear OLS regression model on the training data: *ln⁡(annual boardings)= β0 + β1(n daily trips) + β2(n daily routes) + β3(population density)+ β4(job density)+ β5(pct noncitizens) + β6(pct youth) + β7(pct seniors) + β8(pct workers w/o car) + β9(pct poverty) + e* . Versions of this model are generated for weekday, Saturday, and Sunday ridership and corresponding service. In all three models, one additional daily trip corresponds to a roughly 2% increase in ridership at a given bus stop.

For each bus agency for which Cal-ITP collects GTFS-Schedule data, trips and routes per stop are aggregated and joined with ACS and job density variables as described above. Multiplying coefficients (β) by their respective factors and exponentiating the result creates the initial stop-level ridership estimate. Each agency’s stop-level ridership is summed to estimate system-level ridership for weekdays, Saturday, and Sunday. 


#### Predictions from Model

A “calibration factor” is created from the proportion between the model-estimate system-level ridership and NTD system-level ridership. NTD ridership is multiplied by 261/365 to estimate weekday ridership, and Saturday/Sunday ridership is estimated from multiplying the NTD total by 52/365 (this over-estimates weekend ridership and under-estimates weekday ridership by a small amount). Where a calibration factor cannot be created because an agency does not report ridership to the NTD, the median calibration factor of all other agencies is used. NTD-calibrated stop-level ridership estimate is equal to the model-estimate stop-level ridership divided by this calibration factor.

This stop-level dataset is expanded by the number of routes that go by a given stop, and then again by the number of trips. The maximum number of additional trips for a route is 20. The number of additional annual riders associated with an additional trip is about 2% of the initial stop-level annual ridership. The dashboard summarizes the additional annual riders across each stop that a route goes by. This tool does not currently accept additional routes or routing changes, but Cal-ITP is seeking feedback for the next cycle. 



## Frequently Asked Questions

**Why isn't my agency listed in the dashboard?** Applicant agencies must meet the following conditions to use this (optional) annual ridership estimate tool:
* Must provide GTFS Schedule data to Caltrans (Cal-ITP)
* Must provide fixed-route bus service If you don't see your agency here but meet both of these requirements, please email hello@calitp.org.

**How do I estimate future years of ridership?** This tool requires annual ridership increase estimates be tied to specific increases in service. For example, if your agency expects to add 1 additional trip on weekdays in year 1, and 1 additional trip on Saturday/Sunday in year f, please plug in the estimated additional trips for year f and report those numbers. If you have previously estimated future increases with different assumptions, please email hello@calitp.org with technical questions.

**My agency wants to add or change one or more routes. Can I estimate annual ridership with this tool?** At this time, this tool estimates annual ridership increases based increased service on existing routes (as of the week of 11/28). We are actively collecting feedback to develop this capability for future cycles.




## Who We Are
This website was created by the [California Department of Transportation](https://dot.ca.gov/)'s Division of Data and Digital Services. We are a group of data analysts and scientists who analyze transportation data, such as General Transit Feed Specification (GTFS) data, or data from funding programs such as the Active Transportation Program. Our goal is to transform messy and indecipherable original datasets into usable, customer-friendly products to better the transportation landscape. For more of our work, visit our [portfolio](https://analysis.calitp.org/).

<img src="https://raw.githubusercontent.com/cal-itp/data-analyses/main/portfolio/Calitp_logo_MAIN.png" alt="Alt text" width="200" height="100"> <img src="https://raw.githubusercontent.com/cal-itp/data-analyses/main/portfolio/CT_logo_Wht_outline.gif" alt="Alt text" width="129" height="100">

<br>Caltrans®, the California Department of Transportation® and the Caltrans logo are registered service marks of the California Department of Transportation and may not be copied, distributed, displayed, reproduced or transmitted in any form without prior written permission from the California Department of Transportation.









