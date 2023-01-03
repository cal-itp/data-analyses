# AHSC Ridership Prediction Methodology

## Overview

This ridership prediction tool estimates increases in annual bus ridership that correspond to planned increases in scheduled bus service. Caltrans ([Cal-ITP](https://www.calitp.org/)) has constructed a statistical model from GTFS data, ridership data, and other socioeconomic/demographic data to assist AHSC grant applicants. If you have additional questions, email hello@calitp.org. 

## Input Data Processing

### GTFS-Schedule

Cal-ITP collects GTFS Schedule data from the feeds of over 180 transit agencies. This analysis uses information from the `stops`, `routes`, `trips`, and `stop_times` tables. For a representative weekday (Wednesday), Saturday, and Sunday, all trips (regardless of route) that stop at a given stop in each day are counted. This is the primary measure of service that ridership is regressed on. The data also counts the number of routes that stop by a given stop.

### Ridership Data for Model Training

In order to estimate the relationship between service and ridership, this model uses stop-level ridership figures (weekday, Saturday, and Sunday boardings) from three agencies:
* LA Metro (March 2022)
* Santa Barbara MTD (July 2021 – June 2022)
* Monterey-Salinas Transit (September 2021 – August 2022)

Data from LA Metro was annualized based on the ratio between March 2022 NTD ridership report and the annual ridership figure from the National Transit Database (NTD). LA Metro BRT routes were also dropped from analysis for having ridership that is significantly higher than regular bus service. A small number of stops with riders could not be matched to stops in collected GTFS data. Cal-ITP is actively seeking to add stop-level ridership data from other agencies for future iterations of this model.

### Ridership Data for Prediction Calibration

Because the predicted annual ridership values from the statistical model are based on ridership data from medium-to-large agencies, this estimate uses NTD ridership data to “rescale” the stop-level estimates. This analysis gets the sum of unlinked passenger trips (UPT) on any bus modality from October 2021 to September 2022 for each California agency that reports to NTD. The rescaling process is described below.

### ACS Data

In addition to bus service levels, this statistical model also controls for the effect of some demographic and socioeconomic factors that correlate with ridership in academic literature. From 2018 American Community Survey census tract-level tables, we calculate population density, the percent of population that are not US citizens, the percent of youth (age 0-24), the percent of population that are seniors (age 65+), the percent of the working population without access to a car, and the percent of population (for whom poverty status can be determined) living below the poverty line. 

To aggregate socioeconomic and demographic variables to each stop, census tracts whose boundaries intersect a quarter-mile radius buffer circle around a bus stop are associated with that stop. This distance roughly corresponds with 10 minutes walking distance. A stop can be associated with multiple tracts, and a tract can be associated with multiple stops. The counts for the relevant numerators (e.g. population, for population density) and denominators (e.g. land area, for population density) are summed for a given stop neighborhood, and the percentage variables are created after that.

### Job Density Data

This statistical model also controls for job density, which is derived from the Urban Institute’s summary files for Longitudinal Employer-Household Dynamics (LEHD) Origin-Destination Employment Statistics. The analysis uses Workplace Area Characteristic (WAC) tables to sum federal and non-federal jobs for each census tract. Additional documentation is available on the Urban Institute’s [website](https://datacatalog.urban.org/dataset/longitudinal-employer-household-dynamics-origin-destination-employment-statistics-lodes). These figures are aggregated in the same way as the ACS data, above.

## Statistical Modeling

In order to estimate the effect of bus service on bus ridership at each stop, this analysis employs the following log-linear OLS regression model on the training data: ln⁡〖annual boardings〗= β_0+ β_1 (n daily trips)+ β_2 (n daily routes)+ β_3 (population density)+ β_4 (job density)+ β_5 (pct noncitizens)+β_6 (pct youth)+ β_7 (pct seniors)+β_8 (pct workers w/o car)+ β_9 (pct poverty)+e . Versions of this model are generated for weekday, Saturday, and Sunday ridership and corresponding service. In all three models, one additional daily trip corresponds to a roughly 2% increase in ridership at a given bus stop.

## Predictions from Model

For each bus agency for which Cal-ITP collects GTFS-Schedule data, trips and routes per stop are aggregated and joined with ACS and job density variables as described above. Multiplying coefficients (β) by their respective factors and exponentiating the result creates the initial stop-level ridership estimate. Each agency’s stop-level ridership is summed to estimate system-level ridership for weekdays, Saturday, and Sunday. 

A “calibration factor” is created from the proportion between the model-estimate system-level ridership and NTD system-level ridership. NTD ridership is multiplied by 261/365 to estimate weekday ridership, and Saturday/Sunday ridership is estimated from multiplying the NTD total by 52/365 (this over-estimates weekend ridership and under-estimates weekday ridership by a small amount). Where a calibration factor cannot be created because an agency does not report ridership to the NTD, the median calibration factor of all other agencies is used. NTD-calibrated stop-level ridership estimate is equal to the model-estimate stop-level ridership divided by this calibration factor.

This stop-level dataset is expanded by the number of routes that go by a given stop, and then again by the number of trips. The maximum number of additional trips for a route is 20. The number of additional annual riders associated with an additional trip is about 2% of the initial stop-level annual ridership. The dashboard summarizes the additional annual riders across each stop that a route goes by. This tool does not currently accept additional routes or routing changes, but Cal-ITP is seeking feedback for the next cycle. 

