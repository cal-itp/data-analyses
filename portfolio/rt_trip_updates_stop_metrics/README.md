# README

One of the most common transit user behaviors is to consult an app (Google Maps, Apple Maps, NextBus, etc) to find out when the bus or train is going to arrive.

That widely desired piece of information is powered by GTFS Real-Time Trip Updates, specifically the [Stop Time Updates](https://gtfs.org/documentation/realtime/reference/#message-stoptimeupdate) specification.

GTFS Real Time trip updates performance metrics, specifically the stop time update messages. 
Accurate and reliable information should be provided to transit users for journey planning. These performance metrics provide insights into:

* availability and completeness of RT - is there information available for users?
* prediction inconsistency - how much are predictions changing from minute to minute as the bus approaches time of arrival?
* prediction reliability and accuracy - are these predictions accurate (when compared to our estimated actual time of arrival)?


## Reliable Prediction Accuracy

The prediction is considered **accurate** if it falls within the bounds of this equation: `-60ln(Time to Prediction+1.3) < Prediction Error < 60ln(Time to Prediction+1.5)`.

As the bus approaches each stop, the software is making predictions for when the bus should arrive. When the bus is 30 min away from arrival, there is a more generous buffer for accuracy; this buffer tightens as the bus is nearing the stop.

| Minutes Until Bus Arives | Accurate Within Bounds          |
|--------------------------|---------------------------------|
| 0 min                    | -0.26 min early - 0.41 min late |
| 10 min                   | -2.42 min early - 2.44 min late |
| 30 min                   | -3.44 min early - 3.45 min late |

* Positive values = arrival came **after** the prediction. 
   * actual_arrival = 8:05 am
   * predicted arrival = 8:00 am
   * actual_arrival - predicted_arrival = +5 seconds
   * follow the prediction, you will catch the bus
* Negative values = arrival came **before** the prediction.
   * actual_arrival = 8:05 am
   * predicted arrival = 8:10 am
   * actual_arrival - predicted_arrival = -5 seconds
   * follow the prediction, you will **miss** the bus...this is very bad!
   * we want fewer of these kinds of predictions, and would much rather wait for the bus than to miss it

## Availability and Completeness of Predictions

* This metric is the easiest to achieve. For starters, having information is better than no information.
* For each instance of scheduled stop arrival, there is complete information if there are at least 2 predictions each minute.
* For the 30 minute period before the bus arrives at each stop, each minute is an observation that goes into this calculation (up to 30 observations).
* This ensures that we have fairly equal number of observations for each stop and can compare across stops.
   * We want to avoid having 30 minutes of predictions for the 1st stop and 60 minutes of predictions for the last stop and comparing metrics that have different denominators.
   
## Prediction Inconsistency

* This metric (also called jitter or wobble) captures another aspect of transit user experience. Any change in prediction is counted, so this metric **only has positive values**, but smaller positive values are better.
   * If the prediction is changing from minute to minute, a large spread would show up.
   * If the prediction is fairly consistent, we would see small spread.
* There is [research](https://www.sciencedirect.com/science/article/abs/pii/S0965856416303494) around how transit users perceive wait time, and that users perceive longer wait times than what is actually experienced. Decreasing the perceived wait time by providing real-time information has positive benefits for user experience. 

## Master Services Agreement
Exhibit H definitions (pg 53 on pdf)

| **Item** | **Report Metric** | **Definition** | **Implementation Notes** |
|---|---|---|---|
| 3. Availability of<br>Acceptable StopTimeUpdate<br>Messages | pct_tu_complete_minutes, <br>n_tu_complete_minutes,<br>n_tu_minutes_available | Percent of time riders have up-to-date prediction information available, calculated as the percent of one-minute time bins for a given trip and stop during a Trip Time Span where there are two (2) or greater GTFS-RT StopTimeUpdate arrival predictions per minute. | Each minute for the 30 minute period <br>before **each** stop's <br>arrival for equal comparison across stops |
| 9. Experienced Wait <br>Time Delay | prediction_error_label, <br>avg_prediction_error_minutes | The amount of time a transit rider perceives they have waited after seeing the real-time information in their Journey Planning Application and the arrival of the next vehicle arrives at their stop. This is calculated as the time interval between the next trip to arrive at a stop for a given route_id/shape_id/stop_id combination and the next predicted arrival time from a StopTimeUpdate message for that route_id/shape_id/stop_id combination as sampled for each minute of the day that the route_id/shape_id/stop_id combination is in service. | Use a simpler derived version with average<br>prediction error.<br>Current aggregation does not support <br>route aggregation yet. |
| 23. Measurement Time Windows |  | A series of 30 consecutive time windows, each starting one (1) minute apart and lasting two (2) minutes. | Each minute for the 30 minute period <br>before **each** stop's <br>arrival for equal comparison across stops |
| 27. Prediction Error | avg_prediction_error_minutes | Actual Trip Stop Arrival Time minus the Predicted Trip Stop Arrival Time in seconds. Note that while Prediction Error is not the final metric in this case, it is useful to retain this value into the future and in archival storage in the event that<br>the definition of the frontier defined in Reliable Accuracy is changed in the future based on a specific agency’s needs |  |
| 28. Prediction<br>Inconsistency | avg_prediction_spread_minutes | How much the prediction changes in the last thirty (30) minutes before a vehicle arrives at a stop, calculated for a given trip and stop as the average Predicted Trip Stop Arrival Spread of all Measurement Time Windows where a given window has a StopTimeUpdate message for the trip and stop with a timestamp in that window. |  |
| 29. Prediction Reliability | pct_tu_accurate_minutes, <br>n_tu_accurate_minutes,<br>n_tu_minutes_available,<br>pct_tu_predictions_early/ontime/late,<br>n_predictions,<br>n_predictions_early/ontime/late | The percent of time transit riders are looking at a reliably good prediction – understanding that the closer a vehicle is<br>to a stop, the better the prediction should be, calculated as the percent of minutes for each stop for each trip where predictions have Reliable Accuracy, starting sixty (60) minutes before the first scheduled stop for the trip. | Each minute for the 30 minute period <br>before **each** stop's <br>arrival for equal comparison across stops |
| 32. Reliable Accuracy | pct_tu_accurate_minutes, <br>n_tu_accurate_minutes,<br>n_tu_minutes_available,<br>pct_tu_predictions_early/ontime/late,<br>n_predictions,<br>n_predictions_early/ontime/late | A prediction has reliable accuracy if:<br>-60ln(Time to Prediction+1.3) < Prediction Error < 60ln(Time<br>to Prediction+1.5). |  |
| 39. Time to Prediction |  | The current time until the Predicted Trip Stop Arrival Time in minutes |  |
| 43. Trip Start Time  |  | Time of the first scheduled stop arrival of the trip per the GTFS Schedule for trips with ScheduleRelationship =<br>SCHEDULED or CANCELED or the first predicted arrival time for other ScheduleRelationship values. |  |
| 44. Trip Time Span |  | Time in minutes from the Trip Start Time to the arrival time<br>at the stop being measured | Each minute for the 30 minute period <br>before **each** stop's <br>arrival for equal comparison across stops |

## References
* Caltrans GTFS RT Master Service Agreement Contract
   * Swiftly provides a prediction accuracy exponential equation
* Professor Gregory Newmark's paper: [Assessing GTFS Accuracy](https://transweb.sjsu.edu/sites/default/files/2017-Newmark-Public-Transit-Statistical-Analysis.pdf)
    * This project is a work in progress for productionizing and implementing all the ideas presented in this paper.
    * This paper provides the basis of policy and planning interpretations around the various metrics.
    * We replicate as many of the visualizations and tables as possible.
* Yingling Fan, Andrew Guthrie, David Levinson's paper on [Waiting time perceptions at transit stops and stations](https://www.sciencedirect.com/science/article/abs/pii/S0965856416303494)

### Data Models and Data Processing Scripts
1. Big Query SQL models (upstream to downstream SQL)
   * 2 week [sample](https://dbt-docs.dds.dot.ca.gov/index.html#!/model/model.calitp_warehouse.fct_stop_time_updates_sample)
   * [actual arrivals](https://dbt-docs.dds.dot.ca.gov/index.html#!/model/model.calitp_warehouse.int_gtfs_rt__trip_updates_trip_stop_day_map_grouping)
   * [daily stop time metrics](https://dbt-docs.dds.dot.ca.gov/index.html#!/model/model.calitp_warehouse.fct_stop_time_metrics) 
   * [daily stop metrics](https://dbt-docs.dds.dot.ca.gov/index.html#!/model/model.calitp_warehouse.fct_trip_updates_stop_metrics) --> desired future aggregation: move to this stop on a June 2025 weekday instead of this stop on June 1, 2025
   * [GitHub issue](https://github.com/cal-itp/data-infra/issues/4101)


2. Python scripts
   * [report notebook](https://github.com/cal-itp/data-analyses/blob/main/rt_predictions/rt_trip_updates_report.ipynb)
   * [Makefile](https://github.com/cal-itp/data-analyses/blob/main/rt_predictions/Makefile)
   * [download warehouse tables](https://github.com/cal-itp/data-analyses/blob/main/rt_predictions/download_warehouse_tables.py)
   * [prep data](https://github.com/cal-itp/data-analyses/blob/main/rt_predictions/prep_data.py)