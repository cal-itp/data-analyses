# Solutions for Congested Corridors Program (SCCP) and Local Partnership Program (LPP) Level of Transit Delay Metrics

We helped generate transit delay metrics for the 2022 SCCP/LPP, as specified in the [guidebook](https://catc.ca.gov/-/media/ctc-media/documents/ctc-workshops/2022/sb-1/performance-measurement-guidebook-final-draft.pdf) (page 66). 

The methodology we settled on is decribed below, and you can also reference the notebook for a few live examples. It may also be helpful to view `rt_delay/rt_analysis/sccp_tools.py` and the `add_corridor` and `corridor_metrics` methods in `rt_delay/rt_analysis/rt_filter_map_plot.py`.

For future cycles, we may want to consider providing guidance to applicants on how to appropriately size their bounding box. Presumably we want a bounding box large enough to capture relevant local transit routes, but not so large that our metric includes too many trips and routes outside the proposed project's influence. Also, applicants would like additional guidance on how to estimate the metric for future conditons after the project is built.


## Example communication to applicant (AC Transit, 2022)

> I noticed you provided three polygons quite close together for your corridor, to avoid potential double-counting and other measurement issues I’ve combined them into a single polygon. Let me know if you have further questions on this.
> 
> The schedule-based metric for the corridor provided is 49 minutes. This is a daily average of the sum of median trip stop delays along the corridor. To further explain, we took each corridor trip that we have data for and looked at the delay in comparison to the schedule at each stop, after subtracting off any delay present as the trip entered the corridor. For each trip we then took the median delay of all stops along the corridor, and summed these medians to create the metric for each day. The final metric is a simple daily average of the daily metric for a nine-day period (April 30 2022 to May 8 2022).
>   
> The speed-based metric for the corridor provided is 252 minutes. This is a daily average of the sum of delays for each trip traversing the corridor as compared to a reference speed of 16 miles per hour. To further explain, we took each corridor trip that we have data for and calculated the hypothetical time it would take for that trip to traverse the corridor at a speed of 16 mph. The difference between the actual time it took for the trip to traverse the corridor and that hypothetical time is the speed-based delay for that trip, and we summed those delays to create the metric for each day. As before, the final metric is an average for that nine-day period.
>  
> This metric is intended to provide a more consistent basis for comparison independent of scheduling practices.
>  
> To calculate which routes we consider to be along the corridor, we first filtered to routes stopping within the corridor. We then filtered our data for those trips to the subset of each trip from the last stop before entering the corridor to the first stop after leaving the corridor.
>  
> Here is a list of routes included in our analysis: 800, 72, 72M, 76, 376, 675, L, 667, 7.
>  
> Additionally, attached are a map of the corridor and transit stops within it, as well as a static version of our speedmaps for May 5 filtered to corridor routes.
>  
> I hope this helps you keep the application moving, please let me know if there’s anything I can clarify.
