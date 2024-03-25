trip
* number of vp_idx = total_vp
* number of minutes = rt service min

trip-minute
* number of vp_idx
* 2+ in that minute
   * group by trip: count number of minutes with 2+, count number of minutes
   

trip-with-shape
* number of vp_idx = vp_in_shape


metrics
* spatial accuracy = vp_in_shape / total_vp
* minutes w gtfs = number of minutes / rt_service_min
* minutes of scheduled service w gtfs = number of minutes / sched_service_min (>1 potentially)
* minutes w gtfs 2+ = number of minutes with 2+ / rt_service_min
* pings_per_min = total_vp / rt_service_min
* rt_service vs scheduled service = rt_service_min / sched_service_min
