# README

Use GTFS Real-Time vehicle positions to find where the positions we're capturing did not get near a scheduled stop.

To start, we calculate is which and how many vehicle positions got within 10 meters, 25 meters, 50 meters, and 100 meters of a stop for that trip.


Starting heuristics:
To get at whether real-time stops were serviced, we need to know whether we captured vehicle positions near the scheduled stop for that trip.

For stops that are eventually detoured or out of service, vehicle positions would likely get near enough (serving surrounding stops) while skipping the stop in question. Also, real-time vehicle positions information must be available enough that day for that stop (out of all the scheduled trips for that stop, from `stop_times`, how many of these trips had vehicle positions data? Let's assume at least 20% of the trips had real-time information.

All the following conditions must be met.
* zero vehicle positions within 10 meters, 25 meters, and 50 meters of a stop
* some threshold (50? 100? distinct vehicle positions) were captured within 100 meters of the stop
* at least 20% of the scheduled trips had vehicle positions
