# Conveyal Transit Paths

* GH Issue: https://github.com/cal-itp/data-analyses/issues/1098

## Conveyal SOP

* prepare a csv with lat, lon, and od column with 0 for origin and 1 for destination
    * allow freeform, use od col as id in upload
* run Conveyal Analysis: 8-10am, standard transit parameters, add JSON feed_id param
* run Regional Analysis: 120min max time, 5, 50, 95 %ile, get paths and travel times
