---
jupyter:
  jupytext:
    formats: md//md,ipynb//ipynb
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.11.2
  kernelspec:
    display_name: venv-notebooks
    language: python
    name: venv-notebooks
---

```python
%run 0_data_model.ipynb

pk_str = ["calitp_itp_id", "calitp_url_number"]
pk_col = (_.calitp_itp_id, _.calitp_url_number)

DATE_START = "2021-04-01"
DATE_END = "2021-05-01"
```

## Table overview

* agency_trips
* stops_and_times
* schedule_daily

Tables used for questions:

* **common route types** - agency_trips -> distinct routes (or gtfs_schedule_routes table)
* **routes in service** - agency_trips + schedule_daily
* **stops per route** - agency_trips + stops_and_times
* **stops per route in service** - everything above + schedule_dailly

```python
tbl_agency_trips
```

## Which route type is most common?

```python
top_cases = {
    "0": "Tram, Streetcar, Light rail",
    "1": "Subway, Metro",
    "2": "Rail",
    "3": "Bus",
    "4": "Ferry",
    "5": "Cable tram",
}

main_case_expr = {_.route_type == k: top_cases[k] for k in top_cases}

case_expr = case_when(_, {**main_case_expr, True: "Other"})

# note, could also use gtfs_schedule_routes directly
(
    tbl.gtfs_schedule_routes()
    >> mutate(route_type=case_expr)
    >> count(_.route_type)
)
```

## Routes in service on a specific day

```python
recent_schedule = (
    tbl_schedule_daily
    >> filter(_.service_date.between(DATE_START, DATE_END))
    >> select(_.service_date, _.service_id, *pk_col)
)
```

```python
recent_trips = (
    tbl_agency_trips
    >> left_join(
        _, recent_schedule, ["calitp_itp_id", "calitp_url_number", "service_id"],
    )
    >> group_by(*pk_col, _.route_id)
    >> summarize(
        last_trip=_.service_date.max(),
        first_trip=_.service_date.min(),
        n_trips=_.trip_id.count(),
    )
    #>> left_join(_, tbl_agency_routes, [*pk_str, "route_id"])
)
```

```python
recent_trips >> count(_.last_trip)
```

## Number of stops per route


`stops_and_times` - one row per stop time, with stop information joined in.


```python
tbl_stops_and_times >> count()
```

```python
trip_stop_times = (
    tbl_stops_and_times
    >> inner_join(
        _,
        tbl_agency_trips >> select(_.trip_id, _.service_id, _.route_id, *pk_col),
        [*pk_str, "trip_id"],
    )
)
```

```python
calc_route_metrics = group_by(*pk_col, _.route_id) >> summarize(
    n_unique_trips=_.trip_id.nunique(),
    n_unique_stops=_.stop_id.nunique(),
    n_stop_times=n(_),
    first_departure=_.departure_time.min(),
    last_arrival=_.arrival_time.max()
)

all_route_metrics = (trip_stop_times >> calc_route_metrics)
```

```python
active_routes = (
    trip_stop_times
    >> inner_join(
        _,
        recent_schedule >> filter(_.service_date == DATE_END),
        [*pk_str, "service_id"],
    )
)

active_route_metrics = (active_routes
    >> calc_route_metrics
)
```

```python
all_route_metrics >> count()
```

```python
active_route_metrics >> count()
```

### Which route has the most stops locations?

```python
most_stops = active_route_metrics >> filter(_.n_unique_stops == _.n_unique_stops.max())

most_stops
```

```python
# however, note that this is not necessarily the route with the trip
# that stops the most times
most_stops
```

### Which route has the trip with most stop times?

```python
n_trip_stops = (
    trip_stop_times
    >> distinct(*pk_col, _.route_id, _.trip_id, _.stop_id)
    >> count(*pk_col, _.route_id, _.trip_id)
    >> group_by(*pk_col, _.route_id)
    >> summarize(n_max_trip_stops=_.n.max(), n_min_trip_stops=_.n.min())
)

n_trip_stop_times = (
    trip_stop_times
    >> count(*pk_col, _.route_id, _.trip_id)
    >> group_by(*pk_col, _.route_id)
    >> summarize(n_max_trip_stop_times=_.n.max(), n_min_trip_stop_times=_.n.min(),)
)

(
    active_route_metrics
    >> inner_join(_, n_trip_stops, [*pk_str, "route_id"])
    >> inner_join(_, n_trip_stop_times, [*pk_str, "route_id"])
    >> filter(_.n_max_trip_stop_times == _.n_max_trip_stop_times.max())
)
```

## Do agencies use multiple route entries for the same "route"?

The GTFS schedule defines a route as "a group of trips that are displayed to riders as a single service".

* Dead routes. There are two entries, with the same route long name, but 
* Similar route names, different directions. E.g. Green Line Southbound, Green Line Northbound.
* Duplicate active route names. E.g. Several active routes exist, and they all share the same long_name.


### Similar route names, different directions

```python
route_names = tbl.gtfs_schedule_routes() >> select(*pk_col, _.route_id, _.route_short_name, _.route_long_name)
```

```python
opposite_routes = (
    active_route_metrics
    >> inner_join(_, active_route_metrics, [*pk_str, "n_unique_trips", "n_stop_times"])
    >> filter(_.route_id_x != _.route_id_y)
    >> rename(route_id="route_id_x")
    >> inner_join(
        _, route_names, [*pk_str, "route_id"]
    )
    >> collect()
)

opposite_routes >> filter(_.calitp_itp_id.isin([2, 3]))

opposite_routes.calitp_itp_id.nunique()
```

```python tags=[]
opposite_routes >> filter(_.route_long_name.str.lower().str.contains("(north)|(south)|(east)|(west)|(clock)"))
```

### Duplicate active route names

```python
active_route_metrics >> count()
```

```python
active_route_names = active_route_metrics >> inner_join(
    _, route_names, [*pk_str, "route_id"]
)

duplicate_route_names = (
    active_route_names
    >> inner_join(_, active_route_names, [*pk_str, "route_long_name"])
    >> filter(_.route_id_x != _.route_id_y)
)

duplicate_route_names >> count(_.calitp_itp_id, _.calitp_url_number)
```

```python
duplicate_route_names >> filter(_.calitp_itp_id == 182) >> count(_.route_long_name)
```

### Dead routes

```python
full_route_metrics = (
    tbl.gtfs_schedule_routes()
    >> left_join(_, active_route_metrics, [*pk_str, "route_id"])
    >> mutate(n_stop_times=_.n_stop_times.fillna(0))
    >> select(
        _.calitp_itp_id,
        _.calitp_url_number,
        _.route_id,
        _.n_stop_times,
        _.route_long_name,
    )
)

inactive_routes = full_route_metrics >> filter(_.n_stop_times == 0)
active_routes = full_route_metrics >> filter(_.n_stop_times > 0)

dead_routes = inner_join(inactive_routes, active_routes, [*pk_str, "route_long_name"],)
```

```python
dead_routes >> count()
```

```python
dead_routes >> count(_.calitp_itp_id)
```

```python
(
    dead_routes
    >> select(_.startswith("calitp"), _.route_long_name, _.contains(""))
    >> arrange(_.calitp_itp_id, _.calitp_url_number)
)
```
