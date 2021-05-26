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

* gtfs_schedule_trips
    * stops_and_times
    * schedule_daily

Tables used for questions:

* **Stop times by location type** - stops_and_times
* **Stops in service** - stops_and_times + (gtfs_schedule_trips + schedule_daily)


## Stop times by location types

It looks like stops are either not coded for location type or type 1 (stations).

```python
(tbl_stops_and_times
  >> count(_.location_type)
)
```

## Stop sequences

```python
from siuba.dply.vector import dense_rank

(
    tbl_stops_and_times
    >> group_by(_.trip_id)
    >> mutate(
        stop_sequence=_.stop_sequence.astype(int),
        stop_order=dense_rank(_.stop_sequence, na_option="keep"),
    )
    >> ungroup()
    >> summarize(max=_.stop_order.max())
)
```

## Stops in service on a specific day

```python
recent_trip_stops = (
    tbl_stops_and_times
    >> inner_join(
        _,
        tbl.gtfs_schedule_trips() >> select(_.trip_id, _.service_id, _.route_id, *pk_col),
        [*pk_str, "trip_id"],
    )
    >> inner_join(_, tbl_schedule_daily >> filter(_.service_date == DATE_END), [*pk_str, "service_id"])
)
```

```python
# counts number of stop *times*
recent_trip_stops >> count()
```

```python
# counts stops that are being serviced. note that the distinct
# ensures we do not count a physical stop more than once
recent_agency_stops = recent_trip_stops >> distinct(*pk_col, _.stop_id)
```

```python
recent_agency_stops >> count()
```

```python
recent_agency_stops >> count(*pk_col)
```

## Stops out of service


```python
tbl_stops_and_times
```
