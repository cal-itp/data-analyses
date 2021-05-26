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

* [report link](https://docs.google.com/document/d/1-iXNdl4f2EbYayKg3e7jkTALAcNzht9yGQuyIOtPLc4/edit#)

```python
%run 0_data_model.ipynb
from plotnine import *
from siuba import *
```

```python
pk_cols = (_.calitp_itp_id, _.calitp_url_number)
```

<!-- #region toc-hr-collapsed=true toc-nb-collapsed=true -->
## Table previews
<!-- #endregion -->

```python
tbl.gtfs_schedule_feed_info()
```

```python
# one row per trip [route [ agency]]
tbl_feed_trips
```

## Metrics for most recent published feed

```python
tbl_feed_trips >> distinct(*pk_cols, _.route_id) >> count(*pk_cols)
```

* Date published
* Number of routes in service
* Number of stops in service


### Date feed was published

```python
(
    tbl.gtfs_schedule_feed_info()
    >> collect()
    >> mutate(feed_start_date=_.feed_start_date.astype("datetime64[ns]"))
    >> ggplot(aes("''", "feed_start_date")) + geom_boxplot()
)
```

### Number of routes in service

```python
from siuba.dply.forcats import fct_reorder

caterpiller_plot = lambda d: d >> mutate(
    calitp_itp_id=fct_reorder(_.calitp_itp_id, _.n)
)

(
    tbl.gtfs_schedule_routes()
    >> count(*pk_cols, sort=True)
    >> collect()
    >> pipe(caterpiller_plot)
    >> ggplot(aes("n")) + geom_histogram() + labs(title="Number of routes per feed")
    #     >> mutate(calitp_itp_id=fct_reorder(_.calitp_itp_id, _.n))
    #     >> ggplot(aes("calitp_itp_id", "n")) + geom_point() + coord_flip()
)
```

### Number of stops in service

```python
(
    tbl.gtfs_schedule_stops()
    >> count(*pk_cols, sort=True)
    >> collect()
    >> pipe(caterpiller_plot)
    >> ggplot(aes("n")) + geom_histogram()
     + labs(title = "Number of stops per feed")
)
```

## Aggregated metrics

```python
trip_metrics = (
    tbl_trip_stops
    >> group_by(*pk_cols, _.trip_id)
    >> summarize(
        trip_last_arrival_time=_.arrival_time.max(),
        trip_first_arrival_time=_.arrival_time.min(),
        trip_last_departure_time=_.departure_time.max(),
        trip_first_departure_time=_.departure_time.min(),
        n_stop_times = n(_)
    )
    >> ungroup()
)
```

```python
trip_metrics
```

```python
tbl_trip_stops 
```

## Monthly GTFS Quality Report
