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

## Setup

```python
from siuba import *
from siuba.sql import LazyTbl
from siuba.dply import vector as vec
from siuba.dply.vector import n
from siuba.sql import sql_raw

from sqlalchemy import create_engine

%run _setup.ipynb

tbl = AutoTable(
    engine,
    lambda s: s.replace(".", "_").replace("test_", ""),
    lambda s: "test_" in s and "__staging" not in s
)
```

```python
pk_str = ("calitp_itp_id", "calitp_url_number")
pk_cols = (_.calitp_itp_id, _.calitp_url_number)
tbl_agency = tbl.gtfs_schedule_agency()
tbl_routes = tbl.gtfs_schedule_routes()
tbl_trips = tbl.gtfs_schedule_trips()

```

```python
#tbl_agency
```

<!-- #region toc-hr-collapsed=true toc-nb-collapsed=true -->
## Main table previews
<!-- #endregion -->

```python
#tbl_agency
```

```python
#tbl_routes
```

```python
#tbl_trips
```

<!-- #region toc-hr-collapsed=true toc-nb-collapsed=true -->
## Model agency routes view
<!-- #endregion -->

<!-- #region toc-hr-collapsed=true toc-nb-collapsed=true -->
### Fill in implicit agency_ids
<!-- #endregion -->

```python
expr_fill_id = _.agency_id.fillna(_.calitp_itp_id.astype(str))

tbl_agency_trips = (
    tbl_trips
    >> left_join(
        _,
        tbl_routes >> mutate(agency_id=expr_fill_id) >> select(-_.calitp_extracted_at),
        [*pk_str, "route_id"],
    )
    >> left_join(
        _,
        tbl_agency >> mutate(agency_id=expr_fill_id) >> select(-_.calitp_extracted_at),
        [*pk_str, "agency_id"],
    )
)
```

## Model stops and times

```python
from siuba.dply.vector import dense_rank

tbl_stops_and_times = (
    tbl.gtfs_schedule_stop_times()
    # TODO: note that we can't parse to time here, because times may be
    # > 24:00:00. However, bigquery should sort these times okay, so we can
    # calc the min and max, and then do some post-processing on them, based
    # on their calendar schedules
    #   >> mutate(
    #       arrival_time = sql_raw('PARSE_TIME("%T", arrival_time)'),
    #       departure_time =sql_raw('PARSE_TIME("%T", departure_time)')
    #   )
    >> left_join(_, tbl.gtfs_schedule_stops() >> select(-_.calitp_extracted_at), [*pk_str, "stop_id"])
    >> group_by(_.trip_id)
    >> mutate(
        stop_sequence=_.stop_sequence.astype(int),
        stop_sequence_rank=dense_rank(_.stop_sequence, na_option="keep"),
    )
    >> ungroup()
)
```

## Model schedule daily


### Gather function

```python
from siuba import gather
from siuba.sql import LazyTbl
from siuba.dply.verbs import singledispatch2

@gather.register(LazyTbl)
def _gather_sql(__data, key="key", value="value", *args, drop_na=False, convert=False):
    from siuba.dply.verbs import var_select, var_create
    from siuba.sql.verbs import lift_inner_cols
    import pandas as pd
    from sqlalchemy import sql

    if not args:
        raise NotImplementedError("must specify columns to gather as *args")

    # most recent select statement and inner columns ----
    sel = __data.last_op
    columns = lift_inner_cols(sel)
    
    # tidy select variables for gathering ----
    var_list = var_create(*args)
    od = var_select(pd.Series(columns.keys()), *var_list)

    # get sql columns corresponding to variables ----
    value_vars = [columns[k] for k in od]
    id_vars = [columns[k] for k in columns.keys() if k not in od]

    # union each key variable into long format ----
    queries = []
    for value_col in value_vars:
        # TODO: may require CTE
        subquery = (
            sel
            .with_only_columns(
                [
                    *id_vars,
                    sql.literal(value_col.name).label(key),
                    value_col.label(value),
                ]
            )
        )
        queries.append(subquery)
        
    # make union all into a subquery for now, just to be safe, since
    # siuba might not respond well to a CompoundSelect
    return __data.append_op(sql.union_all(*queries).select())


```

### Query

```python
from siuba.sql import sql_raw

process_cal_dates = mutate(
    date=sql_raw('PARSE_DATE("%Y%m%d", date)')
) >> select(-_["exception_type", "calitp_extracted_at"], _.service_date == _.date)

date_include = (
    tbl.gtfs_schedule_calendar_dates()
    >> filter(_.exception_type == "1")
    >> mutate(service_inclusion=True)
    >> process_cal_dates
)
date_exclude = (
    tbl.gtfs_schedule_calendar_dates()
    >> filter(_.exception_type == "2")
    >> mutate(service_exclusion=True)
    >> process_cal_dates
)

tbl_schedule_daily = (
    tbl.gtfs_schedule_calendar()
    # parse dates
    >> mutate(
        start_date=sql_raw('PARSE_DATE("%Y%m%d", start_date)'),
        end_date=sql_raw('PARSE_DATE("%Y%m%d", end_date)'),
    )
    # convert wide weekday to long
    >> gather("day_name", "service_indicator", _["monday":"sunday"])
    >> mutate(day_name=_.day_name.str.title())
    # expand all dates range using calendar
    # needs to be an inner join, in case a scheduled interval is e.g. 1 day,
    # since gathering will still produce 7 rows (1 per day of week).
    >> inner_join(
        _,
        tbl.views_dim_date() >> select(_.day_name, _.full_date),
        sql_on=(
            lambda lhs, rhs: (lhs.day_name == rhs.day_name)
            & (lhs.start_date <= rhs.full_date)
            & (lhs.end_date >= rhs.full_date)      # end date is inclusive
        ),
    )
    >> select(-_.startswith("day_name"))
    >> rename(service_date="full_date", service_cal_start_date="start_date", service_cal_end_date="end_date")
    # full join, since an agency can define a schedule using only calendar dates
    # e.g. every day a service runs is specified using exceptions
    >> full_join(_, date_include, [*pk_str, "service_id", "service_date"])
    >> full_join(_, date_exclude, [*pk_str, "service_id", "service_date"])
    >> mutate(is_in_service = (_.service_indicator == "1") & ~_.service_exclusion.fillna(False) | _.service_inclusion.fillna(False))
)

# sanity check that vals are either 0 or 1
#tbl_schedule_daily >> distinct(_.is_in_service)
```

```python
#tbl_schedule_daily >> arrange(_.calitp_itp_id, _.calitp_url_number, _.service_id, _.service_date)
```

```python
#tbl_schedule_daily >> count()
```

```python
# (        tbl_schedule_daily
#         >> filter(_.service_date >= "2021-04-01", _.service_date < "2021-05-01", _.calitp_itp_id == 3, _.calitp_url_number == 0)
#         >> count(_.service_id)
        
# )
```

```python tags=[]

```
