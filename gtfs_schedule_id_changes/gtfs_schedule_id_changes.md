<details>

<summary>show code</summary>



```python
from siuba import *
from siuba.sql import LazyTbl
from siuba.dply import vector as vec
from siuba.dply.vector import n

from plotnine import *

from sqlalchemy import create_engine

# TODO: once calitp package is up, should be able to use
# that to get the relevant tables
%run ../_gtfs_schedule_views/ipynb/_setup.ipynb

tbl = AutoTable(
    engine,
    lambda s: s.replace(".", "_").replace("test_", ""),
    lambda s: "test_" not in s and "__staging" not in s
)
```


</details>

## Grab specific agency feeds

<details>

<summary>show code</summary>



```python
DATE_START="2021-04-16"
DATE_END="2021-05-16"

THE_FUTURE="2099-01-01"


EXAMPLE_AGENCY_NAMES = [
    "Tahoe Truckee Area Regional Transportation",
    "Metro",
    "Monterey-Salinas Transit",
    "Fairfield and Suisun Transit",
    "AC Transit",
    "Big Blue Bus"
]
```


</details>

<details>

<summary>show code</summary>



```python
tbl_feeds = (
    tbl.gtfs_schedule_calitp_status()
    >> select(
        _.calitp_itp_id == _.itp_id, _.calitp_url_number == _.url_number, _.agency_name
    )
    >> filter(_.agency_name.isin(EXAMPLE_AGENCY_NAMES))

    >> mutate(agency_name = _.agency_name + " (" + _.calitp_url_number.astype(str) + ")")
)

# will be used to limit the number of feeds shown in data
join_feeds = inner_join(_, tbl_feeds, ["calitp_itp_id", "calitp_url_number"])

tbl_feeds
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=100000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>calitp_itp_id</th>
      <th>calitp_url_number</th>
      <th>agency_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>0</td>
      <td>Big Blue Bus (0)</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1</td>
      <td>Fairfield and Suisun Transit (1)</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>2</td>
      <td>Fairfield and Suisun Transit (2)</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>0</td>
      <td>AC Transit (0)</td>
    </tr>
    <tr>
      <th>4</th>
      <td>8</td>
      <td>1</td>
      <td>Monterey-Salinas Transit (1)</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 3 columns</p><p># .. may have more rows</p></div>



<details>

<summary>show code</summary>



```python
def query_id_changes(start_table, end_table, id_vars, agg=False):
    sym_id_vars = [_[k] for k in id_vars]

    is_in_start = start_table >> select(*id_vars) >> mutate(is_in_start=True)
    is_in_end = end_table >> select(*id_vars) >> mutate(is_in_end=True)

    tallies = (
        is_in_start
        >> full_join(_, is_in_end, id_vars)
        >> count(*sym_id_vars, _.is_in_start, _.is_in_end)
        >> mutate(
            status=case_when(
                _,
                {
                    _.is_in_end.isna(): "Removed",
                    _.is_in_start.isna(): "Added",
                    True: "Unchanged",
                },
            )
        )
    )
    
    if agg:
        return tallies >> count(*sym_id_vars[:-1], _.status)
    
    return tallies

def fetch_date(table, date, future_date = THE_FUTURE):
    return table >> filter(_.calitp_extracted_at <= date, _.calitp_deleted_at.fillna(future_date) > date)
```


</details>

## Route id changes

<details>

<summary>show code</summary>



```python
routes_start = (tbl.gtfs_schedule_type2_routes()
    >> filter(_.calitp_extracted_at <= DATE_START, _.calitp_deleted_at.fillna(THE_FUTURE) > DATE_START)
)

routes_end = (tbl.gtfs_schedule_type2_routes()
    >> filter(_.calitp_extracted_at <= DATE_END, _.calitp_deleted_at.fillna(THE_FUTURE) > DATE_END)
)
```


</details>

<details>

<summary>show code</summary>



```python
keep_keys = ("calitp_itp_id", "calitp_url_number", "route_id")

route_id_changes = (
    routes_start
    >> select(*keep_keys)
    >> mutate(is_in_start=True)
    >> full_join(
        _, routes_end >> select(*keep_keys) >> mutate(is_in_end=True), keep_keys
    )
    >> count(_.calitp_itp_id, _.calitp_url_number, _.is_in_start, _.is_in_end)
    >> mutate(
        status=case_when(
            _,
            {
                _.is_in_end.isna(): "Removed",
                _.is_in_start.isna(): "Added",
                True: "Unchanged",                
            },
        )
    )
)
```


</details>

<details>

<summary>show code</summary>



```python
(
    route_id_changes
    >> join_feeds
    >> collect()
    >> ggplot(aes("agency_name", "n", fill="status"))
    + geom_col()
    + theme(axis_text_x=element_text(angle=45, hjust=1))
    + labs(title = "Route ID Changes Between %s - %s"% (DATE_START, DATE_END))
)
```


</details>


    
![png](gtfs_schedule_id_changes_files/gtfs_schedule_id_changes_8_0.png)
    





    <ggplot: (314534320)>



## Trip ID changes

<details>

<summary>show code</summary>



```python
trips_start = fetch_date(tbl.gtfs_schedule_type2_trips(), DATE_START)
trips_end = fetch_date(tbl.gtfs_schedule_type2_trips(), DATE_END)

(
    query_id_changes(
        trips_start,
        trips_end,
        ["calitp_itp_id", "calitp_url_number", "trip_id"],
        agg=True,
    )
    >> join_feeds
    >> collect()
    >> ggplot(aes("agency_name", "n", fill="status"))
    + geom_col()
    + theme(axis_text_x=element_text(angle=45, hjust=1))
    + labs(title = "Trip ID Changes Between %s - %s"% (DATE_START, DATE_END))
    
)
```


</details>


    
![png](gtfs_schedule_id_changes_files/gtfs_schedule_id_changes_10_0.png)
    





    <ggplot: (-9223372036540011022)>



## Stop ID changes

<details>

<summary>show code</summary>



```python
stops_start = fetch_date(tbl.gtfs_schedule_type2_stops(), DATE_START)
stops_end = fetch_date(tbl.gtfs_schedule_type2_stops(), DATE_END)

(
    query_id_changes(
        stops_start,
        stops_end,
        ["calitp_itp_id", "calitp_url_number", "stop_id"],
        agg=True,
    )
    >> join_feeds
    >> collect()
    >> ggplot(aes("agency_name", "n", fill="status"))
    + geom_col()
    + theme(axis_text_x=element_text(angle=45, hjust=1))
    + labs(title = "Stop ID Changes Between %s - %s"% (DATE_START, DATE_END))
    
)
```


</details>


    
![png](gtfs_schedule_id_changes_files/gtfs_schedule_id_changes_12_0.png)
    





    <ggplot: (314770054)>


