<details>

<summary>show code</summary>



```python
%run 0_data_model.ipynb

pk_str = ["calitp_itp_id", "calitp_url_number"]
pk_col = (_.calitp_itp_id, _.calitp_url_number)

DATE_START = "2021-04-01"
DATE_END = "2021-05-01"
```


</details>

## Table overview

* agency_trips
* stops_and_times
* schedule_daily

Tables used for questions:

* **common route types** - agency_trips -> distinct routes (or gtfs_schedule_routes table)
* **routes in service** - agency_trips + schedule_daily
* **stops per route** - agency_trips + stops_and_times
* **stops per route in service** - everything above + schedule_dailly

<details>

<summary>show code</summary>



```python
tbl_agency_trips
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>calitp_itp_id</th>
      <th>calitp_url_number</th>
      <th>route_id</th>
      <th>service_id</th>
      <th>trip_id</th>
      <th>shape_id</th>
      <th>trip_headsign</th>
      <th>trip_short_name</th>
      <th>direction_id</th>
      <th>block_id</th>
      <th>wheelchair_accessible</th>
      <th>bikes_allowed</th>
      <th>calitp_extracted_at</th>
      <th>route_desc</th>
      <th>route_sort_order</th>
      <th>route_color</th>
      <th>continuous_drop_off</th>
      <th>route_url</th>
      <th>continuous_pickup</th>
      <th>route_short_name</th>
      <th>route_long_name</th>
      <th>agency_id</th>
      <th>route_text_color</th>
      <th>route_type</th>
      <th>agency_lang</th>
      <th>agency_name</th>
      <th>agency_fare_url</th>
      <th>agency_email</th>
      <th>agency_phone</th>
      <th>agency_url</th>
      <th>agency_timezone</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>3</td>
      <td>0</td>
      <td>574</td>
      <td>0</td>
      <td>30-Qr1RhyMHGk2G</td>
      <td>902</td>
      <td>Clockwise</td>
      <td>None</td>
      <td>0</td>
      <td>194098</td>
      <td>None</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>King East Clockwise</td>
      <td>None</td>
      <td>4B63AD</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>King East</td>
      <td>DASH King East Clockwise</td>
      <td>30</td>
      <td>FFFFFF</td>
      <td>3</td>
      <td>en</td>
      <td>LADOT</td>
      <td>https://store.ladottransit.com/</td>
      <td>None</td>
      <td>213-808-2273</td>
      <td>https://www.ladottransit.com/</td>
      <td>America/Los_Angeles</td>
    </tr>
    <tr>
      <th>1</th>
      <td>183</td>
      <td>0</td>
      <td>574</td>
      <td>0</td>
      <td>30-LoYj7FMNmm3Y</td>
      <td>902</td>
      <td>Clockwise</td>
      <td>None</td>
      <td>0</td>
      <td>194082</td>
      <td>None</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>King East Clockwise</td>
      <td>None</td>
      <td>4B63AD</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>King East</td>
      <td>DASH King East Clockwise</td>
      <td>30</td>
      <td>FFFFFF</td>
      <td>3</td>
      <td>en</td>
      <td>LADOT</td>
      <td>https://store.ladottransit.com/</td>
      <td>None</td>
      <td>213-808-2273</td>
      <td>https://www.ladottransit.com/</td>
      <td>America/Los_Angeles</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>0</td>
      <td>574</td>
      <td>0</td>
      <td>30-LoYj7FMNmm3Y</td>
      <td>902</td>
      <td>Clockwise</td>
      <td>None</td>
      <td>0</td>
      <td>194082</td>
      <td>None</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>King East Clockwise</td>
      <td>None</td>
      <td>4B63AD</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>King East</td>
      <td>DASH King East Clockwise</td>
      <td>30</td>
      <td>FFFFFF</td>
      <td>3</td>
      <td>en</td>
      <td>LADOT</td>
      <td>https://store.ladottransit.com/</td>
      <td>None</td>
      <td>213-808-2273</td>
      <td>https://www.ladottransit.com/</td>
      <td>America/Los_Angeles</td>
    </tr>
    <tr>
      <th>3</th>
      <td>183</td>
      <td>0</td>
      <td>574</td>
      <td>0</td>
      <td>30-IXNJSVmHMskp</td>
      <td>902</td>
      <td>Clockwise</td>
      <td>None</td>
      <td>0</td>
      <td>194098</td>
      <td>None</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>King East Clockwise</td>
      <td>None</td>
      <td>4B63AD</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>King East</td>
      <td>DASH King East Clockwise</td>
      <td>30</td>
      <td>FFFFFF</td>
      <td>3</td>
      <td>en</td>
      <td>LADOT</td>
      <td>https://store.ladottransit.com/</td>
      <td>None</td>
      <td>213-808-2273</td>
      <td>https://www.ladottransit.com/</td>
      <td>America/Los_Angeles</td>
    </tr>
    <tr>
      <th>4</th>
      <td>183</td>
      <td>0</td>
      <td>574</td>
      <td>0</td>
      <td>30-oA34l2Db98G2</td>
      <td>902</td>
      <td>Clockwise</td>
      <td>None</td>
      <td>0</td>
      <td>194098</td>
      <td>None</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>King East Clockwise</td>
      <td>None</td>
      <td>4B63AD</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>King East</td>
      <td>DASH King East Clockwise</td>
      <td>30</td>
      <td>FFFFFF</td>
      <td>3</td>
      <td>en</td>
      <td>LADOT</td>
      <td>https://store.ladottransit.com/</td>
      <td>None</td>
      <td>213-808-2273</td>
      <td>https://www.ladottransit.com/</td>
      <td>America/Los_Angeles</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 31 columns</p><p># .. may have more rows</p></div>



## Which route type is most common?

<details>

<summary>show code</summary>



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


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>route_type</th>
      <th>n</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Bus</td>
      <td>2789</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Ferry</td>
      <td>29</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Tram, Streetcar, Light rail</td>
      <td>18</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Subway, Metro</td>
      <td>16</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Rail</td>
      <td>15</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 2 columns</p><p># .. may have more rows</p></div>



## Routes in service on a specific day

<details>

<summary>show code</summary>



```python
recent_schedule = (
    tbl_schedule_daily
    >> filter(_.service_date.between(DATE_START, DATE_END))
    >> select(_.service_date, _.service_id, *pk_col)
)
```


</details>

<details>

<summary>show code</summary>



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


</details>

<details>

<summary>show code</summary>



```python
recent_trips >> count(_.last_trip)
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>last_trip</th>
      <th>n</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2021-05-01</td>
      <td>2217</td>
    </tr>
    <tr>
      <th>1</th>
      <td>None</td>
      <td>429</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2021-04-30</td>
      <td>56</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2021-04-15</td>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2021-04-28</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 2 columns</p><p># .. may have more rows</p></div>



## Number of stops per route

`stops_and_times` - one row per stop time, with stop information joined in.


<details>

<summary>show code</summary>



```python
tbl_stops_and_times >> count()
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>n</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>9579025</td>
    </tr>
  </tbody>
</table>
<p>1 rows × 1 columns</p><p># .. may have more rows</p></div>



<details>

<summary>show code</summary>



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


</details>

<details>

<summary>show code</summary>



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


</details>

<details>

<summary>show code</summary>



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


</details>

<details>

<summary>show code</summary>



```python
all_route_metrics >> count()
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>n</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2708</td>
    </tr>
  </tbody>
</table>
<p>1 rows × 1 columns</p><p># .. may have more rows</p></div>



<details>

<summary>show code</summary>



```python
active_route_metrics >> count()
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>n</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2217</td>
    </tr>
  </tbody>
</table>
<p>1 rows × 1 columns</p><p># .. may have more rows</p></div>



### Which route has the most stops locations?

<details>

<summary>show code</summary>



```python
most_stops = active_route_metrics >> filter(_.n_unique_stops == _.n_unique_stops.max())

most_stops
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>calitp_itp_id</th>
      <th>calitp_url_number</th>
      <th>route_id</th>
      <th>n_unique_trips</th>
      <th>n_unique_stops</th>
      <th>n_stop_times</th>
      <th>first_departure</th>
      <th>last_arrival</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>182</td>
      <td>0</td>
      <td>267-13139</td>
      <td>165</td>
      <td>287</td>
      <td>12192</td>
      <td>05:30:00</td>
      <td>20:07:00</td>
    </tr>
  </tbody>
</table>
<p>1 rows × 8 columns</p><p># .. may have more rows</p></div>



<details>

<summary>show code</summary>



```python
# however, note that this is not necessarily the route with the trip
# that stops the most times
most_stops
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>calitp_itp_id</th>
      <th>calitp_url_number</th>
      <th>route_id</th>
      <th>n_unique_trips</th>
      <th>n_unique_stops</th>
      <th>n_stop_times</th>
      <th>first_departure</th>
      <th>last_arrival</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>182</td>
      <td>0</td>
      <td>267-13139</td>
      <td>165</td>
      <td>287</td>
      <td>12192</td>
      <td>05:30:00</td>
      <td>20:07:00</td>
    </tr>
  </tbody>
</table>
<p>1 rows × 8 columns</p><p># .. may have more rows</p></div>



### Which route has the trip with most stop times?

<details>

<summary>show code</summary>



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


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>calitp_itp_id</th>
      <th>calitp_url_number</th>
      <th>route_id</th>
      <th>n_unique_trips</th>
      <th>n_unique_stops</th>
      <th>n_stop_times</th>
      <th>first_departure</th>
      <th>last_arrival</th>
      <th>n_max_trip_stops</th>
      <th>n_min_trip_stops</th>
      <th>n_max_trip_stop_times</th>
      <th>n_min_trip_stop_times</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>182</td>
      <td>0</td>
      <td>90-13139</td>
      <td>177</td>
      <td>282</td>
      <td>19118</td>
      <td>05:06:00</td>
      <td>24:24:00</td>
      <td>136</td>
      <td>68</td>
      <td>136</td>
      <td>68</td>
    </tr>
  </tbody>
</table>
<p>1 rows × 12 columns</p><p># .. may have more rows</p></div>



## Do agencies use multiple route entries for the same "route"?

The GTFS schedule defines a route as "a group of trips that are displayed to riders as a single service".

* Dead routes. There are two entries, with the same route long name, but 
* Similar route names, different directions. E.g. Green Line Southbound, Green Line Northbound.
* Duplicate active route names. E.g. Several active routes exist, and they all share the same long_name.

### Similar route names, different directions

<details>

<summary>show code</summary>



```python
route_names = tbl.gtfs_schedule_routes() >> select(*pk_col, _.route_id, _.route_short_name, _.route_long_name)
```


</details>

<details>

<summary>show code</summary>



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


</details>




    29



<details>

<summary>show code</summary>



```python
opposite_routes >> filter(_.route_long_name.str.lower().str.contains("(north)|(south)|(east)|(west)|(clock)"))
```


</details>

    /Users/machow/.virtualenvs/notebooks/lib/python3.6/site-packages/pandas/core/strings.py:2001: UserWarning: This pattern has match groups. To actually get the groups, use str.extract.
      return func(self, *args, **kwargs)





<table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>calitp_itp_id</th>
      <th>calitp_url_number</th>
      <th>route_id</th>
      <th>n_unique_trips</th>
      <th>n_unique_stops_x</th>
      <th>n_stop_times</th>
      <th>first_departure_x</th>
      <th>last_arrival_x</th>
      <th>route_id_y</th>
      <th>first_departure_y</th>
      <th>last_arrival_y</th>
      <th>n_unique_stops_y</th>
      <th>route_short_name</th>
      <th>route_long_name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2</td>
      <td>0</td>
      <td>BCT109 SB</td>
      <td>50</td>
      <td>77</td>
      <td>3850</td>
      <td>10:00:00</td>
      <td>9:59:00</td>
      <td>BCT109 NB</td>
      <td>10:00:00</td>
      <td>9:59:00</td>
      <td>77</td>
      <td>None</td>
      <td>Beach Cities Transit 109 Southbound</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>0</td>
      <td>BCT109 NB</td>
      <td>50</td>
      <td>77</td>
      <td>3850</td>
      <td>10:00:00</td>
      <td>9:59:00</td>
      <td>BCT109 SB</td>
      <td>10:00:00</td>
      <td>9:59:00</td>
      <td>77</td>
      <td>None</td>
      <td>Beach Cities Transit 109 Northbound</td>
    </tr>
    <tr>
      <th>10</th>
      <td>3</td>
      <td>0</td>
      <td>572</td>
      <td>96</td>
      <td>29</td>
      <td>2880</td>
      <td>06:00:00</td>
      <td>19:38:00</td>
      <td>573</td>
      <td>06:00:00</td>
      <td>19:35:00</td>
      <td>29</td>
      <td>Wilshire Center/Koreatown Counterclockwise</td>
      <td>DASH Wilshire Center/Koreatown Counterclockwise</td>
    </tr>
    <tr>
      <th>11</th>
      <td>3</td>
      <td>0</td>
      <td>573</td>
      <td>96</td>
      <td>29</td>
      <td>2880</td>
      <td>06:00:00</td>
      <td>19:35:00</td>
      <td>572</td>
      <td>06:00:00</td>
      <td>19:38:00</td>
      <td>29</td>
      <td>Crenshaw</td>
      <td>DASH Crenshaw Clockwise</td>
    </tr>
    <tr>
      <th>14</th>
      <td>183</td>
      <td>0</td>
      <td>572</td>
      <td>96</td>
      <td>29</td>
      <td>2880</td>
      <td>06:00:00</td>
      <td>19:38:00</td>
      <td>573</td>
      <td>06:00:00</td>
      <td>19:35:00</td>
      <td>29</td>
      <td>Wilshire Center/Koreatown Counterclockwise</td>
      <td>DASH Wilshire Center/Koreatown Counterclockwise</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>81</th>
      <td>8</td>
      <td>1</td>
      <td>042</td>
      <td>16</td>
      <td>61</td>
      <td>496</td>
      <td>08:50:00</td>
      <td>20:40:00</td>
      <td>045</td>
      <td>08:15:00</td>
      <td>17:44:00</td>
      <td>60</td>
      <td>42</td>
      <td>East Salinas-Westridge</td>
    </tr>
    <tr>
      <th>82</th>
      <td>8</td>
      <td>1</td>
      <td>045</td>
      <td>16</td>
      <td>60</td>
      <td>496</td>
      <td>08:15:00</td>
      <td>17:44:00</td>
      <td>042</td>
      <td>08:50:00</td>
      <td>20:40:00</td>
      <td>61</td>
      <td>45</td>
      <td>East Market-Creekbridge</td>
    </tr>
    <tr>
      <th>83</th>
      <td>278</td>
      <td>0</td>
      <td>201</td>
      <td>66</td>
      <td>11</td>
      <td>792</td>
      <td>05:50:00</td>
      <td>22:31:00</td>
      <td>202</td>
      <td>05:45:00</td>
      <td>22:23:00</td>
      <td>11</td>
      <td>201</td>
      <td>Super Loop - Counterclockwise</td>
    </tr>
    <tr>
      <th>84</th>
      <td>278</td>
      <td>0</td>
      <td>202</td>
      <td>66</td>
      <td>11</td>
      <td>792</td>
      <td>05:45:00</td>
      <td>22:23:00</td>
      <td>201</td>
      <td>05:50:00</td>
      <td>22:31:00</td>
      <td>11</td>
      <td>202</td>
      <td>Super Loop - Clockwise</td>
    </tr>
    <tr>
      <th>92</th>
      <td>294</td>
      <td>0</td>
      <td>827</td>
      <td>4</td>
      <td>25</td>
      <td>76</td>
      <td>06:16:00</td>
      <td>17:39:00</td>
      <td>104</td>
      <td>06:10:00</td>
      <td>17:41:00</td>
      <td>35</td>
      <td>ACE Yellow</td>
      <td>Great America ACE Station -  South Santa Clara</td>
    </tr>
  </tbody>
</table>
<p>11 rows × 14 columns</p>



### Duplicate active route names

<details>

<summary>show code</summary>



```python
active_route_metrics >> count()
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>n</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2217</td>
    </tr>
  </tbody>
</table>
<p>1 rows × 1 columns</p><p># .. may have more rows</p></div>



<details>

<summary>show code</summary>



```python
active_route_names = active_route_metrics >> inner_join(
    _, route_names, [*pk_str, "route_id"]
)

unique_route_names = tbl.gtfs_schedule_routes() >> distinct(_.calitp_itp_id, _.calitp_url_number, _.route_long_name)

duplicate_route_names = (
    active_route_names
    >> inner_join(_, unique_route_names, [*pk_str, "route_long_name"])
)

duplicate_route_names >> count(_.calitp_itp_id, _.calitp_url_number)
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>calitp_itp_id</th>
      <th>calitp_url_number</th>
      <th>n</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>182</td>
      <td>0</td>
      <td>122</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4</td>
      <td>0</td>
      <td>113</td>
    </tr>
    <tr>
      <th>2</th>
      <td>278</td>
      <td>0</td>
      <td>74</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>0</td>
      <td>61</td>
    </tr>
    <tr>
      <th>4</th>
      <td>183</td>
      <td>0</td>
      <td>61</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 3 columns</p><p># .. may have more rows</p></div>



<details>

<summary>show code</summary>



```python
duplicate_route_names >> filter(_.calitp_itp_id == 182) >> count(_.route_long_name)
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>route_long_name</th>
      <th>n</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Metro Local Line</td>
      <td>101</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Metro Rapid Line</td>
      <td>11</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Metro Express Line</td>
      <td>6</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Metro Limited Line</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Metro G Line (Orange) 901</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 2 columns</p><p># .. may have more rows</p></div>



### Dead routes

<details>

<summary>show code</summary>



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


</details>

<details>

<summary>show code</summary>



```python
dead_routes >> count()
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>n</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>41</td>
    </tr>
  </tbody>
</table>
<p>1 rows × 1 columns</p><p># .. may have more rows</p></div>



<details>

<summary>show code</summary>



```python
dead_routes >> count(_.calitp_itp_id)
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>calitp_itp_id</th>
      <th>n</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>232</td>
      <td>29</td>
    </tr>
    <tr>
      <th>1</th>
      <td>142</td>
      <td>3</td>
    </tr>
    <tr>
      <th>2</th>
      <td>235</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>314</td>
      <td>2</td>
    </tr>
    <tr>
      <th>4</th>
      <td>70</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 2 columns</p><p># .. may have more rows</p></div>



<details>

<summary>show code</summary>



```python
(
    dead_routes
    >> select(_.startswith("calitp"), _.route_long_name, _.contains(""))
    >> arrange(_.calitp_itp_id, _.calitp_url_number)
)
```


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>calitp_itp_id</th>
      <th>calitp_url_number</th>
      <th>route_long_name</th>
      <th>route_id_x</th>
      <th>n_stop_times_x</th>
      <th>route_id_y</th>
      <th>n_stop_times_y</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>70</td>
      <td>0</td>
      <td>Santa Rosa, Rohnert Park, Cotati, Petaluma</td>
      <td>1052</td>
      <td>0</td>
      <td>1035</td>
      <td>1728</td>
    </tr>
    <tr>
      <th>1</th>
      <td>70</td>
      <td>0</td>
      <td>Cloverdale, Healdsburg, Windsor, Santa Rosa</td>
      <td>1053</td>
      <td>0</td>
      <td>1036</td>
      <td>2556</td>
    </tr>
    <tr>
      <th>2</th>
      <td>142</td>
      <td>0</td>
      <td>Fullerton - Newport Beach</td>
      <td>47A</td>
      <td>0</td>
      <td>47</td>
      <td>22886</td>
    </tr>
    <tr>
      <th>3</th>
      <td>142</td>
      <td>0</td>
      <td>Tustin - Newport Beach</td>
      <td>79A</td>
      <td>0</td>
      <td>79</td>
      <td>4308</td>
    </tr>
    <tr>
      <th>4</th>
      <td>142</td>
      <td>0</td>
      <td>Seal Beach - Orange</td>
      <td>42A</td>
      <td>0</td>
      <td>42</td>
      <td>13781</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 7 columns</p><p># .. may have more rows</p></div>


