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

* gtfs_schedule_trips
    * stops_and_times
    * schedule_daily

Tables used for questions:

* **Stop times by location type** - stops_and_times
* **Stops in service** - stops_and_times + (gtfs_schedule_trips + schedule_daily)

## Stop times by location types

It looks like stops are either not coded for location type or type 1 (stations).

<details>

<summary>show code</summary>



```python
(tbl_stops_and_times
  >> count(_.location_type)
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
      <th>location_type</th>
      <th>n</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>None</td>
      <td>5524733</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0</td>
      <td>4054292</td>
    </tr>
  </tbody>
</table>
<p>2 rows × 2 columns</p><p># .. may have more rows</p></div>



## Stop sequences

<details>

<summary>show code</summary>



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


</details>




<div><pre># Source: lazy query
# DB Conn: Engine(bigquery://cal-itp-data-infra/?maximum_bytes_billed=1000000000)
# Preview:
</pre><table border="0" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>max</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>136</td>
    </tr>
  </tbody>
</table>
<p>1 rows × 1 columns</p><p># .. may have more rows</p></div>



## Stops in service on a specific day

<details>

<summary>show code</summary>



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


</details>

<details>

<summary>show code</summary>



```python
# counts number of stop *times*
recent_trip_stops >> count()
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
      <td>7023803</td>
    </tr>
  </tbody>
</table>
<p>1 rows × 1 columns</p><p># .. may have more rows</p></div>



<details>

<summary>show code</summary>



```python
# counts stops that are being serviced. note that the distinct
# ensures we do not count a physical stop more than once
recent_agency_stops = recent_trip_stops >> distinct(*pk_col, _.stop_id)
```


</details>

<details>

<summary>show code</summary>



```python
recent_agency_stops >> count()
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
      <td>92222</td>
    </tr>
  </tbody>
</table>
<p>1 rows × 1 columns</p><p># .. may have more rows</p></div>



<details>

<summary>show code</summary>



```python
recent_agency_stops >> count(*pk_col)
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
      <td>13283</td>
    </tr>
    <tr>
      <th>1</th>
      <td>142</td>
      <td>0</td>
      <td>5171</td>
    </tr>
    <tr>
      <th>2</th>
      <td>235</td>
      <td>0</td>
      <td>5171</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>0</td>
      <td>4579</td>
    </tr>
    <tr>
      <th>4</th>
      <td>278</td>
      <td>0</td>
      <td>3427</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 3 columns</p><p># .. may have more rows</p></div>



## Stops out of service


<details>

<summary>show code</summary>



```python
tbl_stops_and_times
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
      <th>trip_id</th>
      <th>stop_id</th>
      <th>stop_sequence</th>
      <th>arrival_time</th>
      <th>departure_time</th>
      <th>stop_headsign</th>
      <th>pickup_type</th>
      <th>drop_off_type</th>
      <th>continuous_pickup</th>
      <th>continuous_drop_off</th>
      <th>shape_dist_traveled</th>
      <th>timepoint</th>
      <th>calitp_extracted_at</th>
      <th>stop_timezone</th>
      <th>stop_code</th>
      <th>stop_desc</th>
      <th>stop_lat</th>
      <th>stop_lon</th>
      <th>stop_name</th>
      <th>location_type</th>
      <th>parent_station</th>
      <th>level_id</th>
      <th>zone_id</th>
      <th>wheelchair_boarding</th>
      <th>platform_code</th>
      <th>stop_url</th>
      <th>tts_stop_name</th>
      <th>stop_sequence_rank</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>372</td>
      <td>0</td>
      <td>009e81cc-688d-4a96-9049-9b008afb4306</td>
      <td>688a134e-8c33-4776-b276-d03da1dd587c</td>
      <td>0</td>
      <td>14:07:00</td>
      <td>14:07:00</td>
      <td>None</td>
      <td>0</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>0</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>None</td>
      <td>23358</td>
      <td>"Cache Creek Casino"</td>
      <td>38.733355</td>
      <td>-122.142352</td>
      <td>Cache Creek Casino Resort</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>372</td>
      <td>0</td>
      <td>009e81cc-688d-4a96-9049-9b008afb4306</td>
      <td>4fb90529-df3d-4817-b289-7913bab8e803</td>
      <td>100</td>
      <td>14:16:00</td>
      <td>14:16:00</td>
      <td>None</td>
      <td>0</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>11067.33</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>None</td>
      <td>23526</td>
      <td>None</td>
      <td>38.707340</td>
      <td>-122.048604</td>
      <td>Hwy. 16 at Rd. 85 (Capay) EB</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>2</td>
    </tr>
    <tr>
      <th>2</th>
      <td>372</td>
      <td>0</td>
      <td>009e81cc-688d-4a96-9049-9b008afb4306</td>
      <td>7b7a754a-71f2-4796-b7c9-c9bfda47d8e6</td>
      <td>140</td>
      <td>14:20:00</td>
      <td>14:20:00</td>
      <td>None</td>
      <td>0</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>14827.6</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>None</td>
      <td>23355</td>
      <td>None</td>
      <td>38.692962</td>
      <td>-122.016728</td>
      <td>Yolo at Grafton (Esparto) SB</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>372</td>
      <td>0</td>
      <td>009e81cc-688d-4a96-9049-9b008afb4306</td>
      <td>14989d21-0e8e-43d1-93f9-fbd511ed63fd</td>
      <td>168</td>
      <td>14:26:00</td>
      <td>14:26:00</td>
      <td>None</td>
      <td>0</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>20391.6</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>None</td>
      <td>23353</td>
      <td>""</td>
      <td>38.679074</td>
      <td>-121.968471</td>
      <td>Railroad at Main (Madison)</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>372</td>
      <td>0</td>
      <td>009e81cc-688d-4a96-9049-9b008afb4306</td>
      <td>ce57f7f5-4110-40e8-a062-fb58b27e5dfa</td>
      <td>242</td>
      <td>14:39:00</td>
      <td>14:39:00</td>
      <td>None</td>
      <td>0</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>35749.63</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>None</td>
      <td>23293</td>
      <td>None</td>
      <td>38.675253</td>
      <td>-121.801838</td>
      <td>W. Lincoln at Rd 98 EB</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>5</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 30 columns</p><p># .. may have more rows</p></div>


