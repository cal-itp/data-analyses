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
      <th>calitp_extracted_at_x</th>
      <th>parent_station</th>
      <th>stop_code</th>
      <th>zone_id</th>
      <th>stop_lat</th>
      <th>stop_url</th>
      <th>level_id</th>
      <th>stop_timezone</th>
      <th>stop_lon</th>
      <th>stop_desc</th>
      <th>calitp_extracted_at_y</th>
      <th>wheelchair_boarding</th>
      <th>platform_code</th>
      <th>tts_stop_name</th>
      <th>stop_name</th>
      <th>location_type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>111</td>
      <td>0</td>
      <td>781444</td>
      <td>11064</td>
      <td>68</td>
      <td>16:10:00</td>
      <td>16:10:00</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>None</td>
      <td>11064</td>
      <td>0</td>
      <td>38.688547</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>-121.186693</td>
      <td>Buses head NB</td>
      <td>2021-04-16</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>AMERICAN RIVER CANYON DR &amp; GREY CANYON DR (NB)</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>111</td>
      <td>0</td>
      <td>781449</td>
      <td>11160</td>
      <td>63</td>
      <td>11:05:00</td>
      <td>11:05:00</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>None</td>
      <td>11160</td>
      <td>0</td>
      <td>38.672938</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>-121.202331</td>
      <td>Buses head NB</td>
      <td>2021-04-16</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>MAIN AVE &amp; MADISON AVE (NB)</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>111</td>
      <td>0</td>
      <td>781375</td>
      <td>11065</td>
      <td>67</td>
      <td>11:09:00</td>
      <td>11:09:00</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>None</td>
      <td>11065</td>
      <td>0</td>
      <td>38.684880</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>-121.188730</td>
      <td>Buses head NB</td>
      <td>2021-04-16</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>AMERICAN RIVER CANYON DR &amp; BOULDER CANYON WAY (NB)</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>350</td>
      <td>1</td>
      <td>40090</td>
      <td>79013</td>
      <td>0</td>
      <td>11:44:00</td>
      <td>11:44:00</td>
      <td>None</td>
      <td>0</td>
      <td>0</td>
      <td>None</td>
      <td>None</td>
      <td>0.00000000</td>
      <td>1</td>
      <td>2021-04-29</td>
      <td>None</td>
      <td>79013</td>
      <td>None</td>
      <td>37.598792</td>
      <td>None</td>
      <td>None</td>
      <td>America/Los_Angeles</td>
      <td>-122.065656</td>
      <td>Terminal</td>
      <td>2021-04-29</td>
      <td>1</td>
      <td>None</td>
      <td>None</td>
      <td>Union Landing Transit Center</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2</td>
      <td>0</td>
      <td>BCT109 NB_SAT.T10</td>
      <td>163</td>
      <td>67</td>
      <td>16:35:30</td>
      <td>16:35:30</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>33.930741</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>-118.387028</td>
      <td>None</td>
      <td>2021-04-16</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>Imperial Hwy. / Nash St.</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 30 columns</p><p># .. may have more rows</p></div>


