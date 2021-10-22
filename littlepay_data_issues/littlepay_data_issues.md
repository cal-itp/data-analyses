```python
from calitp import get_engine
import pandas as pd
import pyarrow
```


```python
engine = get_engine()
connection = engine.connect()
```

## Duplicates in the `micropayment_device_transactions` table

We expect the number of duplicate `littlepay_transaction_id`, `micropayment_id` pairs to be `0`.


```python
duplicate_micropayment_transaction_pair_ids_sql = f"""
with 

distinct_id_pairs as (
    select littlepay_transaction_id, micropayment_id, count(*) as cnt
    from `cal-itp-data-infra.payments.micropayment_device_transactions` 
    group by 1, 2
),

duplicate_id_pairs as (
    select littlepay_transaction_id, micropayment_id, cnt
    from distinct_id_pairs
    where cnt > 1
)

select
    (select count(*) from distinct_id_pairs) as `Total_Distinct_ID_Pairs`,
    (select count(*) from duplicate_id_pairs) as `Duplicate_ID_Pairs`
"""

pd.read_sql_query(duplicate_micropayment_transaction_pair_ids_sql, connection)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Total_Distinct_ID_Pairs</th>
      <th>Duplicate_ID_Pairs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>22402</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



The duplicate data that we are seeing is:


```python
pd.read_sql_query("""
    select littlepay_transaction_id, micropayment_id
    from `cal-itp-data-infra.payments.micropayment_device_transactions` 
    group by 1, 2
    having count(*) > 1
    limit 10
""", connection)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>littlepay_transaction_id</th>
      <th>micropayment_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>75358478-c900-4af0-816c-04df7bc1f2be</td>
      <td>eba917b8-8a80-4886-b5a9-a27884712651</td>
    </tr>
  </tbody>
</table>
</div>



## Duplicate `littlepay_transaction_id` values in `device_transactions`

We expect the duplicate transaction ID counts for each agency to be `0`.


```python
duplicate_transaction_ids_sql = f"""
with 

all_ids as (
    select littlepay_transaction_id
    from `cal-itp-data-infra.payments.device_transactions`
    where participant_id = %(agency)s
),

duplicate_ids as (
    select littlepay_transaction_id
    from all_ids 
    group by 1
    having count(*) > 1
)

select
    %(agency)s as `Agency`,
    (select count(distinct littlepay_transaction_id) from all_ids) as `Total_Unique_IDs`,
    (select count(*) from duplicate_ids) as `Duplicate_IDs`
"""

pd.read_sql_query(duplicate_transaction_ids_sql, connection, params={'agency': 'mst'})
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Agency</th>
      <th>Total_Unique_IDs</th>
      <th>Duplicate_IDs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>mst</td>
      <td>19582</td>
      <td>1352</td>
    </tr>
  </tbody>
</table>
</div>




```python
pd.read_sql_query(duplicate_transaction_ids_sql, connection, params={'agency': 'sbmtd'})
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Agency</th>
      <th>Total_Unique_IDs</th>
      <th>Duplicate_IDs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>sbmtd</td>
      <td>527</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>



For MST, many of the duplicates have at least one entry where the route is `'Route Z'`.


```python
count_duplicate_transaction_records_sql = f"""
with 

duplicate_ids as (
    select littlepay_transaction_id, array_agg(route_id) as route_ids, count(distinct route_id) cnt_route_ids
    from `cal-itp-data-infra.payments.device_transactions` 
    where participant_id = %(agency)s
    group by 1
    having count(*) > 1
)

select
    %(agency)s as Agency,
    (select count(*) from duplicate_ids) as `Total_Duplicate_IDs`,
    (select count(*) from duplicate_ids where cnt_route_ids = 1) as `Duplicate_IDs_With_Same_Route`,
    (select count(*) from duplicate_ids where 'Route Z' in unnest(route_ids)) as `Duplicate_IDs_With_Route_Z`,
    (select count(*) from duplicate_ids where cnt_route_ids = 1 and 'Route Z' in unnest(route_ids)) as `Duplicate_IDs_With_Only_Route_Z`
"""

pd.read_sql_query(count_duplicate_transaction_records_sql, connection, params={'agency': 'mst'})
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Agency</th>
      <th>Total_Duplicate_IDs</th>
      <th>Duplicate_IDs_With_Same_Route</th>
      <th>Duplicate_IDs_With_Route_Z</th>
      <th>Duplicate_IDs_With_Only_Route_Z</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>mst</td>
      <td>1352</td>
      <td>75</td>
      <td>1330</td>
      <td>53</td>
    </tr>
  </tbody>
</table>
</div>




```python
sample_duplicate_transaction_records_sql = f"""
with 

duplicate_ids as (
    select littlepay_transaction_id
    from `cal-itp-data-infra.payments.device_transactions` 
    where participant_id = %(agency)s
    group by 1
    having count(*) > 1
)

select distinct *
from `cal-itp-data-infra.payments.device_transactions`
where participant_id = %(agency)s
    and littlepay_transaction_id in (select * from duplicate_ids)
order by littlepay_transaction_id
limit 10
"""

pd.read_sql_query(sample_duplicate_transaction_records_sql, connection, params={'agency': 'mst'})
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>participant_id</th>
      <th>customer_id</th>
      <th>device_transaction_id</th>
      <th>littlepay_transaction_id</th>
      <th>device_id</th>
      <th>device_id_issuer</th>
      <th>type</th>
      <th>transaction_outcome</th>
      <th>transction_deny_reason</th>
      <th>transaction_date_time_utc</th>
      <th>...</th>
      <th>zone_id</th>
      <th>route_id</th>
      <th>mode</th>
      <th>direction</th>
      <th>latitude</th>
      <th>longitude</th>
      <th>vehicle_id</th>
      <th>granted_zone_ids</th>
      <th>onward_zone_ids</th>
      <th>calitp_extracted_at</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>mst</td>
      <td>2e8bfdc1-ce64-4630-a6ff-3035b8e6581f</td>
      <td>36481BC5-7633-A21C-3EB6-484093BD91D3</td>
      <td>0004dabe-abde-4539-8c60-ba49ff7b8c25</td>
      <td>17F1267C</td>
      <td>Littlepay</td>
      <td>single</td>
      <td>allow</td>
      <td>None</td>
      <td>2021-07-22T22:21:48.000Z</td>
      <td>...</td>
      <td>None</td>
      <td>016</td>
      <td>BUS</td>
      <td>inbound</td>
      <td>36.65778350830078</td>
      <td>-121.7696304321289</td>
      <td>977</td>
      <td>None</td>
      <td>None</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>1</th>
      <td>mst</td>
      <td>2e8bfdc1-ce64-4630-a6ff-3035b8e6581f</td>
      <td>36481BC5-7633-A21C-3EB6-484093BD91D3</td>
      <td>0004dabe-abde-4539-8c60-ba49ff7b8c25</td>
      <td>17F1267C</td>
      <td>Littlepay</td>
      <td>single</td>
      <td>allow</td>
      <td>None</td>
      <td>2021-07-22T22:21:48.000Z</td>
      <td>...</td>
      <td>None</td>
      <td>Route Z</td>
      <td>BUS</td>
      <td>outbound</td>
      <td>36.65778350830078</td>
      <td>-121.7696304321289</td>
      <td>977</td>
      <td>None</td>
      <td>None</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>2</th>
      <td>mst</td>
      <td>e600de16-582c-4795-a433-7780adb4985d</td>
      <td>C7741986-B015-55C4-B448-8DD5085D8499</td>
      <td>000df49a-aff8-422a-abbe-7c6b97123b6c</td>
      <td>17F12523</td>
      <td>Littlepay</td>
      <td>single</td>
      <td>allow</td>
      <td>None</td>
      <td>2021-08-30T23:26:35.000Z</td>
      <td>...</td>
      <td>None</td>
      <td>Route Z</td>
      <td>BUS</td>
      <td>outbound</td>
      <td>36.41764831542969</td>
      <td>-121.32093048095703</td>
      <td>2103</td>
      <td>None</td>
      <td>None</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>3</th>
      <td>mst</td>
      <td>e600de16-582c-4795-a433-7780adb4985d</td>
      <td>C7741986-B015-55C4-B448-8DD5085D8499</td>
      <td>000df49a-aff8-422a-abbe-7c6b97123b6c</td>
      <td>17F12523</td>
      <td>Littlepay</td>
      <td>single</td>
      <td>allow</td>
      <td>None</td>
      <td>2021-08-30T23:26:35.000Z</td>
      <td>...</td>
      <td>None</td>
      <td>023</td>
      <td>BUS</td>
      <td>outbound</td>
      <td>36.41764831542969</td>
      <td>-121.32093048095703</td>
      <td>2103</td>
      <td>None</td>
      <td>None</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>4</th>
      <td>mst</td>
      <td>20962f6b-b7f1-4eac-9c94-71af37d000f8</td>
      <td>14B37ADA-8D27-8181-B93B-7BD833A47676</td>
      <td>0057ac79-a7f0-4314-93d2-9e53be215666</td>
      <td>17F125BE</td>
      <td>Littlepay</td>
      <td>single</td>
      <td>allow</td>
      <td>None</td>
      <td>2021-08-16T23:39:15.000Z</td>
      <td>...</td>
      <td>None</td>
      <td>044</td>
      <td>BUS</td>
      <td>inbound</td>
      <td>36.71390914916992</td>
      <td>-121.65453338623047</td>
      <td>2008</td>
      <td>None</td>
      <td>None</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>5</th>
      <td>mst</td>
      <td>20962f6b-b7f1-4eac-9c94-71af37d000f8</td>
      <td>14B37ADA-8D27-8181-B93B-7BD833A47676</td>
      <td>0057ac79-a7f0-4314-93d2-9e53be215666</td>
      <td>17F125BE</td>
      <td>Littlepay</td>
      <td>single</td>
      <td>allow</td>
      <td>None</td>
      <td>2021-08-16T23:39:15.000Z</td>
      <td>...</td>
      <td>None</td>
      <td>Route Z</td>
      <td>BUS</td>
      <td>outbound</td>
      <td>36.71390914916992</td>
      <td>-121.65453338623047</td>
      <td>2008</td>
      <td>None</td>
      <td>None</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>6</th>
      <td>mst</td>
      <td>eac3857e-ec07-4d86-9c7d-4bbc81305c24</td>
      <td>78BA0E4E-7B1B-7099-89A5-AA141640E0AE</td>
      <td>007ee236-05ee-49eb-a42b-90f5b284bf95</td>
      <td>17F12881</td>
      <td>Littlepay</td>
      <td>single</td>
      <td>allow</td>
      <td>None</td>
      <td>2021-08-02T22:57:08.000Z</td>
      <td>...</td>
      <td>None</td>
      <td>018</td>
      <td>BUS</td>
      <td>inbound</td>
      <td>36.59715270996094</td>
      <td>-121.85679626464844</td>
      <td>1724</td>
      <td>None</td>
      <td>None</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>7</th>
      <td>mst</td>
      <td>eac3857e-ec07-4d86-9c7d-4bbc81305c24</td>
      <td>78BA0E4E-7B1B-7099-89A5-AA141640E0AE</td>
      <td>007ee236-05ee-49eb-a42b-90f5b284bf95</td>
      <td>17F12881</td>
      <td>Littlepay</td>
      <td>single</td>
      <td>allow</td>
      <td>None</td>
      <td>2021-08-02T22:57:08.000Z</td>
      <td>...</td>
      <td>None</td>
      <td>Route Z</td>
      <td>BUS</td>
      <td>outbound</td>
      <td>36.59715270996094</td>
      <td>-121.85679626464844</td>
      <td>1724</td>
      <td>None</td>
      <td>None</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>8</th>
      <td>mst</td>
      <td>ab8bd3d6-6e5b-4026-a536-baf9fa19b7ef</td>
      <td>00CB3E8B-1C0D-458D-0610-3235AD1A5DD1</td>
      <td>0086e6ce-9e7d-43b6-b7f7-dee23c407102</td>
      <td>17F1255D</td>
      <td>Littlepay</td>
      <td>single</td>
      <td>allow</td>
      <td>None</td>
      <td>2021-09-22T02:21:19.000Z</td>
      <td>...</td>
      <td>None</td>
      <td>049</td>
      <td>BUS</td>
      <td>inbound</td>
      <td>36.676063537597656</td>
      <td>-121.65709686279297</td>
      <td>1721</td>
      <td>None</td>
      <td>None</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>9</th>
      <td>mst</td>
      <td>ab8bd3d6-6e5b-4026-a536-baf9fa19b7ef</td>
      <td>00CB3E8B-1C0D-458D-0610-3235AD1A5DD1</td>
      <td>0086e6ce-9e7d-43b6-b7f7-dee23c407102</td>
      <td>17F1255D</td>
      <td>Littlepay</td>
      <td>single</td>
      <td>allow</td>
      <td>None</td>
      <td>2021-09-22T02:21:19.000Z</td>
      <td>...</td>
      <td>None</td>
      <td>Route Z</td>
      <td>BUS</td>
      <td>outbound</td>
      <td>36.676063537597656</td>
      <td>-121.65709686279297</td>
      <td>1721</td>
      <td>None</td>
      <td>None</td>
      <td>2021-09-27</td>
    </tr>
  </tbody>
</table>
<p>10 rows × 23 columns</p>
</div>



What is `Route Z`?

## Duplicate `micropayment_id`

We expect the number of duplicate micropayment IDs from  each agency to be `0`.


```python
duplicate_micropayment_ids_sql = f"""
with 

all_ids as (
    select micropayment_id
    from `cal-itp-data-infra.payments.micropayments`
    where participant_id = %(agency)s
),

duplicate_ids as (
    select micropayment_id
    from all_ids 
    group by 1
    having count(*) > 1
)

select
    %(agency)s as `Agency`,
    (select count(distinct micropayment_id) from all_ids) as `Total_IDs`,
    (select count(*) from duplicate_ids) as `Duplicate_IDs`
"""

pd.read_sql_query(duplicate_micropayment_ids_sql, connection, params={'agency': 'mst'})
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Agency</th>
      <th>Total_IDs</th>
      <th>Duplicate_IDs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>mst</td>
      <td>13807</td>
      <td>8112</td>
    </tr>
  </tbody>
</table>
</div>




```python
pd.read_sql_query(duplicate_micropayment_ids_sql, connection, params={'agency': 'sbmtd'})
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Agency</th>
      <th>Total_IDs</th>
      <th>Duplicate_IDs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>sbmtd</td>
      <td>527</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>



For MST, many of the duplicates have different transaction times. 


```python
count_duplicate_micropayment_records_sql = f"""
with 

duplicate_ids as (
    select micropayment_id
    from `cal-itp-data-infra.payments.micropayments` 
    where participant_id = %(agency)s
    group by 1
    having count(*) > 1
)

select *
from `cal-itp-data-infra.payments.micropayments`
where micropayment_id in (select * from duplicate_ids)
order by micropayment_id
limit 10
"""

pd.read_sql_query(count_duplicate_micropayment_records_sql, connection, params={'agency': 'mst'})
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>micropayment_id</th>
      <th>aggregation_id</th>
      <th>participant_id</th>
      <th>customer_id</th>
      <th>funding_source_vault_id</th>
      <th>transaction_time</th>
      <th>payment_liability</th>
      <th>charge_amount</th>
      <th>nominal_amount</th>
      <th>currency_code</th>
      <th>type</th>
      <th>charge_type</th>
      <th>calitp_extracted_at</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>000317e7-220f-4c09-817c-b32a7b308812</td>
      <td>08a13a93-f9fc-4d8f-bfc0-e57b7e6fab01</td>
      <td>mst</td>
      <td>3aec6af1-208d-4316-a254-98571fc79724</td>
      <td>246574a7-8ac3-4a29-a1dc-7681fdc8dc78</td>
      <td>2021-07-17 22:36:18+00:00</td>
      <td>OPERATOR</td>
      <td>2.5</td>
      <td>2.5</td>
      <td>840</td>
      <td>DEBIT</td>
      <td>complete_variable_fare</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>1</th>
      <td>000317e7-220f-4c09-817c-b32a7b308812</td>
      <td>08a13a93-f9fc-4d8f-bfc0-e57b7e6fab01</td>
      <td>mst</td>
      <td>3aec6af1-208d-4316-a254-98571fc79724</td>
      <td>246574a7-8ac3-4a29-a1dc-7681fdc8dc78</td>
      <td>2021-07-17 22:29:19+00:00</td>
      <td>OPERATOR</td>
      <td>2.5</td>
      <td>2.5</td>
      <td>840</td>
      <td>DEBIT</td>
      <td>complete_variable_fare</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>2</th>
      <td>001393af-c1cf-4ff4-8edc-8ac34bb81cb9</td>
      <td>e342a964-aed2-4131-80f1-3476cee01c59</td>
      <td>mst</td>
      <td>13d2f3e7-ca12-43f0-81c9-265efad6f84c</td>
      <td>0eefbba9-5498-440e-b5f8-18fc86fbfe54</td>
      <td>2021-07-31 03:23:24+00:00</td>
      <td>OPERATOR</td>
      <td>2.5</td>
      <td>2.5</td>
      <td>840</td>
      <td>DEBIT</td>
      <td>complete_variable_fare</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>3</th>
      <td>001393af-c1cf-4ff4-8edc-8ac34bb81cb9</td>
      <td>e342a964-aed2-4131-80f1-3476cee01c59</td>
      <td>mst</td>
      <td>13d2f3e7-ca12-43f0-81c9-265efad6f84c</td>
      <td>0eefbba9-5498-440e-b5f8-18fc86fbfe54</td>
      <td>2021-07-31 03:47:16+00:00</td>
      <td>OPERATOR</td>
      <td>2.5</td>
      <td>2.5</td>
      <td>840</td>
      <td>DEBIT</td>
      <td>complete_variable_fare</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0014011f-f7b4-4bfa-b3b5-157b61b63611</td>
      <td>ec04f603-30ac-473f-b355-36c25dcc9085</td>
      <td>mst</td>
      <td>3137815e-3376-4a77-98a4-56e8381a1cd2</td>
      <td>a2be3cb3-23ac-4b12-b9f0-2bcf03226ecc</td>
      <td>2021-08-09 13:48:33+00:00</td>
      <td>OPERATOR</td>
      <td>1.5</td>
      <td>1.5</td>
      <td>840</td>
      <td>DEBIT</td>
      <td>complete_variable_fare</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>5</th>
      <td>0014011f-f7b4-4bfa-b3b5-157b61b63611</td>
      <td>ec04f603-30ac-473f-b355-36c25dcc9085</td>
      <td>mst</td>
      <td>3137815e-3376-4a77-98a4-56e8381a1cd2</td>
      <td>a2be3cb3-23ac-4b12-b9f0-2bcf03226ecc</td>
      <td>2021-08-09 13:58:46+00:00</td>
      <td>OPERATOR</td>
      <td>1.5</td>
      <td>1.5</td>
      <td>840</td>
      <td>DEBIT</td>
      <td>complete_variable_fare</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>6</th>
      <td>00257883-a117-47d6-ab85-482c11aab3e0</td>
      <td>bae840f5-9ba8-4492-9941-0e615a2783ce</td>
      <td>mst</td>
      <td>ec9f103c-fe7e-46d9-b242-0d04002e280e</td>
      <td>e9c39bc7-9786-40e4-8c55-ddcdf4c72ab7</td>
      <td>2021-08-16 19:34:28+00:00</td>
      <td>OPERATOR</td>
      <td>3.5</td>
      <td>3.5</td>
      <td>840</td>
      <td>DEBIT</td>
      <td>complete_variable_fare</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>7</th>
      <td>00257883-a117-47d6-ab85-482c11aab3e0</td>
      <td>bae840f5-9ba8-4492-9941-0e615a2783ce</td>
      <td>mst</td>
      <td>ec9f103c-fe7e-46d9-b242-0d04002e280e</td>
      <td>e9c39bc7-9786-40e4-8c55-ddcdf4c72ab7</td>
      <td>2021-08-16 18:28:29+00:00</td>
      <td>OPERATOR</td>
      <td>3.5</td>
      <td>3.5</td>
      <td>840</td>
      <td>DEBIT</td>
      <td>complete_variable_fare</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>8</th>
      <td>0026c9ef-22ba-412e-9fec-412db4cb5e4b</td>
      <td>a660f8aa-4390-4692-9af0-72af4256a371</td>
      <td>mst</td>
      <td>d0fdfde6-3106-40eb-9677-2a22e7000c90</td>
      <td>a803a01c-270e-4289-bedd-e43aeb6c0263</td>
      <td>2021-08-30 15:01:17+00:00</td>
      <td>OPERATOR</td>
      <td>2.5</td>
      <td>2.5</td>
      <td>840</td>
      <td>DEBIT</td>
      <td>complete_variable_fare</td>
      <td>2021-09-27</td>
    </tr>
    <tr>
      <th>9</th>
      <td>0026c9ef-22ba-412e-9fec-412db4cb5e4b</td>
      <td>a660f8aa-4390-4692-9af0-72af4256a371</td>
      <td>mst</td>
      <td>d0fdfde6-3106-40eb-9677-2a22e7000c90</td>
      <td>a803a01c-270e-4289-bedd-e43aeb6c0263</td>
      <td>2021-08-30 15:40:30+00:00</td>
      <td>OPERATOR</td>
      <td>2.5</td>
      <td>2.5</td>
      <td>840</td>
      <td>DEBIT</td>
      <td>complete_variable_fare</td>
      <td>2021-09-27</td>
    </tr>
  </tbody>
</table>
</div>



## Inconsistent `customer_id` values

When we join `micropayments` to `device_transactions` through `micropayment_device_transactions` we end up with a lot of mismatching `customer_id` values. This is even after we control for duplicate records:


```python
unique_id_counts_sql = f"""
with

transaction_customer_counts as (
    select littlepay_transaction_id, count(distinct customer_id) as distinct_customer_cnt
    from payments.device_transactions
    where participant_id = %(agency)s
    group by 1
),

micropayment_customer_counts as (
    select micropayment_id, count(distinct customer_id) as distinct_customer_cnt
    from payments.micropayments
    where participant_id = %(agency)s
    group by 1
)

select
    %(agency)s as `Agency`,
    (select count(*) from transaction_customer_counts where distinct_customer_cnt > 1) as `Transaction_IDs_with_different_Customer_IDs`,
    (select count(*) from micropayment_customer_counts where distinct_customer_cnt > 1) as `Micropayment_IDs_with_different_Customer_IDs`
"""

pd.read_sql_query(unique_id_counts_sql, connection, params={'agency': 'mst'})
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Agency</th>
      <th>Transaction_IDs_with_different_Customer_IDs</th>
      <th>Micropayment_IDs_with_different_Customer_IDs</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>mst</td>
      <td>0</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>



Given that the same `littlepay_transaction_id` always has the same `customer_id` (even for duplicate `littlepay_transaction_id` records), and the same `micropayment_id` always has the same `customer_id` (even for duplicate `micropayment_id` records), we make the assumptions below that:
* If we select distinct `littlepay_transaction_id`/`customer_id` pairs, and
* We select distinct `micropayment_id`/`customer_id` pairs, and
* We join these through the distinct `micropayment_device_transactions` records, then
* We should have matching `customer_id` values.

However, this isn't the case (at least for MST):


```python
inconsistent_customer_ctes = """
with

micropayments as (
    select distinct micropayment_id, customer_id as micropayment_customer_id
    from payments.micropayments
    where participant_id = %(agency)s
),

transactions as (
    select distinct littlepay_transaction_id, customer_id as transaction_customer_id
    from payments.device_transactions
    where participant_id = %(agency)s
),

through as (
    select distinct littlepay_transaction_id, micropayment_id
    from payments.micropayment_device_transactions
),

combined as (
    select *
    from micropayments
    join through using (micropayment_id)
    join transactions using (littlepay_transaction_id)
)

"""

pd.read_sql_query(inconsistent_customer_ctes + """
    select
        %(agency)s as `Agency`,
        (select count(*) from combined where micropayment_customer_id = transaction_customer_id) as `Matching_Customer_ID_count`,
        (select count(*) from combined where micropayment_customer_id != transaction_customer_id) as `Mismatch_Customer_ID_count`
""", connection, params={'agency': 'mst'})
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Agency</th>
      <th>Matching_Customer_ID_count</th>
      <th>Mismatch_Customer_ID_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>mst</td>
      <td>21706</td>
      <td>144</td>
    </tr>
  </tbody>
</table>
</div>




```python
pd.read_sql_query(inconsistent_customer_ctes + """
    select
        %(agency)s as `Agency`,
        (select count(*) from combined where micropayment_customer_id = transaction_customer_id) as `Matching_Customer_ID_count`,
        (select count(*) from combined where micropayment_customer_id != transaction_customer_id) as `Mismatch_Customer_ID_count`
""", connection, params={'agency': 'sbmtd'})
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Agency</th>
      <th>Matching_Customer_ID_count</th>
      <th>Mismatch_Customer_ID_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>sbmtd</td>
      <td>527</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>




```python
pd.read_sql_query(inconsistent_customer_ctes + """
    select *
    from combined
    where micropayment_customer_id != transaction_customer_id
    order by micropayment_id
""", connection, params={'agency': 'mst'})
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>littlepay_transaction_id</th>
      <th>micropayment_id</th>
      <th>micropayment_customer_id</th>
      <th>transaction_customer_id</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>4fcf8757-c0d5-4df2-a0b2-b07e0bdb0037</td>
      <td>004ca44f-4b35-446f-8d85-515bc938b167</td>
      <td>4c8200ca-5e45-47b8-a13d-6b9b48607c29</td>
      <td>2b800774-633e-4e87-9ac9-50de498abce3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>edb76395-6b0b-4e7a-b35b-8f1ed59fe66e</td>
      <td>05089d34-1e57-498e-b64a-e0f9a5f108c0</td>
      <td>e074383d-779e-4378-ac61-7d9ef6422d91</td>
      <td>59bcaa6c-2329-499f-91d7-67fc5c037712</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4918c915-f30d-48a7-a52a-d022526c85be</td>
      <td>051de55a-c69f-4314-975a-0143054183d5</td>
      <td>d2978f9b-b7d0-4402-be25-55bf6c6bce13</td>
      <td>87919b31-80db-467f-a690-e703253e366d</td>
    </tr>
    <tr>
      <th>3</th>
      <td>91a4e433-47c8-4a63-b34c-bb82ae597b32</td>
      <td>05cff6bd-d616-429b-848f-11d9808e05f2</td>
      <td>6b26b584-0133-40ea-9913-3386ca2dc608</td>
      <td>b98a8401-a00f-4c0f-9ef5-4b884e46ba13</td>
    </tr>
    <tr>
      <th>4</th>
      <td>186cad2c-78e7-41e8-8691-257779a2de74</td>
      <td>066fbc09-c2a5-4c48-be0d-6f5d2b172b2d</td>
      <td>555e0c45-af79-4f40-83e8-09a958d167a0</td>
      <td>d2a3c751-1beb-4995-a01a-28609c4c6f73</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>139</th>
      <td>ea465579-61d6-48d1-a9fb-8d8504054d14</td>
      <td>f7b4bf0b-05c6-4b85-a854-e4c699773363</td>
      <td>f63d30ca-56d9-49ae-a72f-8478ff0250d4</td>
      <td>af70f71d-dfdf-4fe6-b7f7-31d125d9e2f0</td>
    </tr>
    <tr>
      <th>140</th>
      <td>ec4b7f25-da2b-4b49-9971-4db4fb0c3bba</td>
      <td>f953bb7e-05a3-4a4a-bc0e-6629fc0c2416</td>
      <td>3f1bbc14-3786-4485-90b2-6e2ea685c79f</td>
      <td>7f1ae877-00b7-460f-979c-b308468a75d7</td>
    </tr>
    <tr>
      <th>141</th>
      <td>1ca2935c-5d10-4a7e-b9f2-1ccb0f5f389b</td>
      <td>f9cff78b-7478-4874-a387-17a2cc22cc32</td>
      <td>54941ea1-46b3-4124-868a-83f37a2c6fa5</td>
      <td>d7ac4c3e-84b0-469f-a491-0dfa2122f715</td>
    </tr>
    <tr>
      <th>142</th>
      <td>f370c84d-ddd3-4e7b-ab81-f145339ef9e7</td>
      <td>fcd75fd3-95cb-4c58-8ac9-97a10c040c0b</td>
      <td>a5f9e75e-ba8e-4e20-82fa-172fbf4eebaf</td>
      <td>937a5a5c-c021-4313-a10b-689b59fa87ba</td>
    </tr>
    <tr>
      <th>143</th>
      <td>19c1e53e-546b-4fac-8fb3-d82d0143dea7</td>
      <td>ffdcf553-df30-46da-bdeb-7607592adbc6</td>
      <td>cb306804-071f-46a3-b4d3-d189b278af93</td>
      <td>30774de2-fe1d-4c9b-b1b5-f8d8c54aef43</td>
    </tr>
  </tbody>
</table>
<p>144 rows × 4 columns</p>
</div>



## Types of trips

At least for MST, we expect there to be trips that aren't just coded as `single`, since MST [charges by the mile](https://mst.org/fares/route-types-fares/#:~:text=Contactless%20Fares:%20How%20Calculated%20and%20Charged) when using contactless payment.


```python
pd.read_sql_query("""
    select distinct type as `Distinct_Trip_Types_for_MST`
    from payments.device_transactions
    where participant_id = 'mst'
""", connection)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Distinct_Trip_Types_for_MST</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>single</td>
    </tr>
  </tbody>
</table>
</div>


