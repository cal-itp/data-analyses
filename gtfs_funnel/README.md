## Download Single Day of GTFS Schedule & Real-Time Data

Use `update_vars` and input one or several days to download. 

1. **Schedule data**: download data for [trips](./download_trips.py), [stops](./download_stops.py), [shapes](./download_shapes.py), and [stop times](./download_stop_times.py) and cache parquets in GCS
1. **Vehicle positions data**: download [RT vehicle positions](./download_vehicle_positions.py)
1. Use the `Makefile` and download schedule and RT data. In terminal: `make download_gtfs_data_one_day`