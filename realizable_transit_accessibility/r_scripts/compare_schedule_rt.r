options(java.parameters = "-Xmx5G")
library(r5r)
library(sf)
library(dplyr)

# Get geometries in analysis area
tracts_albers <- st_transform(st_read("tl_2025_06_tract/tl_2025_06_tract.shp"), "EPSG:3310")
tracts_centroids <- st_transform(st_centroid(tracts_albers), "EPSG:4326")
analysis_area <- st_transform(st_read("west_la.geojson"), "EPSG:4326")
centroids_in_analysis_area <- st_intersection(tracts_centroids, analysis_area) |> rename(id = GEOID)

# Build schedule network
west_la_schedule_network <- build_network(
  data_path = "downloaded_schedule_feeds"
)
# Build RT network
west_la_rt_network <- build_network(
  data_path = "generated_rt_feeds_2025-11-05"
)

# Get schedule matrix
origins <- data.frame(id = c("downtown"), lat = c(34.015112), lon = c(-118.493648))
schedule_matrix <- travel_time_matrix(
  r5r_network = west_la_schedule_network,
  origins = origins,
  destinations = centroids_in_analysis_area,
  departure_datetime = as.POSIXct("2025-11-05 08:00:00", tz = "America/Los_Angeles"),
  max_trip_duration = 90,
  mode = "TRANSIT",
) |>
  select(to_id, travel_time_p50) |>
  rename(schedule_travel_time_p50 = travel_time_p50, GEOID = to_id)
# Get RT matrix
# Get schedule matrix
rt_matrix <- travel_time_matrix(
  r5r_network = west_la_rt_network,
  origins = origins,
  destinations = centroids_in_analysis_area,
  departure_datetime = as.POSIXct("2025-11-05 08:00:00", tz = "America/Los_Angeles"),
  max_trip_duration = 90,
  mode = "TRANSIT",
) |>
  select(to_id, travel_time_p50) |>
  rename(rt_travel_time_p50 = travel_time_p50, GEOID = to_id)

tracts_with_travel_times <- right_join(
  right_join(
    tracts_albers, schedule_matrix,
    by = "GEOID"
  ),
  rt_matrix,
  by = "GEOID"
) |> select(GEOID, rt_travel_time_p50, schedule_travel_time_p50, geometry)
tracts_with_travel_times$schedule_rt_difference <- tracts_with_travel_times$schedule_travel_time_p50 - tracts_with_travel_times$rt_travel_time_p50
st_write(st_transform(tracts_with_travel_times, "EPSG:4326"), "output.geojson", delete_dsn = TRUE)
plot(tracts_with_travel_times)
