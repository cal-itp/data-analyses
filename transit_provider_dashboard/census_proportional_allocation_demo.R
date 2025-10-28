# Load packages
library(tidycensus)
library(dplyr)
library(sf)

# Get Census population data with spatial data
population <- get_decennial(
  geography = "tract",
  variables = "P1_001N",
  year = 2020,
  state = 06,
  geometry = TRUE
) %>%
  # Transform to a projected coordinate system
  st_transform(3310) %>%
  # Calculate the area of each spatial unit
  mutate(area1 = st_area(.))

# Get the geometry for aggregation (i.e. transit stops, routes, etc)
geometry <- read_sf("/Users/S152973/Downloads/test.geojson") %>%
  # Create an id based on row (data may already have an id)
  mutate(id = as.character(row_number())) %>%
  # Transform to a projected coordinate system
  st_transform(3310) %>%
  # Buffer around the geometry (this one is 500 meters)
  st_buffer(500)

# Intersect the geometry with the Census data
geometry_intersect <- st_intersection(geometry, population) %>%
  # Calculate the new area of the intersected Census geometry
  mutate(area2 = st_area(.),
         # Calculated adjusted population for each intersected unit
         # THis is just total population times the proportion of total area intersected
         adjusted_pop = as.numeric(value * (area2 / area1))) %>%
  # Drop geometry
  st_drop_geometry() %>%
  # Group by the unique to id to aggregate
  group_by(id) %>%
  # Sum adjusted populations by id to get an adjusted Census count for each point
  summarise(adjusted_pop = sum(adjusted_pop, na.rm = T)) %>%
  ungroup()
