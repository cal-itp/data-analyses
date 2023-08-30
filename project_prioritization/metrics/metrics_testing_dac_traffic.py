# DAC Traffic Impacts CSIS Metric
# Pseudo Code

# For projects in list:
  # Sum AADT values across segments (i.e. peak/off-peak, HOV/non-HOV)
  # Repeat for truck AADT
  # Apply a weighting factor of 6 to the truck AADT
  # Sum car and weighted truck AADT to get AADT value
  # Repeat for both build and no-build scenarios
  # Calculate the percent change in truck-weighted AADT

# Join the build and no-build AADT data to the project spatial geometry on the project name field

# Generate a 500 meter buffer for all project geometries

# read in EQI Traffoc Exposure Screen

# If the project geometry buffer DOES NOT touch an EQI Traffic Exposure Screened geography, the project score defaults to 0
# Else If the project geometry buffer DOES touch an EQI Traffic Exposure Screened geography, the change in truck-weighted AADT is considered and a relevant score is determined.
