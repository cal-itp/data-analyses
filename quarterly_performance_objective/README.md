# Mass Transit Performance Objectives

## Performance Objective 01: Increase total amount of service on the SHN and reliability (speed) of that service

Transit routes along the SHN can be categorized into 3 groups:

1. **On SHN** - where at least 20% of the transit route runs the SHN (within 50 ft) 
1. **Intersects SHN** - where at least 20% of the transit route runs within 0.5 mile of the SHN.
1. **Other** - all other transit routes.


Initially presented for the Planning and Modal Advisory Committee (PMAC).

## Workflow
### Data Generation

1. [Clean and process data each month](https://github.com/cal-itp/data-analyses/blob/main/quarterly_performance_objective/clean_data.py)
1. [Compile monthly data into time-series](https://github.com/cal-itp/data-analyses/blob/main/quarterly_performance_objective/compile_time_series.py)

### Helper Functions
[Categorize routes](https://github.com/cal-itp/data-analyses/blob/main/rt_segment_speeds/segment_speed_utils/parallel_corridors.py) into on SHN, parallel / intersects SHN, or other.

### Reports

Create a report of current quarter's snapshot with a historical comparison.