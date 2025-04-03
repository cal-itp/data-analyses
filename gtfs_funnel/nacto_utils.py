"""
For the most common shape for each route-direction,
apply some definitions for NACTO route typologies
and service frequency to roads.

Do our best at assigning scores to road segments
across multiple operators. We'll take the aggregate
stop arrivals and calculate an overall frequency
for that segment.

https://nacto.org/publication/transit-street-design-guide/introduction/service-context/transit-route-types/

https://nacto.org/publication/transit-street-design-guide/introduction/service-context/transit-frequency-volume/.
"""
import pandas as pd

from shared_utils.gtfs_utils_v2 import RAIL_ROUTE_TYPES

route_typology_types = [
    "downtown_local", "local", "coverage",
    "rapid", "express", "rail", "ferry"
]

def tag_rapid_express_rail_ferry(
    route_name: str, route_type: str
) -> pd.Series:
    """
    Use the combined route_name and see if we can 
    tag out words that indicate the route is
    express, rapid, local, and rail.
    
    Treat rail as own category.
    For local routes, we'll pass that through NACTO to see
    if we can better categorize as downtown_local, local, or coverage.
    """
    route_name_lower = route_name.lower()
    
    express = 0
    rapid = 0
    rail = 0
    ferry = 0
    
    if any(substring in route_name_lower for substring in 
           ["express", "limited"]):
        express = 1
    if "rapid" in route_name_lower:
        rapid = 1
    
    if route_type in RAIL_ROUTE_TYPES:
        rail = 1
    
    if route_type == "4":
        ferry = 1
    
    return pd.Series(
            [express, rapid, rail, ferry], 
            index=['is_express', 'is_rapid', 'is_rail', 'is_ferry']
        )

def nacto_peak_frequency_category(freq_value: float) -> str:
    """
    Assign peak frequencies into categories.
    Be more generous, if there are overlapping
    cutoffs for categories, we'll use the lower value
    so transit route / road can achieve a better score.
    
    Source: https://nacto.org/publication/transit-street-design-guide/introduction/service-context/transit-frequency-volume/
    """
    # Set the upper bounds here 
    low_cutoff = 4
    mod_cutoff = 10
    high_cutoff = 20
    
    if freq_value < low_cutoff:
        return "low"
    elif freq_value >= low_cutoff and freq_value < mod_cutoff:
        return "moderate"
    elif freq_value >= mod_cutoff and freq_value < high_cutoff:
        return "high"
    elif freq_value >= high_cutoff:
        return "very_high"

    
def nacto_stop_frequency(
    stop_freq: float, 
    service_freq: str
) -> str:
    """
    Assign NACTO route typologies.
    Be more generous, if there are overlapping
    cutoffs for categories, we'll use the lower value
    so transit route / road can achieve a better score.
    
    https://nacto.org/publication/transit-street-design-guide/introduction/service-context/transit-route-types/
    """
    cut1 = 3
    cut2 = 4
    mod_high = ["moderate", "high"]
    
    if stop_freq >= cut2:
        return "downtown_local"
    
    elif (stop_freq >= cut1 and 
          stop_freq < cut2 and 
          service_freq in mod_high
         ):
        return "local"
    
    elif (stop_freq >= 1 and stop_freq < cut1 and 
          service_freq in mod_high):
        return "rapid"
    
    elif service_freq == "low":
        #(stop_freq >= 2 and stop_freq < 8
        return "coverage"
    
    # last category is "express", which we'll have to tag on 
    # the route name side
    
def reconcile_route_and_nacto_typologies(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Let's see if we can sort out local routes into more
    specific NACTO local route types.
    If it's ever flagged as downtown_local or coverage,
    we'll use that.
    """
    df = df.assign(
        is_rapid = df[["is_rapid", "is_nacto_rapid"]].max(axis=1),                              
    ).rename(columns = {
        "is_nacto_downtown_local": "is_downtown_local",
        "is_nacto_coverage": "is_coverage"
    })
    
    # Retain as local if coverage or downtown_local aren't true
    df = df.assign(
        is_local = df.apply(
            lambda x: 1 if ((x.is_coverage==0) and (x.is_downtown_local == 0))
            or (x.is_nacto_local==1)
            else 0, axis=1)
    )
    
    drop_cols = [c for c in df.columns if "is_nacto_" in c]
    
    df2 = df.drop(columns = drop_cols)
    
    integrify = [f"is_{c}" for c in route_typology_types]
    df2[integrify] = df2[integrify].astype(int)
    
    return df2