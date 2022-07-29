import pandas as pd
import numpy as np
from calitp import to_snakecase

from siuba import *


def add_airtable_filter(orgs):
    #narrow down the columns
    orgs = (orgs>>select(_.name,
                         _.organization_type,
                         _.ntp_id,
                         _.itp_id,
                         _.opm_id_drmt,
                         _.brand, 
                         _.alias,
                         _.reporting_category,
                         _.fares_v2_status,
                         _.mobility_services_managed,
                         _.mobility_services_operated,
                         _.gtfs_datasets_produced,
                         _.service_type__from_mobility_services_managed_,
                         _.service_type__from_mobility_services_operated_,
                         _.currently_operating__from_mobility_services_managed_,
                         _.headquarters_place,
                         _.caltrans_district,
                         _.gtfs_dataset__from_mobility_services_managed_,
                         _.gtfs_schedule_status,
                         _['#_of_fixed_route_services'],
                         _['#_fixed_route_or_deviated_fixed_route_services'],
                         _.gtfs_static_status,
                         _.gtfs_realtime_status)
        )
    
    # filter for reporting category
    orgs_2 = ((orgs.query("reporting_category == ('Core') or reporting_category ==('Other Public Transit')"))
              )
    
    # filter for service type
    fixed = orgs_2[
        (orgs_2["service_type__from_mobility_services_managed_"].str.contains("fixed-route")
        ) | (orgs_2["service_type__from_mobility_services_managed_"].str.contains("deviated fixed-route")
            ) | (orgs_2["service_type__from_mobility_services_managed_"].str.contains("on-demand fixed-route"))
    ]
    
    # filter for currently operating
    fixed = fixed[~fixed["currently_operating__from_mobility_services_managed_"].str.contains("0 checked out of")]
    
    return fixed



def make_long(df, keep_cols = [], category_name = "managed"):
    df1 = df[["name", "gtfs_schedule_status"] + keep_cols]
    
    for col_name in ["mobility_services", "service_type"]:
        df1 = df1.rename(columns=lambda c: col_name 
                        if c.startswith(col_name) else c)
    
    df1 = df1[["name", "gtfs_schedule_status", "mobility_services", "service_type"]]
    
    
    df2 = df1.assign(
        mobility_services = (df1.mobility_services.fillna("None")
                         .apply(lambda x: x.split(','))
                        )
    )
    
    # Cannot add service_type to explode
    # Service types do not match mobility_services (1 service can provide both flex and fixed route)
    # That's fine, maybe service type is not too important to keep anyway
    df3 = df2.explode("mobility_services")
    
    # New variable to track the category name. 
    # Within mobility services, there are 2 sub-categories, managed / operated
    df3 = df3.assign(
        category = category_name
    )
    
    return df3
    
    