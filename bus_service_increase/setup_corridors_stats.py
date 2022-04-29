"""
Combine parallel and competitive datasets.
Aggregate into summary stats at the operator or highway Route level.

Competitive routes must be subset of parallel routes.
Parallel routes are combination of transit route with highway Route,
and a route may be parallel to 1 highway but not other highways it intersects with.
"""
import intake
import pandas as pd

from shared_utils import geography_utils

catalog = intake.open_catalog("./*.yml")


def competitive_to_route_level():
    # This is output from `make_stripplot_data.py`
    # Wrangle it so it is at route-level, instead of trip-level    
    df = catalog.competitive_route_variability.read()

    keep_cols = [
        "calitp_itp_id", "route_id", 
        "pct_trips_competitive", 
        #"caltrans_district", 
    ]

    df2 = (df[keep_cols].drop_duplicates()
           .rename(columns = {"calitp_itp_id": "itp_id"})
           .reset_index(drop=True)
          )
        
    return df2


def calculate_parallel_competitive_stats(df, group_cols):
    df2 = geography_utils.aggregate_by_geography(
        df,
        group_cols = group_cols,
        sum_cols = ["parallel", "competitive"],
        nunique_cols = ["route_id"]
    )
    
    # Calculate % parallel
    df2 = df2.assign(
        pct_parallel = df2.parallel.divide(df2.route_id).round(3),
        pct_competitive = df2.competitive.divide(df2.route_id).round(3),
    )
    
    df2 = (df2.rename(columns = {
                "route_id": "unique_route_id",
                "parallel": "num_parallel",
                "competitive": "num_competitive"})
           .sort_values(group_cols)
           .reset_index(drop=True)
           .astype({"unique_route_id": int})
          )

    return df2


def aggregate_highways(df):
    group_cols = ["Route", "County", "District", 
                      "NB", "SB", "EB", "WB"]
    
    # First, aggregate once to get rid of edge cases where RouteType differs
    # 110 in LA County is both Interstate and State Highway
    # Make sure other highway characteristics are correctly grabbed (max or sum)
    df2 = (df.groupby(["Route", "County", "District",
                     "route_id", "total_routes"])
        .agg({
            "NB": "max",
            "SB": "max", 
            "EB": "max",
            "WB": "max",
            "route_length": "sum",
            "pct_route": "sum",
            "pct_highway": "sum",
            "highway_length": "sum",
            "parallel": "max",
            "competitive": "max",
        }).reset_index()
    )

    # Now we took sum for pct_highway, values can be > 1, set it back to 1 max again.
    df2 = df2.assign(
        pct_highway = df2.apply(lambda x: 1 if x.pct_highway > 1 
                                 else x.pct_highway, axis=1),
    )
    
    df3 = calculate_parallel_competitive_stats(df2, group_cols)
    
    df4 = (df3.assign(
        NB_SB = df3.apply(lambda x: 1 if (x.NB == 1) or (x.SB == 1)
                          else 0, axis=1).astype(int),
        EB_WB = df3.apply(lambda x: 1 if (x.EB == 1) or (x.WB == 1)
                          else 0, axis=1).astype(int),
        ).drop(columns = ["NB", "SB", "EB", "WB"])
       .astype({"District": int, "Route": int})
    )
    
    return df4   
    
    
def aggregate_operators(df):
    group_cols = ["itp_id", "County"]
    
    # Put this operator_hwys first before it gets aggregated and overwritten
    operator_hwys = grab_highways_for_operator(df)
    
    # For the unique route_id, flag it as parallel if it is parallel to any hwy Route
    # also flag if it is competitive along any hwy Route
    df2 = (df.groupby(group_cols + ["route_id"])
            .agg({"parallel": "max", 
                  "competitive": "max"})
            .reset_index()
    )
    
    df3 = calculate_parallel_competitive_stats(df2, group_cols)

    df4 = pd.merge(df3,
               operator_hwys,
               on = "itp_id", 
               how = "left",
               validate = "m:1"
              ).astype({"itp_id": int})
    
    return df4
    


def grab_highways_for_operator(df):
    # Assemble a list of highways associated with the operator
    # create a column that lists those highways
    # merge that in
    operator_hwys = (df[["itp_id", "Route"]][df.Route.notna()]
         .drop_duplicates()
         .astype(int)
        )

    operator_hwys = (operator_hwys.groupby("itp_id")["Route"]
                     .apply(lambda x: x.tolist())
                     .reset_index()
                     .rename(columns = {"Route": "hwy_list"})
                    )
    return operator_hwys

    
def aggregated_transit_hwy_stats():    
    parallel = catalog.parallel_or_intersecting_routes.read()
    competitive = competitive_to_route_level()
    
    gdf = pd.merge(
        parallel,
        competitive,
        on = ["itp_id", "route_id"],
        how = "left",
        # m:1 because parallel is at itp_id, route_id, shape_id level
        # competitive is at itp_id, route_id level
        validate = "m:1"
    )
    
    # Since competitive routes should be a subset of parallel routes,
    # But competitive routes are determined at route-level, 
    # whereas parallel is determined at route-highway intersection level,
    # set competitive to be 1 only if it is occurring on parallel segment.
    gdf = gdf.assign(
        competitive = gdf.apply(lambda x: 
                               1 if ((x.pct_trips_competitive >= 0.75) and (x.parallel == 1))
                               else 0, axis=1)
    )
    
    operator_stats = (aggregate_operators(gdf)
                      .sort_values(["pct_parallel", "pct_competitive"], 
                                   ascending=[False, False])
                      .reset_index(drop=True)
                     )
        
    hwy_stats = (aggregate_highways(gdf)
                 .sort_values("pct_parallel", ascending=False)
                 .reset_index(drop=True)
                ) 
        
    return operator_stats, hwy_stats
    
    
def routes_highways_geom_for_operator(operator_df):
    # This seems slightly overlapping with C1, where gdf is
    # imported at the beginning of ipywidget
    # but, not sure how to combine and make more efficient
    gdf = catalog.parallel_or_intersecting_routes.read()
    highways = catalog.highways_cleaned.read()

    transit_routes = gdf[gdf.itp_id==operator_df.itp_id.iloc[0]]
    
    # Select the highways for the map when plotting transit map
    hwys_df = highways[(highways.Route.isin(operator_df.hwy_list.iloc[0])) & 
                  (highways.County.isin(operator_df.County))
                 ]
    
    return transit_routes, hwys_df
