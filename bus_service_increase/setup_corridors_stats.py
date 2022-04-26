import intake
import pandas as pd

from shared_utils import geography_utils

catalog = intake.open_catalog("./*.yml")

# First, aggregate once to get rid of edge cases where RouteType differs
# 110 in LA County is both Interstate and State Highway
# Make sure other highway characteristics are correctly grabbed (max or sum)
def extra_highway_aggregation(gdf):
    gdf2 = (gdf.groupby(["Route", "County", "District",
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
            }).reset_index()
    )

    # Now we took sum for pct_highway, values can be > 1, set it back to 1 max again.
    gdf2 = gdf2.assign(
        pct_highway = gdf2.apply(lambda x: 1 if x.pct_highway > 1 
                                 else x.pct_highway, axis=1),
    )

    return gdf2


def extra_operator_aggregation(gdf):
    # For the unique route_id, flag it as parallel if it is parallel to any hwy Route
    gdf2 = (gdf.groupby(["itp_id", "County", "route_id"])
            .agg({"parallel": "max"})
            .reset_index()
    )
    
    return gdf2


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


def aggregate(df, by="operator"):
    if by=="operator":
        group_cols = ["itp_id", "County"]
        # Put this operator_hwys first before it gets aggregated and overwritten
        operator_hwys = grab_highways_for_operator(df)
        df = extra_operator_aggregation(df)
        
    elif by=="highway":
        group_cols = ["Route", "County", "District", 
                      "NB", "SB", "EB", "WB"]
        df = extra_highway_aggregation(df)
    
    df2 = (geography_utils.aggregate_by_geography(
            df,
            group_cols = group_cols,
            sum_cols = ["parallel"],
            nunique_cols = ["route_id"]
        )
    )
    
    # Calculate % parallel
    df2 = (df2.assign(
            pct_parallel = df2.parallel.divide(df2.route_id).round(3)
        ).rename(columns = {
            "route_id": "unique_route_id",
            "parallel": "num_parallel",
        }).sort_values(group_cols).reset_index(drop=True)
           .astype({"unique_route_id": int})
    )
    
    # Last minute clean-up after aggregation
    if by == "operator":        
        df2 = pd.merge(df2,
                       operator_hwys,
                       on = "itp_id", 
                       how = "left",
                       validate = "m:1"
                      ).astype({"itp_id": int})
        
        
    if by=="highway":
        df2 = (df2.assign(
                NB_SB = df2.apply(lambda x: 1 if (x.NB == 1) or (x.SB == 1)
                                  else 0, axis=1).astype(int),
                EB_WB = df2.apply(lambda x: 1 if (x.EB == 1) or (x.WB == 1)
                                  else 0, axis=1).astype(int),
            ).drop(columns = ["NB", "SB", "EB", "WB"])
               .astype({"District": int, "Route": int})
        )
        
    return df2
    

def aggregated_transit_hwy_stats():    
    gdf = catalog.parallel_or_intersecting_routes.read()

    operator_stats = (aggregate(gdf, by="operator")
                      .sort_values("pct_parallel", ascending=False)
                      .reset_index(drop=True)
                     )
        
    hwy_stats = (aggregate(gdf, by="highway")
                 .sort_values("pct_parallel", ascending=False)
                 .reset_index(drop=True)
                ) 
    
    # Should add the competitive routes info here
    # summarize it for operator and attach it too
    
    return operator_stats, hwy_stats
    
    
def routes_highways_geom_for_operator(operator_df):
    gdf = catalog.parallel_or_intersecting_routes.read()
    highways = catalog.highways_cleaned.read()

    transit_routes = gdf[gdf.itp_id==operator_df.itp_id.iloc[0]]
    
    # Select the highways for the map when plotting transit map
    hwys_df = highways[(highways.Route.isin(operator_df.hwy_list.iloc[0])) & 
                  (highways.County.isin(operator_df.County))
                 ]
    
    return transit_routes, hwys_df