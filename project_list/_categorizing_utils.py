import pandas as pd
def add_categories(df):
    """
    Create general categories for each projects.
    https://github.com/cal-itp/data-analyses/blob/29ed3ad1d107c6be09fecbc1a5f3d8ef5f2b2da6/dla/dla_utils/clean_data.py#L305
    """
    # There are many projects that are
    ACTIVE_TRANSPORTATION = [
        "bike",
        "bicycle",
        "cyclist",
        "pedestrian",
        ## including the spelling errors of `pedestrian`
        "pedestrain",
        "crosswalk",
        "bulb out",
        "bulb-out",
        "active transp",
        "traffic reduction",
        "speed reduction",
        "ped",
        "srts",
        "safe routes to school",
        "sidewalk",
        "side walk",
        "Cl ",
        "trail",
        "atp",
    ]
    TRANSIT = [
        "bus",
        "metro",
        "station",  # Station comes up a few times as a charging station and also as a train station
        "transit",
        "fare",
        "brt",
        "yarts",
        "railroad",
        "highway-rail",
        "streetcar",
        "mass transit",
        # , 'station' in description and 'charging station' not in description
    ]
    BRIDGE = ["bridge", "viaduct"]
    STREET = [
        "traffic signal",
        "resurface",
        "resurfacing",
        "slurry",
        "seal" "sign",
        "stripe",
        "striping",
        "median",
        "guard rail",
        "guardrail",
        "road",
        "street",
        "sinkhole",
        "intersection",
        "signal",
        "curb",
        "light",
        "tree",
        "pavement",
        "roundabout",
    ]

    NOT_INC = []

    FREEWAY = [
        "freeway",
        "highway",
        "hwy",
        "congestion pricing",
        "variable tolls",
        "express lane",
        "value pricing",
        "rush hour",
        "cordon",
        "dynamic pricing",
        "dynamically priced",
        "high occupancy",
        "mobility pricing",
        "occupancy",
        "toll lane",
        "performance pricing",
        "peak travel",
        "managed lane",
        "tollway",
        "express toll",
        "fixed pricing",
        "hot lane",
        "hov lane",
        "expressed toll lane",
    ]

    INFRA_RESILIENCY_ER = [
        "repair",
        "emergency",
        "replace",
        "retrofit",
        "rehab",
        "improvements",
        "seismic",
        "reconstruct",
        "restoration",
    ]

    CONGESTION_RELIEF = [
        "congestion",
        "rideshare",
        "ridesharing",
        "vanpool",
        "car share",
    ]

    PASSENGER_MODE = ["non sov", "high quality transit areas", "hqta", "hov"]

    SAFETY = [
        "fatalities",
        "safe",
        "speed management",
        "signal coordination",
        "slow speeds",
        "roundabouts",
        "victims",
        "collisions",
        "collisoins",
        "protect",
        "crash",
        "modification factors",
        "safety system",
    ]

    def categorize_project_descriptions(row):
        """
        This function takes a individual type of work description (row of a dataframe)
        and returns a dummy flag of 1 if it finds keyword present in
        project categories (active transportation, transit, bridge, etc).
        A description can contain multiple keywords across categories.
        """
        # Clean up project description 2
        project_description = (
            row.project_description.lower()
            .replace("-", "")
            .replace(".", "")
            .replace(":", "")
        )

        # Store a bunch of columns that will be flagged
        # A project can involve multiple things...also, not sure what's in the descriptions
        active_transp = ""
        transit = ""
        bridge = ""
        street = ""
        freeway = ""
        infra_resiliency_er = ""
        congestion_relief = ""
        passenger_mode_shift = ""
        safety = ""

        if any(word in project_description for word in ACTIVE_TRANSPORTATION):
            active_transp = 1
        # if any(word in description if instanceof(word, str) else word(description) for word in TRANSIT)
        if any(word in project_description for word in TRANSIT) and not any(
            exclude_word in project_description for exclude_word in NOT_INC
        ):
            transit = 1
        if any(word in project_description for word in BRIDGE):
            bridge = 1
        if any(word in project_description for word in STREET):
            street = 1
        if any(word in project_description for word in FREEWAY):
            freeway = 1
        if any(word in project_description for word in INFRA_RESILIENCY_ER):
            infra_resiliency_er = 1
        if any(word in project_description for word in CONGESTION_RELIEF):
            congestion_relief = 1
        if any(word in project_description for word in PASSENGER_MODE):
            passenger_mode_shift = 1
        if any(word in project_description for word in SAFETY):
            safety = 1
        new_cols = [
            "active_transp",
            "transit",
            "bridge",
            "street",
            "freeway",
            "infra_resiliency_er",
            "congestion_relief",
            "passenger_mode_shift",
            "safety",
        ]

        category_series = pd.Series(
            [
                active_transp,
                transit,
                bridge,
                street,
                freeway,
                infra_resiliency_er,
                congestion_relief,
                passenger_mode_shift,
                safety,
            ],
            index=new_cols,
        )

        return category_series

    work_categories_df = df.apply(categorize_project_descriptions, axis=1)
    new_cols = list(work_categories_df.columns)
    df2 = pd.concat([df, work_categories_df], axis=1)
    df2[new_cols] = df2[new_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    df2["n_categories"] = df2[new_cols].sum(axis=1)
    return df2

def find_categories_by_mpo(df):
    """
    Count the number of projects that
    fit in a category by MPO
    """
    df = df[
        [
            "project_title",
            "data_source",
            "active_transp",
            "transit",
            "bridge",
            "street",
            "freeway",
            "infra_resiliency_er",
            "congestion_relief",
            "passenger_mode_shift",
            "safety",
        ]
    ]

    df_melt = pd.melt(
        df,
        id_vars=["data_source"],
        value_vars=[
            "active_transp",
            "transit",
            "bridge",
            "street",
            "freeway",
            "infra_resiliency_er",
            "congestion_relief",
            "passenger_mode_shift",
            "safety",
        ],
    ).rename(columns={"variable": "category", "value": "total_projects"})

    df_melt = (
        df_melt.groupby(["data_source", "category"]).agg({"total_projects": "sum"}).reset_index()
    )

    df_melt.category = df_melt.category.str.replace("_", " ").str.title()

    return df_melt

def count_all_categories(df):
    """
    Tally up all the projects
    that fit into a category across the df
    """
    new_cols = [
    "active_transp",
    "transit",
    "bridge",
    "street",
    "freeway",
    "infra_resiliency_er",
    "congestion_relief",
    "passenger_mode_shift",
    "safety"]
    
    df = df[new_cols].sum(axis=0)
    
    df = (
    df.to_frame()
    .reset_index()
    .rename(columns={"index": "Category", 0: "Total Projects"})
    )
    
    df.Category = df.Category.str.replace(
    "_", " ").str.title()
    
    return df