import pandas as pd

la_operators = [
    "LAX FlyAway Schedule",
    "LAX Flyaway Bus Schedule", 
    "LAX Shuttles Schedule",
    "LA Metro Bus Schedule",
    "LA Metro Rail Schedule",
    "Big Blue Bus Schedule",
    "Big Blue Bus Swiftly Schedule",
    "Culver City Schedule",
    "Torrance Schedule",
    "G Trans Schedule",
    "Beach Cities Schedule",
    "Beach Cities GMV Schedule"
]

feed_to_name_dict = {
    '8d9623a1823a27925b7e2f00e44fc5bb': 'LA Metro Bus Schedule',
    'dbee6840cff90155409e6383afd1f16d': 'Beach Cities GMV Schedule',
    '5aa5331522639832a9cfdb692b6945b2': 'LA Metro Rail Schedule',
    '1277a724cc287d17b32b8fc6652eadaf': 'LAX Flyaway Bus Schedule',
    'f6774d861953d4f4cdcffec95e2652c7': 'Culver City Schedule',
    '6a5a841d0f829e6f8aba4e1f619e7a9e': 'LAX Shuttles Schedule',
    '895ab5a382406a279733af6164becd73': 'Torrance Schedule',
    '7a3f513c343b16a30c135ed7d332b6d6': 'Big Blue Bus Schedule',
    '0f0cd1b91cfdb7c398ac6f028fbfd888': 'LAX FlyAway Schedule',
    '21c71baff5435395124b47f4f36261a7': 'G Trans Schedule',
    '8dc47f5bd4666e8c52178b5e08085eeb': 'Big Blue Bus Swiftly Schedule',
    'd95f2f26bbf4846e4eb84d352fb0990d': 'Beach Cities Schedule'
}

subset_feeds = list(feed_to_name_dict.keys())

metro_routes = {
    "803": "Metro C Line",
    "807": "Metro K Line",
    "102-13191": "Metro 102",
    "111-13191": "Metro 111",
    "117-13191": "Metro 117",
    "120-13191": "Metro 120",
    "232-13191": "Metro 232",
}

bbb_routes = {
    "3": "BBB 3 Lincoln Boulevard/LAX",
    "R3": "BBB R3 Lincoln Boulevard/LAX",
    # swiftly codes route_id differently
    "3929": "BBB 3 Lincoln Blvd/LAX Rapid (Swiftly)", 
    "3930": "BBB 3 Lincoln Blvd/LAX (Swiftly)", 
}

culver_routes = {
    "6": "Culver 6 Sepulveda",
    "R6": "Culver R6 Sepulveda",
}

gtrans_routes = {
    "5": "G Trans 5",
}

torrance_routes = {
    "8": "Torrance 8",
}

beach_cities_routes = {
    "BCT109 NB": "Beach Cities Transit 109 NB",
    "BCT109 SB": "Beach Cities Transit 109 SB",
    # GMV schedule codes route_id as the same for both directions
    "4815": "Beach Cities Transit 109 (GMV)", 
}

lawa_routes = {
    # this might be the only one that connects to the station?
    "TL-6": "LAX Shuttles Metro Connector GL",
    # add all other routes anyway
    "5": "LAX FlyAway Schedule LAX to Van Nuys",
    "6": "LAX FlyAway Schedule LAX to Union Station",
    "TL-6": "LAX Flyaway Bus Schedule LAX to Union Station",
}

operators_lax_routes = {
    # https://www.flylax.com/sites/lax/files/documents/Ground%20Transportation%20Waiting%20Areas.pdf
    "LAX FlyAway Schedule": lawa_routes, 
    "LAX Flyaway Bus Schedule": lawa_routes, 
    "LAX Shuttles Schedule": lawa_routes,
    # https://www.metro.net/lax-metro-transit-center/
    "LA Metro Bus Schedule": metro_routes,
    "LA Metro Rail Schedule": metro_routes,
    "Big Blue Bus Schedule": bbb_routes,
    "Big Blue Bus Swiftly Schedule": bbb_routes,
    "Culver City Schedule": culver_routes,
    "Torrance Schedule": torrance_routes,
    "G Trans Schedule": gtrans_routes,
    "Beach Cities Schedule": beach_cities_routes,
    "Beach Cities GMV Schedule": beach_cities_routes,
}


def get_operator_lax_routes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Subset to get only specific routes for an operator,
    since route_ids like 5, 6 are shared even amongst 
    these LAX transit operators.
    """
    df = df.assign(
        gtfs_dataset_name = df.feed_key.map(feed_to_name_dict),
    )

    list_of_dfs = []
    
    for operator, lax_routes_dict in operators_lax_routes.items():
        subset_df = df[
            (df.gtfs_dataset_name==operator) & 
            (df.route_id.isin(list(lax_routes_dict.keys())))
        ]
        
        subset_df = subset_df.assign(
            route_name = subset_df.route_id.map(lax_routes_dict)
        )
        
        list_of_dfs.append(subset_df)
 
    df2 = pd.concat(list_of_dfs, axis=0, ignore_index=True)
    
    return df2
