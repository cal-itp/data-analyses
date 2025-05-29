la_operators = [
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

subset_feeds = [
    '8d9623a1823a27925b7e2f00e44fc5bb',
    'dbee6840cff90155409e6383afd1f16d',
    '5aa5331522639832a9cfdb692b6945b2',
    'f6774d861953d4f4cdcffec95e2652c7',
    '895ab5a382406a279733af6164becd73',
    '7a3f513c343b16a30c135ed7d332b6d6',
    '21c71baff5435395124b47f4f36261a7',
    '8dc47f5bd4666e8c52178b5e08085eeb',
    'd95f2f26bbf4846e4eb84d352fb0990d'
]

subset_routes = {
    "803": "Metro C Line",
    "807": "Metro K Line",
    "102-13191": "Metro 102",
    "111-13191": "Metro 111",
    "117-13191": "Metro 117",
    "120-13191": "Metro 120",
    "232-13191": "Metro 232",
    "3": "BBB 3 Lincoln Boulevard/LAX",
    "R3": "BBB R3 Lincoln Boulevard/LAX",
    # swiftly codes route_id differently
    "3929": "BBB 3 Lincoln Blvd/LAX Rapid (Swiftly)", 
    "3930": "BBB 3 Lincoln Blvd/LAX (Swiftly)", 
    "6": "Culver 6 Sepulveda",
    "R6": "Culver R6 Sepulveda",
    "8": "Torrance 8",
    "5": "G Trans 5",
    "BCT109 NB": "Beach Cities Transit 109 NB",
    "BCT109 SB": "Beach Cities Transit 109 SB",
    "4815": "Beach Cities Transit 109 (GMV)" 
    # GMV schedule codes route_id as the same
}