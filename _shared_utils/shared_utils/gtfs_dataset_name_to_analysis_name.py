"""
bridge_gtfs_analysis_name_x_ntd
matches schedule_gtfs_dataset_name to analysis_name.
It looks on dim_gtfs_dataset schedule_source_record_id
to figure out how to apply analysis_name to dates before we added it.

However, if that record is altered and a new schedule_source_record_id
is used, we need to be able to surface those occurrences.

These are ones where we have a preferred feed to publish,
so we want to exclude.
"""

bay_area_known_dupes = [
    "Bay Area 511 Regional Schedule",  # excluded, yes
    "AC Transit Schedule",  # dupe with Bay Area 511 AC Transit Schedule
    "ACE Schedule",  # dupe with Bay Area 511 ACE Schedule
    "BART Schedule",  # dupe with Bay Area 511 BART Schedule
    "Bay Area 511 Emery Express Schedule",  # public currently operating = N (data_pipeline is ok; not private, is regional subfeed, so this will come up)
    "Caltrain Schedule",  # dupe with Bay Area 511 Caltrain Schedule
    "Capitol Corridor Schedule",  # dupe with Bay Area 511 Capitol Corridor Schedule
    "Commute.org Schedules",  # regional precursor feed
    "County Connection Schedule",  # dupe with Bay Area 511 County Connection
    "Emery Go-Round TripShot Schedule",  # dupe with Bay Area 511 Emery Go-Round Schedule
    "Fairfield Schedule",  # dupe with Bay Area 511 Fairfield and Suisun Transit Schedule
    "Golden Gate Bridge Schedule",  # regional precursor feed
    "Marin Optibus Schedule",  # dupe with Bay Area 511 Marin Schedule
    "Mountain View Community Shuttle Schedule",  # dupe with Bay Area 511 Mountain View Community Shuttle Schedule
    "MVGO Schedule",  # dupe with Bay Area 511 MVGO Schedule
    "Petaluma GMV Schedule",  # dupe with Bay Area 511 Petaluma Schedule
    "SamTrans Schedule",  # dupe with Bay Area 511 SamTrans Schedule
    "San Francisco Bay Ferry Schedule",  # dupe with Bay Area 511 San Francisco Bay Ferry Schedule
    "Santa Rosa CityBus GMV Schedule",  # dupe with Bay Area 511 Santa Rosa CityBus Schedule
    "SCVTA Schedule",  # dupe with Bay Area 511 Santa Clara Transit Schedule
    "SMART Schedule",  # dupe with Bay Area 511 Sonoma-Marin Area Rail Transit Schedule
    "SolTrans Schedule",  # dupe with Bay Area 511 SolTrans Schedule
    "Sonoma Schedule",  # dupe with Bay Area 511 Sonoma County Transit Schedule
    "South San Francisco Schedule",  # dupe with Bay Area 511 South San Francisco Shuttle Schedule
    "Tri-Valley Wheels Schedule",  # dupe with Bay Area 511 Tri-Valley Wheels Schedule
    "Union City GMV Schedule",  # regional precursor feed, dupe with Bay Area 511 Union City Transit Schedule
    "Vacaville Schedule",  # dupe with Bay Area 511 Vacaville City Coach Schedule
    "Vine GMV Schedule",  # dupe with Bay Area 511 Vine Transit Schedule
    "Vine Schedule",  # dupe with Bay Area 511 Vine Transit Schedule
    "WestCAT Schedule",  # dupe with Bay Area 511 WestCAT Schedule
]


other_known_dupes = [
    # Ventura County regional feed (VCTC, Gold Coast, Cities of Camarillo, Moorpark, Ojai, Simi Valley, Thousand Oaks)
    "Gold Coast Schedule",  # regional precursor feed, now VCTC GMV
    "Moorpark Schedule"  # regional precursor feed, now VCTC GMV
    "Simi Valley Schedule",  # CHECK THIS, now VCTC GMV
    "Thousand Oaks Schedule",  # CHECK THIS, now VCTC GMV
    "Basin Transit GMV Schedule",  # dupe with Morongo Basin Schedule
    "Cerritos on Wheels Schedule",  # dupe with Cerritos on Wheels Website Schedule
    "Desert Roadrunner GMV Schedule",  # dupe with Desert Roadrunner Schedule
    "Lawndale Beat GMV Schedule",  # dupe with Lawndale Schedule
    "LAX Shuttles Schedule",  # dupe with LAX Flyaway Bus Schedule, also LAX FlyAway Schedule present
    "Merced GMV Schedule",  # dupe with Merced Schedule
    "Mountain Transit GMV Schedule",  # dupe with Mountain Transit Schedule
    "Tahoe Transportation District GMV Schedule",  # dupe with Tahoe Transportation District Schedule
    "Victor Valley GMV Schedule",  # dupe with Victor Valley Schedule
]
