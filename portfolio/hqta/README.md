# Introduction2

Working with Facilities Strategic Initiative (FSI) to understand potential retrofits of Caltrans facilities.

The facility tiers are numbered in a proposed priority order in which the FSI project team will focus on for assessing how Caltrans plans, manages, prioritizes, and funds infrastructure needs.

Tier 1 facilities: Occupied buildings (owned or leased) used as a regular workplace by Caltrans employees that are intended to be permanent. 

## Research Question

Which Tier 1 facilities could be retrofitted to become bus depots and provide bus chargers? Focus on buses running within the High Quality Transit Areas (HQTAs).

The facility types include office buildings, equipment shops, maintenance stations, transportation mangement centers, and materials labs (categories I, II).

### Data

* Tier 1 facilities with addresses
* HQTA (polygons)
* State highway network postmiles (open data)
* Caltrans districts, CA county polygons (open data)
* [Data cleaning scripts](https://github.com/cal-itp/data-analyses/tree/main/facilities_services)

### Methodology

The locations of Tier 1 facilities were provided with addresses, and in some cases, the postmile marker along the highway. Varying information was provided for each facility category. For example, office locations included square footage and whether the property was owned or leased by Caltrans, maintenance locations were designated by postmiles, and lab locations also had a category II or III flag.

The processed data included as much information in common, such as addresses and categories, along with additional useful information, such as square footage. All the locations were geocoded using an ESRI geocoder wherever possible. A handful were manually geocoded. The geocoded data was then spatially joined to Caltrans district and CA county polygons to provide additional geographical information.

### Notes

* Where square footage was not provided, such as for equipment and maintenance facilities, those values are left missing.
