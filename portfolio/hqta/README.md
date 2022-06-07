# Introduction

All High Quality Transit Area (HQTA) related analysis is shown on this site.

## Facilities Services
Working with Facilities Strategic Initiative (FSI) to understand potential retrofits of Caltrans facilities.

The facility tiers are numbered in a proposed priority order in which the FSI project team will focus on for assessing how Caltrans plans, manages, prioritizes, and funds infrastructure needs.

Tier 1 facilities: Occupied buildings (owned or leased) used as a regular workplace by Caltrans employees that are intended to be permanent. 

### Research Question

Which Tier 1 facilities could be retrofitted to become bus depots and provide bus chargers? Focus on buses running within the HQTAs.

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


## Sacramento Area Council of Governments - Green Means Go

### Research Question

Show the overlap of high quality transit routes and REAP/LEAP funding (proxy via SACOG’s Green Means Go areas) and potentially Caltrans right of way. Regional entities are investing in these corridors. Caltrans has the opportunity to do the same to support walkable, infill environments. 


### Data
* [SACOG Green Means Go](https://sacog.maps.arcgis.com/apps/webappviewer/index.html?id=9fbf73e744c84aecbf7313fc74a04334&extent=-13552958.356,4656731.9575,-13481680.8271,4685930.9023,102100)
* HQTA (polygons)