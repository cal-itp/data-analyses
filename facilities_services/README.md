# Introduction

All High Quality Transit Area (HQTA) related analysis is shown on this site.

## Open Data Portal
1. [HQTA Areas](https://gis.data.ca.gov/datasets/863e61eacbf3463ab239beb3cee4a2c3_0)
1. [HQTA Stops](https://gis.data.ca.gov/datasets/f6c30480f0e84be699383192c099a6a4_0)
1. [CA Transit Routes](https://gis.data.ca.gov/datasets/dd7cb74665a14859a59b8c31d3bc5a3e_0)
1. [CA Transit Stops](https://gis.data.ca.gov/datasets/900992cc94ab49dbbb906d8f147c2a72_0)

## [PyData Global 2022 Slides](https://github.com/cal-itp/data-analyses/blob/main/high_quality_transit_areas/pydata_dask_slides.pdf)

## Facilities Services
Working with Facilities Strategic Initiative (FSI) to understand potential retrofits of Caltrans facilities.

The facility tiers are numbered in a proposed priority order in which the FSI project team will focus on for assessing how Caltrans plans, manages, prioritizes, and funds infrastructure needs.

Tier 1 facilities: Occupied buildings (owned or leased) used as a regular workplace by Caltrans employees that are intended to be permanent. 

### Research Question

Which Tier 1 facilities could be retrofitted to become bus depots and provide bus chargers? Focus on buses running within the HQTAs.

The facility types include office buildings, equipment shops, maintenance stations, transportation mangement centers, and materials labs (categories I, II).

### Data

* Tier 1 facilities with addresses
* [HQTA (polygons)](https://gis.data.ca.gov/datasets/863e61eacbf3463ab239beb3cee4a2c3_0)
* [State highway network postmiles](https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHhighway/SHN_Postmiles_Tenth/FeatureServer/0/) 
* [Caltrans districts (polygons)](https://gis.data.ca.gov/datasets/0144574f750f4ccc88749004aca6eb0c_0.geojson?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D)
* [CA counties (polygons)](https://gis.data.cnra.ca.gov/datasets/CALFIRE-Forestry::california-counties-1.geojson?outSR=%7B)
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
* [HQTA (polygons)](https://gis.data.ca.gov/datasets/863e61eacbf3463ab239beb3cee4a2c3_0)