# Facilities Services

Working with Facilities Strategic Initiative to understand potential retrofits of Caltrans facilities. .

The facility tiers are numbered in a proposed priority order in which the FSI project team will focus on for assessing how Caltrans plans, manages, prioritizes, and funds infrastructure needs.

Tier 1 facilities: Occupied buildings (owned or leased) used as a regular workplace by Caltrans employees that are intended to be permanent. 

## Research Question

Which Tier 1 facilities could be retrofitted to become bus depots and provide bus chargers? Focus on buses running within the High Quality Transit Areas.

The facility types include office buildings, equipment shops, maintenance stations, transportation mangement centers, and materials labs (categories I, II).

### Data
* Tier 1 facilities - to be geocoded
* HQTA major bus stops?
* HQTA shape? Don't need rail or ferry ones

### Steps
* Data import, basic cleaning, standardize across, assemble into 1
* Geocode addresses (put some in geocoder, some manually done if descriptions are given for maintenance facilities)
* Pick HQTA file to use
* Bring in route or stop info? Only picking locations near the origin/destination of route?
* Need to join to HQTA bus stops (major stops? minor stops?)
* Do a first pass to see just how many facilities even join to the HQTA all shapes dissolved. If very few, there are few eligible to be retrofitted...won't matter if it's near start/end of route.