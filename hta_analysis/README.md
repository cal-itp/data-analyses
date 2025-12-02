# Big Data Trip Analysis: Origin-Destinations in Humboldt

### Analysis Background
This analysis looks into the Origin and Destination pairings for trips starting and ending at Cal Poly Humboldt to better understand where and when people are traveling. The analysis uses 
[Replica](https://www.replicahq.com/) Trip Data, which is data that represents what travel looks like during a typical Weekday (Thursday) or Weekend (Saturday) during the Fall or Spring time. These seasons are chosen due to trips types changing in the Summer and Winter as a result of weather, school vacations and other factors. Replica offers two types of data, population data and trip data. We ultimately chose the trip data to gain insights into the trip types, trip timing and duration, as well as trip locations. This type of data is helpful because our traditional data sources, such as our counters on the State Highway Network, are limited to the State Highway Routes, and do not have the ability to determine where the vehicles are coming or going to. Additionally, non-traditional data sources like Replica allow us to get a better understanding on and off the State Highway Network, and distinguish the types of trips occurring. 

### Analysis Parameters
For this analysis, we chose Spring 2025 and set the trip origin filter as the Census Block Group that contains Cal Poly Humboldt. The data then was filtered to all trips that had an origin in the Cal Poly Humboldt block group. We then conducted the same analysis but changed the trip filter to have the destination set as Cal Poly Humboldt to get the trips that ended at the university. Each trip dataset was downloaded from Replica and used in this analysis to easily compare the trip differences in trips to and from the university. 


##### Replica Documentation

Replica Documentation: [Seasonal Trip Table](https://documentation.replicahq.com/docs/disaggregate-trip-tables)