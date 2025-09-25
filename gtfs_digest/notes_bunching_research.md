# Bunching
1. [Michael Haynes 1](https://www.linkedin.com/pulse/bus-bunching-trm-post-15-michael-haynes)
    * Haynes defines this as a bus going the same route and direction within 60 seconds of each other.
    * Saw that 15% of the time, buses were within 120 seconds of each other. 5% of the time they were within 60 seconds.
    * One minute came from analyzing data. 
    * This has clear instructions.
    * 

2. [Wikipedia Bus Bunching](https://en.wikipedia.org/wiki/Bus_bunching)
    * Two or more transit vehicles that were scheduled at regular intervals are bunched together.
    * Occurs because the first vehicle falls behind the schedule.
    * The first vehicle picks up passengers who are actually waiting for the second vehicle.
    * Picking up these passengers delay the first vehicle even more, allowing the second bus to catch up and decrease the interval between them.
    * Bunching can be more than just two buses.
    * How reduce this?
        * Skipping certain stops.
        * Not letting passengers board because another bus is coming.
        * Abandon schedules and strategically delay buses at each stop.
        
3. [Massachusetts Bay Transportation Authority Headway Management](https://static1.squarespace.com/static/533b9a24e4b01d79d0ae4376/t/645e82de1f570b31497c44dc/1683915486889/TransitMatters-Headwaymanagement.pdf)
    * This PDF deals with high-frequency bus corridors (meaning routes with buses every 15 minutes or less).
    * High frequency buses are only useful if they arrive at a consistent interval. 
    * Headway: create a constant gap between buses. 
        * Can compare this with schedule.
        * Headway is about dispatching buses at regularly spaced intervals depending on the day. Schedule requires transit drivers to follow strict departure and arrival times. 
    * High-frequency buses on timepoint schedule will often run early or late.
    * For high-frequency buses, it's important to maintain a headway for rider experience.
    * Routes with lower frequency, delays are actually of lesser consequence. 
    * The more frequent a route is, the harder it is to prevent bunching. 
        * The problem increasingly worsens.
    * Bunching leads to: some people don't have to wait very long for a bus, but most have to wait a long time and get on a crowded bus.
    * Bunching is defined as buses that run within two minutes or less of each other.
        * AH: <b>How did they decide 2 minutes as the "threshold?"</b>
    * MBTA lists the exact stops in which the bus bunches and the % of the time it bunches.
    * Headway requires GPS/AVL (automatic vehicle location) data.
        * Operators need to be trained on how to manage headway.
        * Provide technology on the bus that allows drivers to see the distance between the buses going on the same route as they are.
        * Operators and dispatchers need to communicate information.
        * Dispatch reserve buses if there is a gap when a bus is delayed.
        * Create queue jump lanes, dedicated bus lanes, and signal priority.
     * MBTA flagged routes that are high frequency
          * High frequency is defined as 15 minutes or less. 
          * Riders can arrive at the stop without checking.
          * Route 70
              * This route is slow because it is long, not frequent enough, and there is a short distance between each stop.
          * Routes 35/36/37
              * These routes are scheduled to be spaced 15-30 minutes but in reality, it is spaced 5-10 minutes on average.  
    * Implementation Details
        * Focus on routes with headways of 15 minutes or less.
        * Use relevant data: ridership at different time points, automatic passenger counter data, travel speed, route specific headway, operator reports.
        * MBTA does not currently do real-time monitoring of vehicles using GPS or AVL. 
        
4. [Transit Matters Reveals the MBTA's Slowest and Most Bunched Buses](https://transitmatters.org/blog/reveal-mbtas-slowest-most-bunched-bus)
    * This article is about the 10 slowest buses and the 10 most bunched buses.
    * Identifies the most unreliable routes and gives suggestions.
    * Goal is also to confirm rider experience and promote colalborative efforst.
    * [Full report](https://static1.squarespace.com/static/533b9a24e4b01d79d0ae4376/t/6617ec40675223398aac12bf/1712843871514/TransitMatters-Bus-Bunching-Reports-Oct-2023)
        * Looks like it was made in Python.
        * Bunching is defined as <25% of the scheduled headway.
            * Does this mean: if two buses heading the same route and direction are supposed to be 10 minutes apart, instead they are actually about 3 minutes apart?

5. [Deriving Transit Performance Metrics from GTFS Data
Project Abstract](https://www.morgan.edu/national-transportation-center/the-smarter-center-(2023-2029)/research/deriving-transit-performance-metrics-from-gtfs-data)
    * Project will derive schedule related performance metrics based on GTFS.
    * Schedule deviation: on time performance and bus bunching. 
    * Look at stop and segment level.
    * Project will collect multiple days of schedule and realtime data from 1+ agency. 
    * Create a tool that will allow a user to interact with the data.

6. [Identifying spatio-temporal patterns of bus bunching in urban networks](https://www.tandfonline.com/doi/full/10.1080/15472450.2020.1722949?scroll=top&needAccess=true)
    * "The objective of this paper is to identify hot spots of bus bunching events at the network level, both in time and space, using Automatic Vehicle Location (AVL) data from the Athens (Greece) Public Transportation System...A two-step spatio-temporal clustering analysis is employed for identifying localized hot spots in space and time and for refining detected hot spots."
    * Amanda: couldn't find this paper in Caltrans' JSTOR database.

7. [HEADWAY ADHERENCE. DETECTION AND REDUCTION OF THE BUS BUNCHING EFFECT](https://aetransport.org/public/downloads/Bv7HG/4816-57cd5cc05c897.pdf)
    * Good transit means: high frequencies, low stop spacings, regularity.
    * Bus systems in densely populated cities usually operate at a short headway but due to a variety of reasons, usually buses bunch together causing unstable headways. 
    * Frequent buses usually arrive at the same time, followed by a long wait for a next one to go the same direction.
    * The bunching effect, according to the TCQSM, Transit Capacity
and Quality of Services Manual, can be monitored as the coefficient of
variation of headways, Cv.h: the standard deviation of headways (representing
the range of actual headways), divided by the average (mean) headway.
    * Measuring Bus Regularity
        * Excess Wait Time: average additional waiting time a passenger experiences minus the scheduled wait time.
        * Standard Deviation: standard deviation of the EWT.
        * Wait assessment: a bus is considered regular if it arrives within 2 minutes of its scheduled arrival time. 
        * Service regularity: % of actual headway within 20% of scheduled headway. The higher the percentage, the more regular the servicve.
        * Bus bunching: dividing standard deviation of actual headway with average headway.
    * Bus motion and control strategies
        * Not relevant.
     * This paper uses 4 methodologies to measure service regularity: Excess Wait Time, standard deviation, wait assessment, and service availability.
     * EWT is the only method that reflects the average experience of all passengers.
         * EWT is useful if a route's headway is scheduled in regular intervals.
         
8. [Headway](https://www.uitp.org/news/what-is-bus-headway-and-how-it-impacts-public-transport-quality/)
    * The time between 2 vehicles on the same route.
    * The more headway: the longer the wait.
    * This is different than frequency: frequency is how many times a bus stops at a particular stop.
    * If a bus of the same route passes a stop every 20 minutes, then that bus has a 20 minute headway and a frequency of 3x per hour.
    * 
9. [Transit Capacity and Quality of Service Manual, Third Edition (2013)](https://nap.nationalacademies.org/read/24766/chapter/1)
    * Chapter 15
    * Bunching is about comfort and convenience: it's a metric that reflects the reliability of the bus.
    * Page 198
        * An average headway between 5-10 minutes is considered frequent.
        
    * Page 199: Average headway chart 
        * Existing service frequency is determined using agency's timetable (scheduled) data. 
    * Page 200
        * Hours of service is also interesting (I know it's not related to bunching but it could be something to explore on a route basis.)
    * Page 222
        * Headway adherence: the eveness of the interval between transit time.
        * Derived from measured bus departure times, a task that is simplified using archived AVL data (is this GTFS?)
        * Headway adherence: coefficient of variation of headways (the standard deviation of headways divided by the mean headway).
    * Page 224
        * Headways of 10 minutes or less: vehicle bunching more likely to occur. 
        * Bunching: 2+ vehicles on the same route arrive together, followed by a long waiting period for another vehicle to come. 
        * Headway adherence can also be thoguht of the regularity of vehicle arrivals with respect to the scheduled headway.
        * Headway adherence: take the standard deviation of headways divided by average/mean headway. 
            * standard deviation: represents the range of actual headways
        Headway adhernce in this paper is calculated as the coefficient of variation of headways Cvh.
        * Cvh: a statisical measure, stakeholders might not understand. 
        
   * Page 225
       * There's a chart that matches Cvh values with passenger and operator persective. 
       * What's the difference between "off headway" vs. "bunching?" They seem similar.
   * Page 226
        * Use the chart on page 226 to determine how off a headway a route is. 
    * <i>headway- the time interval between the passing
of the front ends of successive transit units
(vehicles or trains) moving along the same lane or
track (or other guideway) in the same direction,
usually expressed in minutes; see also service
frequency.</i>


# What is needed
* Schedule data for routes: Identify which routes are scheduled at a high frequency
    * What does "high frequency" even mean? 
        * A bus going the same route/dir connection is scheduled for every 10 minutes according to the Transit Capacity Manual?

* Schedule data for stops
    * We need to know the times the bus is scheduled to stop at each stop, to compare it with when it actually stopped
* Look at stop level realtime data. 
    * Forgot where to find this.
    * Look at how often bunching occurs at a particular stop for buses going the same route-direction
    * How do we find out how much headway is actually spaced between buses going the same route and direction versus the scheduled headway?

* Determine what is considered bunching? Is it when 2+ buses are a distance within 1 minute of each other? 2 minutes?
    * This idea comes from [MBTA](https://static1.squarespace.com/static/533b9a24e4b01d79d0ae4376/t/645e82de1f570b31497c44dc/1683915486889/TransitMatters-Headwaymanagement.pd)
    * How did MBTA come up with 1-2 minutes as their "guideline"?
    * This will be compared against the route itself? Or against the entire operator? Or the entire state?
* [This resource](https://aetransport.org/public/downloads/Bv7HG/4816-57cd5cc05c897.pdf) used standard deviation pages 3-4. 
* Split bunching by weekday, Saturday, and Sunday? Or overall?
