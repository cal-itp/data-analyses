# Research Questions

## Olympic Lanes
* Andrew Quinn (Roadway Pricing) had idea to pitch Olympic Lanes.
* Henry presented Olympic Lanes research related to I-10 and I-110 by hour.
    * Mean flows per lane, lane-by-lane for peak periods (6-9 AM and 4-7 PM)
    * Sum of flows per lane, over 3 month period, by hour (then aggregated)
    * Totals will look better, but question was by lane. --> what about per GP lane vs per HOT/HOV lane, so we use totals and then normalize?
    * x-axis is postmile (test whether we can join this to SHN lines)
* Way Traffic Ops treated express lanes is confusing
    * Lanes 1-2 = HOV (sensor is coded this). HOT and HOV classification are mutually exclusive.
    * Lanes 4-6 are General Purpose
    * 110 looks better than 10.
* Data quality concerns because number of sensors may matter, whether they're working or not matter    
    * a lane can drop out, because at each postmile, there can be varying number of lanes
    * it does appear that lane volumes drop down, only to pick up later on at a much higher volume
    * we know that sensors aren't always working, and sometimes they impute stuff. Maybe try only using sensors that are working and then interpolate in between?
    * also removing ramps, since ramps include something different (already excluded in the analysis)
 
    

## Related Questions
Gather a list of typical questions that are asked around this dataset. Let's see what other datasets we can produce.
* How speed affects throughput
* How occupancy differs
* Speed, throughput together
* Occupany of lanes (person throughput). Less vehicles go through HOT/HOV, but can carry more people?
* flow, occupancy, avg speed in a quadrant table
* nonpeak non-congested, peak congested, congested-peak, noncongested offpeak
   * we know that HOT/HOV more efficient during congested times
   * avg lane mile speed, avg lane mile throughput, avg lane mile occupancy
   * do this at corridor level (maybe not 50 mile corridor, but can we chunk this into shorter corridors)
   * under congestion, if GP and HOT/HOV have same flow, then a conversion has no impact on it, and in theory, would improve throughput and occupancy
      * right now, ppl don't want to switch from GP to HOT, but are willing to switch from HOV to HOT. they don't want to wreck GP lanes. if flow is actually the same, then there's no impact.
   * add a lane during traffic really is not a good idea, because traffic does occur at certain bottlenecks
   * express lanes pick up 18.5 - 47.5 (look along this length), how many ppl do you move, vehicles you move, speed at which you move them, across congested vs non-congested time (peak vs offpeak)
   * 4 GP lanes move Y1 ppl vs 2 HOT lanes move Y2 ppl
   * 10 has 1 lane on some part, 2 lanes on other parts
   
## References
* [detector status report](https://github.com/cagov/caldata-mdsa-caltrans-pems/issues/269)
* [high flows GH issue](https://github.com/cagov/caldata-mdsa-caltrans-pems/issues/278)
* [dbt models sources.yml](https://github.com/cagov/caldata-mdsa-caltrans-pems/blob/main/transform/models/_sources.yml)
* [grains naming conventions](https://github.com/cagov/caldata-mdsa-caltrans-pems/issues/241)