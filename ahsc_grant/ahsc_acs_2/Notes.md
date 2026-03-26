Route-level agencies (e.g., SacRT Bus, SamTrans, Gold Coast Transit):

They report ridership per route per stop per day.
Any service increase (extra trips) can be directly linked to a specific route.
The model coefficient for n_trips tells you how adding a bus trip on that route increases ridership.
The n_routes coefficient is irrelevant because each route is already counted individually.


Aggregated agencies (e.g., Caltrain, BART):
They report ridership aggregated across multiple routes at a stop or over the whole system — not per route.
Any service increase is spread across the stop’s ridership, so we cannot directly tie extra trips to a single route.
The model coefficient for n_trips still tells us the expected increase in ridership per additional trip, but it reflects the average effect across all routes included in the aggregation.
Example: If Caltrain runs 4 trains per day and you add 1 more train, the n_trips coefficient predicts the average ridership increase per stop.
The n_routes_effective coefficient is meaningful here, because the number of routes contributes to the total aggregated ridership — more routes generally mean more total boardings at a stop.
