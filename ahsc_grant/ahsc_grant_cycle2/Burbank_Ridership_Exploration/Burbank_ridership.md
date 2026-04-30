# BurbankBus Ridership Modeling: Analytical Summary and Interpretation

## Summary of Improvements Received from the Agency.
Three-year pilot system expansion of BurbankBus service with a new line (Blue Route), an extended line (Orange Route), and addition of weekend service on all lines
- Will add two new transit connections to the Downtown Metrolink station, which serves Metrolink’s Antelope Valley and Ventura County lines, Amtrak’s LOSSAN corridor, six Metro lines, Santa Clarita Transit, and Glendale Beeline
- Orange Route is the only transit connection from BUR airport to Metro rail (North Hollywood B Line); it currently does not run on weekends, limiting potential for employee mode shift and utility for weekend visitors
- Will add transit connections to major regional retail at the Empire Center (Target, Walmart, Lowes, REI, etc.) and Downtown Burbank
- Will adapt service to post-pandemic ridership changes by shifting from a primarily commuter focus to providing weekend service, with connections to major tourist destinations at the Warner Brothers and Universal studios as well as the BUR airport
- Expand and improve transit service to increase ridership for other major destinations and for major events including the LA28 Olympics
•	Woodbury University
•	Providence Saint Joseph Medical Center
•	Major employers such as Disney Studios and Netflix


1. Purpose of the Analysis
The goal of this modeling exercise is to estimate how BurbankBus ridership is expected to change under a system expansion scenario that:
- New Blue Route	n_routes +1
- Extended Orange Route	n_routes +1 (partial effect)
- More service / frequency	n_arrivals ↑
- Weekend service	: currently this model is just weekday model
- New stops	new rows (or modified features)


2. Methodology
A statistical model is used to predict ridership changes at the stop level, based on service characteristics and surrounding population demographics.
Transit ridership data are count data—they are non-negative integers with skewness and often over-dispersion (variance > mean). The Negative Binomial model is ideal for this because:

 It handles over-dispersed count outcomes better than a Poisson model.
 It directly models the expected number of daily boardings at each stop.
 The model estimates how ridership responds to changes in:

- Number of routes serving a stop,
- Number of daily scheduled arrivals,
- Surrounding population and socioeconomic characteristics.



3. Interpretation of Key Predictors

a. Number of Routes (n_routes)
Surprisingly, the model suggests that adding a route, holding arrivals constant, is associated with lower ridership. This happens because:
- If a stop gains a second route but both routes have low frequencies, the model detects that “more routes” alone does not increase accessibility.
- In most systems, frequency, not the count of routes, is the strongest driver of ridership.
- Routes with low headways or overlapping coverage can inflate the route count without increasing usable service.
- The model interprets “more routes without more service” as route fragmentation rather than increased access.
- Thus, the negative sign is not implausible—it reflects that routes matter only when they come with meaningful service levels.

b. Arrivals / Frequency (n_arrivals and log_arrivals)
This is the strongest operational predictor. More scheduled arrivals → shorter waits → more usable transit.
The log transformation captures diminishing marginal returns:

Going from 0 → 30 arrivals produces a large ridership jump.
Going from 30 → 60 arrivals has a smaller incremental effect.



c. Demographic Variables
Population-weighted demographics within a 0.5-mile buffer approximate the market for transit:

Total population: more people → more potential riders.
Workers with no car: high transit reliance → higher ridership baseline.
Youth and senior populations: proxies for transit-dependent groups.
Public assistance population: indicator of economic need and transit dependency.
All contribute proportionally to expected boardings.
