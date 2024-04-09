```mermaid
---
title: Open Data - CA Transit Stops and Routes
---
%%{
  init: {
    'theme': 'base',
    'themeVariables': {
      'primaryColor': '#E5F5FA',
      'primaryTextColor': '#000',
      'primaryBorderColor': '#000',
      'lineColor': '#000',
      'secondaryColor': '#EFF7EB',
      'tertiaryColor': '#fff'
    }
  }
}%%

flowchart TB;
    trips --> ca_transit_routes;
    shapes --> ca_transit_routes
    trips --> ca_transit_stops;
    stops --> ca_transit_stops;
    stop_times --> ca_transit_stops;
```