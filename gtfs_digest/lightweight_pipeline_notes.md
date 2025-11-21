# Lightweight GTFS pipelines

**Goal: warehouse changes perpetuated quickly to data products**

For GTFS data products, we have a good understanding of what we want. As the scripts migrate into dbt models, most of these can be cycled back to the data products quickly. We want to take advantage of this. Can we achieve a rollup tables -> data product type of workflow to get to minimal maintenance? Do a similar thing where warehouse tables are displayed in Metabase dashboards, but with the option of doing more geospatial stuff and more complicated visualizations that Metabase can't easily do.

It would be painstaking to cache and rerun all the Python scripts here when big changes are made. Ideally, we want all good changes to percolate through, but have the ability to reproduce errors or stand up a fail-safe option to revert.


* Running queries on different days can return different results depending on what's happening in the warehouse
   * Happened with holiday query, which is extremely small, rows are just operators, and queries were run perhaps 2 weeks apart
   * Reasonable to expect this to happen on something much larger where we have even less visibility into what's happening
   * GTFS materialized tables get overwritten completely twice a week. Theoretically, not much should be changing, but is this a safe assumption?
   * Find middle ground where we are not aggressively caching and doing data processing outside of warehouse, still cache snapshots as fail-safe, and not lose or create rows in ways we do not intentionally want or understand well
