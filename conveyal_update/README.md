# Updating GTFS/Network Bundles in Conveyal Analysis

## General Notes

* Conveyal is set up to ingest individual GTFS feeds (zipped feeds consisting of textfiles), while our warehouse extracts and transforms these. Potential approaches are:
    * synthesize something that looks like a GTFS feed from our warehouse data (not attempted here)
    * use our warehouse as a guide, but download and supply the raw individual feeds from when we archived them in GCS (this approach)
    
## Scripts

* Set target date in `conveyal_vars.py`. Region boundaries are also set here, but these should remain static unless the decision is made to use entirely different regions in Conveyal. Target date should be a mid-week day.
* `evaluate_feeds.py` includes functions to check to see which feeds have service defined on the target date, and show feeds without any apparent service, including if that service is apparently captured in another feed. This helps check for potential coverage gaps, likely due to GTFS feed expirations and/or the [publishing future service issue](https://github.com/MobilityData/GTFS_Schedule_Best-Practices/issues/48). You may have to shift the target date around to find the best overall coverage, and/or manually edit important but missing feeds to define service if reasonable.
* `match_feeds_regions.py` matches feeds to Conveyal regions, based on if the feed contains _any_ stops within each region.
* `download_data.py` downloads and zips original GTFS feeds, and additionally generates a shell script that can be used to download, crop, and filter OSM data for each region using Osmosis (not currently able to do so via hub, use other platform). Downloaded feeds are labelled `{row.gtfs_dataset_name.replace(" ", "_")}_{row.feed_key}_gtfs.zip`

## Workflow

* `make stage_conveyal_update` to run all scripts
* using generated `crop_filter_osm.sh` or other means, [update Conveyal network bundle](https://docs.conveyal.com/prepare-inputs#creating-a-network-bundle) for each region with cropped and filtered OSM data and feeds.
