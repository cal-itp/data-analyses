# README

## [GitHub Milestones](https://github.com/cal-itp/data-infra/milestone/15)
Refer to milestones and individual GitHub issues.

### Data Creation
1. [warehouse queries](./warehouse_queries.py): Run to get warehouse queries, stash parquets used in notebooks
1. [create_coverage_data](./create_coverage_data.py): GTFS Static and GTFS RT coverage (all stops vs physically accessible stops) for census blocks and tracts. Used in notebooks 01, 02, 05, 06.
1. [create_accessibility_data](./create_accessibility_data.py): High-level accessibility metrics. Used in notebook 03.


### Visualization Helpers
1. [setup_charts](./setup_charts.py): Helper chart functions for notebooks 03, 04.

### Notebooks
1. [02_coverage_mapping](./02_coverage_mapping.ipynb): accessibility coverage maps
1. [06_summary_metrics](./06_summary_metrics.ipynb): accessibility coverage summary table
1. [03_accessibility_feeds](./03_accessibility_feeds.ipynb): accessibility charts
1. [04_validation_errors](./04_validation_errors.ipynb): validation errors charts
1. [07_fares_v2](./07_fares_v2.ipynb): fares v2 charts



