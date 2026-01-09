# Transit Provider Dashboard 
Purpose: To define and quantify the service population of each Cal-ITP partner transit agency using census and related demographic data.

Goal: Provide agency-level summaries that describe the characteristics of populations served, such as size, demographics, income, and travel behavior, to illustrate the reach and impact of Cal-ITP services.

Use: Support data-driven storytelling and performance reporting by supplying key statistics for communications about the benefits, adoption, and equity potential of Cal-ITP initiatives (e.g., open-loop payment systems).

Steps:
- Querying ACS data via the Census API and upload results to a GCS bucket for later usage.
- Census Tract Geometry Processing
- Querying Organization Data from the Data Warehouse and Storing in GCS
- Querying Transit Stop Data and Merging Stop Data with Organization Information
- Spatial Analysis: Stop Buffers and Census Tract Intersections
- Adjusting Population and Demographic Metrics for Stop Service Areas

Files:

- 01_prepare_acs_data.ipynb
- 02_prepare_orgs_ridership_data.ipynb
- 03_prepare_stop_data.ipynb
- 04_data_processing.ipynb
- 05_reconciliation_processing.ipynb



