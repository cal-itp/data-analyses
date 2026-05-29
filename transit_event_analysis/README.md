# Transit Stop Analysis Tool

Identifies **bus and rail transit stops** near any venue and displays interactive maps and summary tables.

---

## Repo Structure

```
├── analysis.ipynb                  # Main notebook — open and run this
├── refresh_data.ipynb              # Run this to fetch fresh data from BigQuery
├── query.sql                       # BigQuery SQL used to fetch stops data
├── df_dim_stops_latest.parquet     # Pre-fetched stops data
├── utils/
│   └── geo_functions.py            # All analysis logic (do not edit unless updating logic)
└── README.md
```

---

## How to Use

1. Open **`analysis.ipynb`** in JupyterHub
2. From the top menu click **Run → Run All Cells**
3. A form will appear with:
   - **Venue name** text box — type any venue (e.g. `SoFi Stadium`, `Chase Center`)
   - **Bus buffer** slider — radius in miles for bus stop search (default: 3 mi)
   - **Rail buffer** slider — radius in miles for rail station search (default: 10 mi)
4. Click **▶ Run Analysis**
5. Maps and tables will appear below

---

## Refreshing the Stops Data

The notebook reads from `df_dim_stops_latest.parquet` in the root directory.  
This file contains the latest transit stops data fetched from BigQuery.

If you need to refresh the data (e.g. new agencies or stops have been added):

1. Open **`refresh_data.ipynb`**
2. Click **Run → Run All Cells**
3. Wait for the query to complete — this may take a few minutes
4. The parquet file will be automatically overwritten with the latest data

> ⚠️ The refresh query bills against BigQuery — only run when fresh data is actually needed.

---

## Requirements

```
pandas
numpy
folium
geopy
ipywidgets
```

---

## Notes

- The `query.sql` file contains the full BigQuery SQL used to generate the stops dataset
- Buffer distances can be adjusted interactively via the sliders — no code editing needed
