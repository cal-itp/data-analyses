"""
geo_functions.py
----------------
All logic for transit stop analysis around a venue.
Called by analysis.ipynb — do not run this file directly.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import folium
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.distance import geodesic
from IPython.display import display, HTML


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATA_DIR = Path("data")

COLORS = [
    "blue", "green", "purple", "orange", "darkred",
    "black", "beige", "darkblue", "darkgreen", "cadetblue",
    "darkpurple", "pink", "lightblue", "lightgreen", "gray"
]

TABLE_STYLES = [
    {
        "selector": "table",
        "props": [
            ("border", "2px solid black"),
            ("border-collapse", "collapse")
        ]
    },
    {
        "selector": "th, td",
        "props": [
            ("border", "1px solid gray"),
            ("padding", "6px")
        ]
    },
    {
        "selector": "caption",
        "props": [
            ("caption-side", "top"),
            ("font-size", "16px"),
            ("font-weight", "bold")
        ]
    }
]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_stops_data() -> pd.DataFrame:
    """Load the pre-fetched stops pickle from the data directory."""
    return pd.read_parquet("df_dim_stops_latest.parquet")


# ---------------------------------------------------------------------------
# Geocoding & filtering
# ---------------------------------------------------------------------------

def geocode_venue(venue_name: str) -> tuple[float | None, float | None, str | None]:
    """
    Return (latitude, longitude, corrected_name) for a venue name.
    - corrected_name is the first part of the address returned by Nominatim,
      reflecting the canonical/corrected venue name.
    - If the user typed a minor typo that Nominatim resolved, corrected_name
      will reflect the real location name to use in table/map captions.
    - Returns (None, None, None) with a friendly printed message if geocoding fails.
    """
    geolocator = Nominatim(user_agent="transit_event_analysis")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    try:
        location = geocode(venue_name)
    except Exception as e:
        print(f"⚠️  Geocoding service error: {e}")
        print("    Please check your internet connection and try again.")
        return None, None, None

    if location is None:
        print(f"❌  Could not find a location matching '{venue_name}'.")
        print("💡  Tips:")
        print("      • Check your spelling")
        print("      • Add a city or state: e.g. 'SoFi Stadium, Los Angeles'")
        print("      • Try a nearby landmark or the street address instead")
        print("      → Please correct the venue name and click ▶ Run Analysis again.")
        return None, None, None

    # Decide what name to show in captions based on what the user typed:
    # - If the input contained a comma → user entered a full/partial address
    #   → show the full corrected address returned by Nominatim
    # - If no comma → user entered a simple venue name (e.g. "SoFi Stadium")
    #   → show just the first segment of the corrected address (the venue name)
    if "," in venue_name:
        corrected_name = location.address
    else:
        corrected_name = location.address.split(",")[0].strip()

    # Notify the user if the name differs from what they typed
    if corrected_name.lower() != venue_name.strip().lower():
        print(f"   ℹ️  Interpreted as: '{corrected_name}'")

    return location.latitude, location.longitude, corrected_name


def filter_nearby_stops(
    df: pd.DataFrame,
    venue_lat: float,
    venue_lon: float,
    max_miles: float
) -> pd.DataFrame:
    """
    Add a distance_miles column and return only stops within max_miles.
    """
    df = df.copy()
    df["distance_miles"] = df.apply(
        lambda row: geodesic(
            (venue_lat, venue_lon),
            (row["stop_lat"], row["stop_lon"])
        ).miles,
        axis=1
    )
    return df[df["distance_miles"] <= max_miles].copy()


# ---------------------------------------------------------------------------
# Bus helpers
# ---------------------------------------------------------------------------

def get_bus_stops(
    nearby_stops: pd.DataFrame,
    bus_buffer_miles: float
) -> pd.DataFrame:
    """Filter nearby stops to bus stops within bus_buffer_miles."""
    return nearby_stops[
        (nearby_stops["mode_type"] == "Bus") &
        (nearby_stops["distance_miles"] <= bus_buffer_miles)
    ].copy()


def display_bus_table(
    bus_stops: pd.DataFrame,
    bus_buffer_miles: float,
    venue_name: str
) -> None:
    """Display a styled summary table of bus stops by agency."""
    bus_summary = (
        bus_stops
        .groupby("organization_name")
        .size()
        .reset_index(name="bus_stop_count")
        .sort_values("bus_stop_count", ascending=False)
        .reset_index(drop=True)
        .rename(columns={
            "organization_name": "Transit Agency",
            "bus_stop_count": "Bus Stop Count"
        })
    )

    display(
        bus_summary.style
        .set_caption(
            f"Number of Bus Stops Within {bus_buffer_miles} Miles of {venue_name}"
        )
        .hide(axis="index")
        .background_gradient(subset=["Bus Stop Count"], cmap="Blues")
        .set_table_styles(TABLE_STYLES)
    )


def display_bus_map(
    bus_stops: pd.DataFrame,
    venue_lat: float,
    venue_lon: float,
    bus_buffer_miles: float,
    venue_name: str
) -> folium.Map:
    """Build and return a Folium map of bus stops around the venue."""
    organizations = sorted(bus_stops["organization_name"].dropna().unique())
    org_color_map = {
        org: COLORS[i % len(COLORS)]
        for i, org in enumerate(organizations)
    }

    m = folium.Map(location=[venue_lat, venue_lon], zoom_start=13)

    # Title
    m.get_root().html.add_child(folium.Element(f"""
        <h3 align="center" style="font-size:20px">
        <b>Bus Stations Within {bus_buffer_miles} Miles of {venue_name}</b>
        </h3>
    """))

    # Venue marker
    folium.Marker(
        [venue_lat, venue_lon],
        tooltip=venue_name,
        icon=folium.Icon(color="red")
    ).add_to(m)

    # Buffer circle
    folium.Circle(
        location=[venue_lat, venue_lon],
        radius=bus_buffer_miles * 1609.344,
        color="black", weight=2, fill=False,
        dash_array="8, 8",
        tooltip=f"{bus_buffer_miles}-mile bus buffer"
    ).add_to(m)

    # Stop markers
    for _, row in bus_stops.iterrows():
        org = row["organization_name"]
        folium.CircleMarker(
            location=[row["stop_lat"], row["stop_lon"]],
            radius=3,
            color=org_color_map.get(org, "gray"),
            fill=True,
            fill_color=org_color_map.get(org, "gray"),
            fill_opacity=0.7,
            tooltip=f"{row['stop_name']} | {org}"
        ).add_to(m)

    # Legend
    legend_html = """
    <div style="
        position: fixed; bottom: 50px; left: 50px; z-index: 9999;
        background-color: white; padding: 10px;
        border: 2px solid grey; border-radius: 5px; font-size: 12px;
    ">
    <b>Organization</b><br>
    """
    for org, color in org_color_map.items():
        legend_html += f"""
        <i style="background:{color}; width:10px; height:10px;
            display:inline-block; margin-right:5px;"></i>{org}<br>
        """
    legend_html += "</div>"
    m.get_root().html.add_child(folium.Element(legend_html))

    return m


# ---------------------------------------------------------------------------
# Rail helpers
# ---------------------------------------------------------------------------

def get_rail_stops(
    nearby_stops: pd.DataFrame,
    rail_buffer_miles: float
) -> pd.DataFrame:
    """Filter nearby stops to rail stops within rail_buffer_miles."""
    return nearby_stops[
        (nearby_stops["mode_type"] == "Rail") &
        (nearby_stops["distance_miles"] <= rail_buffer_miles)
    ].copy()


def display_rail_table(
    rail_stops: pd.DataFrame,
    rail_buffer_miles: float,
    venue_name: str
) -> None:
    """Display a styled summary table of rail stops by agency."""
    rail_summary = (
        rail_stops
        .groupby("organization_name")
        .agg(
            rail_stop_count=("stop_id", "count"),
            nearest_station=(
                "stop_name",
                lambda x: (
                    rail_stops.loc[x.index, ["stop_name", "distance_miles"]]
                    .sort_values("distance_miles")
                    .iloc[0]["stop_name"]
                )
            ),
            approx_distance_to_stadium=("distance_miles", "min")
        )
        .reset_index()
    )

    rail_summary["approx_distance_to_stadium"] = (
        rail_summary["approx_distance_to_stadium"]
        .round(1)
        .astype(str) + " mi"
    )

    rail_summary = (
        rail_summary
        .rename(columns={
            "organization_name": "Transit Agency",
            "rail_stop_count": "Rail Station Count",
            "nearest_station": "Nearest Station(s)",
            "approx_distance_to_stadium": "Approx. Distance to Stadium"
        })
        .sort_values("Rail Station Count", ascending=False)
        .reset_index(drop=True)
    )

    display(
        rail_summary.style
        .set_caption(
            f"Number of Rail Stations Within {rail_buffer_miles} Miles of {venue_name}"
        )
        .hide(axis="index")
        .background_gradient(subset=["Rail Station Count"], cmap="Blues")
        .set_table_styles(TABLE_STYLES)
    )


def display_rail_map(
    rail_stops: pd.DataFrame,
    venue_lat: float,
    venue_lon: float,
    rail_buffer_miles: float,
    venue_name: str
) -> folium.Map:
    """Build and return a Folium map of rail stops around the venue."""
    organizations = sorted(rail_stops["organization_name"].dropna().unique())
    org_color_map = {
        org: COLORS[i % len(COLORS)]
        for i, org in enumerate(organizations)
    }

    m = folium.Map(location=[venue_lat, venue_lon], zoom_start=11.3)

    # Title
    m.get_root().html.add_child(folium.Element(f"""
        <h3 align="center" style="font-size:20px">
        <b>Rail Stations Within {rail_buffer_miles} Miles of {venue_name}</b>
        </h3>
    """))

    # Venue marker
    folium.Marker(
        [venue_lat, venue_lon],
        tooltip=venue_name,
        icon=folium.Icon(color="red")
    ).add_to(m)

    # Buffer circle
    folium.Circle(
        location=[venue_lat, venue_lon],
        radius=rail_buffer_miles * 1609.344,
        color="black", weight=2, fill=False,
        dash_array="8, 8",
        tooltip=f"{rail_buffer_miles}-mile rail buffer"
    ).add_to(m)

    # Stop markers with jitter to separate overlapping points
    for _, row in rail_stops.iterrows():
        org = row["organization_name"]
        lat_jitter = np.random.uniform(-0.003, 0.003)
        lon_jitter = np.random.uniform(-0.003, 0.003)

        folium.CircleMarker(
            location=[
                row["stop_lat"] + lat_jitter,
                row["stop_lon"] + lon_jitter
            ],
            radius=4,
            color=org_color_map.get(org, "#999999"),
            fill=True,
            fill_color=org_color_map.get(org, "#999999"),
            fill_opacity=0.8,
            tooltip=(
                f"{row['stop_name']}<br>"
                f"{org}<br>"
                f"{row['distance_miles']:.1f} mi"
            )
        ).add_to(m)

    # Scrollable legend
    legend_html = """
    <div style="
        position: fixed; bottom: 50px; left: 50px;
        width: 280px; max-height: 300px; overflow-y: scroll;
        background-color: white; border: 2px solid gray;
        padding: 10px; z-index: 9999; font-size: 12px;
    ">
    <b>Organizations</b><br>
    """
    for org, color in org_color_map.items():
        legend_html += f"""
        <div>
        <span style="display:inline-block; width:12px; height:12px;
            background:{color}; margin-right:6px;
            border:1px solid black;"></span>
        {org}
        </div>
        """
    legend_html += "</div>"
    m.get_root().html.add_child(folium.Element(legend_html))

    return m


# ---------------------------------------------------------------------------
# Top-level orchestration — called by the notebook
# ---------------------------------------------------------------------------

def run_analysis(
    venue_name: str,
    bus_buffer_miles: float = 3,
    rail_buffer_miles: float = 10
) -> None:
    """
    Full pipeline: geocode venue, filter stops, display tables and maps.
    This is the single entry point called from analysis.ipynb.
    """
    print(f"📍 Geocoding '{venue_name}'...")
    venue_lat, venue_lon, corrected_name = geocode_venue(venue_name)

    # Geocoding failed — message already printed inside geocode_venue
    if venue_lat is None:
        return

    print(f"   → {venue_lat:.4f}, {venue_lon:.4f}")

    print("📦 Loading stops data...")
    df = load_stops_data()

    print("🔍 Filtering nearby stops...")
    print("⏳ Please wait, generating maps and tables...")
    nearby_stops = filter_nearby_stops(
        df, venue_lat, venue_lon, max_miles=rail_buffer_miles
    )

    # --- Bus ---
    display(HTML(
        '<br><br><h2 style="text-align:center;">🚌 Bus Analysis</h2><hr/>'
    ))
    bus_stops = get_bus_stops(nearby_stops, bus_buffer_miles)
    if bus_stops.empty:
        print(f"   ℹ️  No bus stops found within {bus_buffer_miles} miles of '{corrected_name}'.")
    else:
        display_bus_table(bus_stops, bus_buffer_miles, corrected_name)
        display(HTML("<br>"))
        bus_map = display_bus_map(
            bus_stops, venue_lat, venue_lon, bus_buffer_miles, corrected_name
        )
        display(bus_map)

    # --- Rail ---
    display(HTML(
        '<br><br><h2 style="text-align:center;">🚆 Rail Analysis</h2><hr/>'
    ))
    rail_stops = get_rail_stops(nearby_stops, rail_buffer_miles)
    if rail_stops.empty:
        print(f"   ℹ️  No rail stations found within {rail_buffer_miles} miles of '{corrected_name}'.")
    else:
        display_rail_table(rail_stops, rail_buffer_miles, corrected_name)
        display(HTML("<br>"))
        rail_map = display_rail_map(
            rail_stops, venue_lat, venue_lon, rail_buffer_miles, corrected_name
        )
        display(rail_map)