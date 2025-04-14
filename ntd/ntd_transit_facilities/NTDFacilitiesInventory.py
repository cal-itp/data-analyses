import typing
import geopandas as gpd
import pandas as pd
from constants import *
import categories
from calitp_data_analysis.utils import read_geojson, geojson_gcs_export
import folium
from collections import defaultdict
import pathlib
from urllib.parse import urlparse
import dotenv
import os

class NTDFacilitiesInventory:
    
    def __init__(
        self,
        url: str = NTD_FACILITIES_INVENTORY_URL,
        output_file_uri: str | None = None, 
        address_filter: dict[typing.Literal[*NTD_ADDRESS_FIELDS], str | typing.Iterable[str]] | None = None, # typing.Iterable or something else?
        geospatial_filter: gpd.GeoDataFrame = None,
        attempt_geocoding: bool = True
    ):
        """
        A wrapper for downloading and geocoding information from the National Transit Database facilities inventory
        
        params:
        url: A url or GCS uri where the 2023 Facility Inventory can be found
        output_file_uri: A uri (GCS or local path) where the facilities inventory should be saved
        address_filter: A filter in the format {ntd_column: name} to filter addresses by. Only values that need to be geocoded will be filtered
        geospatial_filter: A GDF that will mask the output. Applies to both geocoded and non geo
        attempt_geocoding: True if geocoding should be attempted, false otherwise
        """
        parsed_url = urlparse(url)
        file_type = pathlib.Path(parsed_url.path).suffix          
        # Load the base ntd file
        if file_type == ".xlsx":
            # Load an xlsx file formatted identically to the 2023 ntd facilities inventory
            df = pd.read_excel(url)
            df["address_only"] = False
            df.loc[
                (df[LONGITUDE].isna() | df[LATITUDE].isna()) & ~(df[[*NTD_ADDRESS_FIELDS]].isna().all(axis=1)), 
                "address_only"
            ] = True
            slice_with_address = df.loc[~df["address_only"]]
            gdf_provided = gpd.GeoDataFrame(
                slice_with_address,
                geometry=gpd.points_from_xy(slice_with_address[LONGITUDE], slice_with_address[LATITUDE], crs=NTD_CRS)
            )
            gdf_provided["Geometry Geocoded"] = False    
        else:
            # Load a GeoDataFrame. Ignore any Latitude or Longitude columns
            if check_is_gs_uri(url) and file_type == ".geojson":
                path, name = split_gs_uri(url)
                gdf = read_geojson(path, name)
            else:
                gdf = gpd.read_file(url)
            gdf["address_only"] = False
            gdf.loc[gdf.geometry.isna(), "address_only"] = True
            gdf_provided = gdf.loc[~gdf["address_only"]].copy()
            df = gdf.drop(gdf.geometry.name, axis=1)
        
        # Geocoding
        if df["address_only"].any() and attempt_geocoding:
            # Get only rows that must be geocoded
            df_address_only = df.loc[df["address_only"]].copy()
            # Get a formatted address
            df_address_only[ZIP_CODE] = df_address_only[ZIP_CODE].astype(int)
            df_address_only[[*NTD_ADDRESS_FIELDS]] = df_address_only[[*NTD_ADDRESS_FIELDS]].fillna("").astype(str)
            for filter_key in address_filter:
                df_address_only = df_address_only.loc[df_address_only[filter_key] == address_filter[filter_key]]
            df_address_only = df_address_only.copy()
            df_address_only["address"] = df_address_only[STREET_ADDRESS] + ", " + df_address_only[CITY] + ", " + df_address_only[STATE] + ", " + df_address_only[ZIP_CODE]
            # Run the geocoder
            geocode_result = geocode_series(
                df_address_only["address"], 
                address_output_name="Geocode Result Address",
                geometry_output_name=gdf_provided.geometry.name
            )
            # Merge the result
            gdf_geocoded = gpd.GeoDataFrame(
                pd.concat([df_address_only, geocode_result], axis=1),
                geometry=gdf_provided.geometry.name
            )
            gdf_geocoded["Geometry Geocoded"] = True
            gdf_combined = pd.concat([gdf_provided, gdf_geocoded], axis=0)
        else:
            gdf_combined = gdf_provided
        
        # Apply the geospatial filter
        if geospatial_filter is not None:
            self._gdf = gdf_combined.clip(geospatial_filter)
        else:
            self._gdf = gdf_combined
        
        # Save the output result
        if output_file_uri is not None:
            if output_file_uri.lower()[:5] == "gs://":
                path, name = split_gs_uri(output_file_uri)
                geojson_gcs_export(
                    self._gdf, path, name # f-string is a necessary cludge to get geojson_gcs_export to save the path properly
                )
            else:
                self._gdf.to_file(output_file_uri)
    
    @property
    def gdf(self):
        """A cleaned GDF containing the values from the original facility inventory and a geometry column based on geocoding"""
        return self._gdf.copy()
    
    def folium_geojson(self, use_categories=True, needed_fields = DEFAULT_TOOLTIP_FIELDS, **args):
        """
        Gets a Folium Geojson object that can be passed to a Folium map. 
        By default, contains a popup with the agency name, facility name and facility type, 
        and is colored based on the facility type.
        Any args passed will override that.
        
        Params:
        use_categories: Whether to categorize entries based on categories.py. Useful for color coding
        needed_fields: the fields that are needed for the display. 
            Must be specified if attempt_geocoding was False and the source did not already have geocoding applied
            Defaults to the values needed for the default args
        **args: Any other args to be passed to Folium. If none specified, a tooltip with the values in DEFAULT_TOOLTIP_FIELDS, a circle marker, and color coding will be applied
        
        Returns:
        A Folium GeoJSON instance that can be added to a map with the .add_to method
        """
        # Get the GDF and the needed fields (note the deep copies)
        gdf = self.gdf 
        needed_fields_altered = list(needed_fields)
        # Add categories
        if use_categories:
            needed_fields_altered.append("Category")
            # Get map from original values to categories
            field_to_category = {}
            for category in categories.TYPES_BY_CATEGORY:
                for facility_type in categories.TYPES_BY_CATEGORY[category]:
                    field_to_category[facility_type] = categories.STRING_VALUE_BY_CATEGORY[category]
            # Add categories to the gdf
            gdf["Category"] = gdf[FACILITY_TYPE].map(
                defaultdict(lambda: categories.STRING_VALUE_BY_CATEGORY[categories.DEFAULT_CATEGORY], field_to_category)
            )
        # Filter for values that are needed, this is to reduce the memory needed to display the map
        gdf_to_plot = gdf[[*needed_fields_altered, gdf.geometry.name]].dropna(subset=[gdf.geometry.name]).fillna("")
        args_with_default = dict(args)
        # Add default args 
        # Tooltip
        if "tooltip" not in args_with_default and "popup" not in args_with_default:
            args_with_default["tooltip"] = folium.GeoJsonTooltip(fields=needed_fields_altered)
        # Circle Marker
        if "marker" not in args_with_default:
            args_with_default["marker"] = folium.CircleMarker(color="#162933", fill_color="#ffb81c", fill=True, fill_opacity=0.8, radius=10, weight=0.5)
        # Color Coding
        if use_categories and "style_function" not in args_with_default:
            string_value_to_color = {
                categories.STRING_VALUE_BY_CATEGORY[category]: categories.COLORS_BY_CATEGORY[category]
                for category in categories.COLORS_BY_CATEGORY.keys()
            }
            args_with_default["style_function"] = lambda feature: {
                "fillColor": string_value_to_color[feature["properties"]["Category"]]
            }
        # Return a Folium GeoJson that can be added to a map
        return folium.GeoJson(data=gdf_to_plot, **args_with_default)
    
def check_is_gs_uri(uri_or_path):
    """Check whether a uri is a gs:// url"""
    return uri_or_path[:5] == "gs://"
    
def split_gs_uri(uri):
    """Split a gs:// url into its parent and name using Pathlib"""
    assert check_is_gs_uri(uri), "URI is not a gs:// url"
    output_file_path = pathlib.PosixPath(uri[5:])
    return f"{output_file_path.parent}/", output_file_path.name
    
def geocode_series(
        addresses: pd.Series | list,
        address_output_name: str,
        geometry_output_name: str = "geometry", 
        geocode_engine: str = GOOGLE_MAPS_ENGINE_NAME,
        api_key_environment_variable: str = GOOGLE_MAPS_API_KEY_ENV, 
        dotenv_path: str | None = DOTENV_PATH,
        warn=True,
    ):
    """
    Geocode a series using Geopandas and Geopy. Returns a GDF with the matched address and geometry By default, uses Google Maps.
    
    Params:
    addresses: A list or Series of addresses. If a Series is used, the index will be preservedf
    address_output_name: The name of the address column in the output gdf
    geometry_output_name: The name of the geometry column in the output gdf, defaults to "geometry"
    geocode_engine: The engine to use for geocoding, defaults to the Google Maps Geocoding API
    api_key_environment_variable: The environment variable to get the api key from, defaults to "GOOGLE_MAPS_API_KEY"
    dotenv_path: The path to the file containing environment variables, defaults to "_env"
    warn: Whether the user should be warned how many queries will be made to the geocoding api, defaults to True
    
    Returns:
    A GeoDataFrame with the returned addresses and geeometries from the geocoder 
    """
    # Get the api key
    if dotenv is not None:
        dotenv.load_dotenv(dotenv_path)
    google_maps_key = os.environ.get(api_key_environment_variable)
    assert google_maps_key is not None, f'The configured Google Maps API Key is not set. To set it, append "{api_key_environment_variable}=\<your_api_key>\" to {pathlib.Path(dotenv_path).resolve()} file.'
    
    # Warn the user of the number of addresses if requested
    if warn:
        geocode_ok = input(f"OK to geocode {addresses.count()} addresses? (Y/n)")
        if geocode_ok.upper() != "Y":
            raise RuntimeError("Geocoding rejected")
            
    # Run the geocoder
    geocode_result = gpd.tools.geocode(
        addresses, provider=GOOGLE_MAPS_ENGINE_NAME, api_key=google_maps_key
    )
    
    # Return results
    results_renamed = geocode_result.rename(
        columns={GEOPANDAS_ADDRESS_NAME: address_output_name}
    )
    if geometry_output_name != results_renamed.geometry.name:
        results_renamed = results_renamed.rename_geometry(geometry_output_name)
    return results_renamed
    