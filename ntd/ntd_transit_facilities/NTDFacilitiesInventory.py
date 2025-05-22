import typing
import geopandas as gpd
import pandas as pd
from constants import *
import categories
from calitp_data_analysis.utils import read_geojson, geojson_gcs_export, make_zipped_shapefile
import folium
from collections import defaultdict
import pathlib
from urllib.parse import urlparse
import dotenv
import os
import gcsfs
import requests

def load_ntd(
        url: str = ORIGINAL_NTD_FACILITIES_INVENTORY_URI,
        output_file_uri: str | None = None, 
        address_filter: dict[typing.Literal[*NTD_ADDRESS_FIELDS], str | typing.Iterable[str]] | None = None, # typing.Iterable or something else?
        geospatial_filter: gpd.GeoDataFrame = None,
        attempt_geocoding: bool = True
) -> gpd.GeoDataFrame:
    """
    A function for downloading and geocoding information from the National Transit Database facilities inventory

    Params:
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
        print("start geocoding")
        geocode_result = geocode_series(
            df_address_only["address"], 
            address_output_name="Geocode Result Address",
            geometry_output_name=gdf_provided.geometry.name
        )
        print("geocoding done")
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
        gdf_inventory_output = gdf_combined.clip(geospatial_filter)
    else:
        gdf_inventory_output = gdf_combined
    
    return gdf_inventory_output
    
def facilities_inventory_gdf_to_folium(
        processed_ntd_gdf: gpd.GeoDataFrame,
        use_categories: bool = True,
        needed_fields: list[typing.Hashable] = DEFAULT_TOOLTIP_FIELDS, # REVIEW NOTE - Not sure if this should be a list[str] or a list[typing.Hashable], since I'm not sure about the type for df column names
        **args
) -> folium.GeoJson:
        """
        Gets a Folium Geojson object that can be passed to a Folium map. 
        By default, contains a popup with the agency name, facility name and facility type, 
        and is colored based on the facility type.
        Any args passed will override that.
        
        Params:
        processed_ntd_gdf: A GeoDataFrame as returned by load_ntd
        use_categories: Whether to categorize entries based on categories.py. Useful for color coding
        needed_fields: the fields that are needed for the display. 
            Must be specified if attempt_geocoding was False and the source did not already have geocoding applied
            Defaults to the values needed for the default args
        **args: Any other args to be passed to Folium. If none specified, a tooltip with the values in DEFAULT_TOOLTIP_FIELDS, a circle marker, and color coding will be applied
        
        Returns:
        A Folium GeoJSON instance that can be added to a map with the .add_to method
        """
        # Get the GDF and the needed fields (note the deep copies)
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
            processed_ntd_gdf["Category"] = processed_ntd_gdf[FACILITY_TYPE].map(
                defaultdict(lambda: categories.STRING_VALUE_BY_CATEGORY[categories.DEFAULT_CATEGORY], field_to_category)
            )
        # Filter for values that are needed, this is to reduce the memory needed to display the map
        gdf_to_plot = processed_ntd_gdf[
            [*needed_fields_altered, processed_ntd_gdf.geometry.name]
        ].dropna(
            subset=[processed_ntd_gdf.geometry.name]
        ).fillna("")
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
    
def facilities_inventory_gdf_to_geojson(processed_ntd_gdf: gpd.GeoDataFrame, output_uri: str) -> None:
    """
    Save the facilities inventory gdf to a geojson file
    """
    # Save the output result
    if output_uri is not None:
        if output_uri.lower()[:5] == "gs://":
            path, name = split_gs_uri(output_uri)
            geojson_gcs_export(
                processed_ntd_gdf, path, name
            )
        else:
            processed_ntd_gdf.to_file(output_uri)

def facilities_inventory_gdf_to_shapefile(
        processed_ntd_gdf: gpd.GeoDataFrame,
        output_uri: str,
        keep_fields: list[typing.Hashable] = DEFAULT_TOOLTIP_FIELDS, column_rename_mapper = SHAPEFILE_MAP, temp_folder = ".temp") -> None:
        """
        Save the gdf as a shapefile to the specified path, either locally or in GCS (as a zip).
        
        Params:
        processed_ntd_gdf: A GeoDataFrame as returned by load_ntd
        output_uri: A string containing a uri, either a local path or a GCS url
        keep_fields: The complete fields to keep
        column_rename_mapper: A dict or function to pass to pd.rename to rename column names before they are truncated to 10 characters or fewer
        temp_folder: A temp folder to store the zipped shapefile in if output_uri is a GCS url
        """
        # Format the GeoDataFrame so it can be saved as a shapefile
        gdf_shortened_columns = processed_ntd_gdf[[*keep_fields, processed_ntd_gdf.geometry.name]]
        if column_rename_mapper is not None:
            gdf_shortened_columns = gdf_shortened_columns.rename(
                columns=column_rename_mapper
            )
        gdf_shortened_columns = gdf_shortened_columns.rename(
            columns=(lambda s: s if len(s) <= 10 else s[:10])
        )
        
        # Save shapefile
        if check_is_gs_uri(output_uri):
            # Get the name of the file to be saved
            _, name = split_gs_uri(output_uri)
            name_stem = pathlib.Path(name).stem
            name_as_zip = name_stem + ".zip"
            # Get the temp path where the file should be stored
            local_path = str((pathlib.Path(temp_folder) / (name_stem)).resolve())
            # Get a zipped shapefile locally (will save to the home directory)
            make_zipped_shapefile(gdf_shortened_columns, local_path, gcs_folder=None)
            # Upload the zipped shapefile to GCS
            fs = gcsfs.GCSFileSystem()
            fs.put(name_as_zip, output_uri)
            # Remove the temp zipped shapefile
            pathlib.Path(name_as_zip).unlink()
        else:
            # Save the file using the default gpd command
            gdf_shortened_columns.to_file(output_uri)
            
def check_is_gs_uri(uri_or_path):
    """Check whether a uri is a gs:// url"""
    return uri_or_path[:5] == "gs://"
    
def split_gs_uri(uri):
    """Split a gs:// uri into its parent and name using Pathlib"""
    assert check_is_gs_uri(uri), "URI is not a gs:// uri"
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
    A GeoDataFrame with the returned addresses and geometries from the geocoder 
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
    