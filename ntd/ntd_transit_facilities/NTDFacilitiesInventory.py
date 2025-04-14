import typing
import geopandas as gpd
import pandas as pd
from constants import *
import categories
from geocoding_utils import Geocoder
from calitp_data_analysis.utils import read_geojson, geojson_gcs_export
import folium
from collections import defaultdict
import pathlib
from urllib.parse import urlparse

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
        gcs_cache_uri: A GCS uri where the facilities inventory should be saved. Should not be specified if url is a GCS uri
        """
        parsed_url = urlparse(url)
        file_type = pathlib.Path(parsed_url.path).suffix            
        if file_type == ".xlsx":
            print(url)
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
        else:
            if check_is_gs_uri(url) and file_type == ".geojson":
                path, name = split_gs_uri(url)
                gdf = read_geojson(path, name)
            else:
                gdf = gpd.read_file(url)
            if "address_only" not in gdf.columns:
                gdf["address_only"] = False
                gdf.loc[gdf.geometry.isna(), "address_only"] = True
            gdf_provided = gdf.loc[~gdf["address_only"]].copy()
            df = gdf.drop(gdf.geometry.name, axis=1)
        gdf_provided["geometry_geocoded"] = False
        
        if df["address_only"].any() and attempt_geocoding:
            geocoder_instance = Geocoder()
            df_address_only = df.loc[df["address_only"]].copy()
            df_address_only[ZIP_CODE] = df_address_only[ZIP_CODE].astype(int)
            df_address_only[[*NTD_ADDRESS_FIELDS]] = df_address_only[[*NTD_ADDRESS_FIELDS]].fillna("").astype(str)
            for filter_key in address_filter:
                df_address_only = df_address_only.loc[df_address_only[filter_key] == address_filter[filter_key]]
            df_address_only = df_address_only.copy()
            df_address_only["address"] = df_address_only[STREET_ADDRESS] + ", " + df_address_only[CITY] + ", " + df_address_only[STATE] + ", " + df_address_only[ZIP_CODE]
            
            geocode_ok = input(f"OK to geocode {df_address_only['address'].count()} addresses? (Y/n)")
            if geocode_ok.upper() != "Y":
                raise RuntimeError("Geocoding rejected")
            gdf_geocoded = geocoder_instance.geocode_df(df_address_only.head(200), "address").to_crs(NTD_CRS)
            gdf_geocoded["geometry_geocoded"] = True
            gdf_combined = pd.concat([gdf_provided, gdf_geocoded], axis=0)
        else:
            gdf_combined = gdf_provided
            
        if geospatial_filter is not None:
            print(geospatial_filter)
            self._gdf = gdf_combined.clip(geospatial_filter)
        else:
            self._gdf = gdf_combined
    
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
        """
        gdf = self.gdf
        needed_fields_altered = list(needed_fields)
        if use_categories:
            needed_fields_altered.append("Category")
            field_to_category = {}
            for category in categories.TYPES_BY_CATEGORY:
                for facility_type in categories.TYPES_BY_CATEGORY[category]:
                    field_to_category[facility_type] = categories.STRING_VALUE_BY_CATEGORY[category]
            gdf["Category"] = gdf[FACILITY_TYPE].map(
                defaultdict(lambda: categories.STRING_VALUE_BY_CATEGORY[categories.DEFAULT_CATEGORY], field_to_category)
            )
        print(gdf["Category"].head())
        gdf_to_plot = gdf[[*needed_fields_altered, gdf.geometry.name]].dropna(subset=[gdf.geometry.name]).fillna("")
        args_with_default = dict(args)
        if "tooltip" not in args_with_default:
            args_with_default["tooltip"] = folium.GeoJsonTooltip(fields=needed_fields_altered)
        if "marker" not in args_with_default:
            args_with_default["marker"] = folium.CircleMarker(radius=6, fillColor="#ffb81c", color="#4b4f54", fillOpacity=0.8, weight=0.75)
        if use_categories and "style_function" not in args_with_default:
            string_value_to_color = {
                categories.STRING_VALUE_BY_CATEGORY[category]: categories.COLORS_BY_CATEGORY[category]
                for category in categories.COLORS_BY_CATEGORY.keys()
            }
            args_with_default["style_function"] = lambda feature: {
                "fillColor": string_value_to_color[feature["properties"]["Category"]]
            }
        return folium.GeoJson(data=gdf_to_plot, **args_with_default)
    
def check_is_gs_uri(uri_or_path):
    return uri_or_path[:5] == "gs://"
    
def split_gs_uri(uri):
    assert check_is_gs_uri(uri), "URI is not a gs:// uri"
    output_file_path = pathlib.PosixPath(uri[5:])
    #assert output_file_path.suffix == ".geojson", "Only .geojson export is currently supported for GCS files. Any Geopandas-supported file type may be used for local exports."
    return f"{output_file_path.parent}/", output_file_path.name
    
    
    
    
        