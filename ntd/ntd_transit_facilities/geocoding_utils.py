import googlemaps
import os
import dotenv
import shapely
import typing
import numpy as np
import geopandas as gpd

class GeocodePoint(typing.NamedTuple):
    point: shapely.Point
    partial_match: bool
    location_type: typing.Literal["ROOFTOP", "RANGE_INTERPOLATED", "GEOMETRIC_CENTER", "APPROXIMATE"]

class GeocodePointResponse(typing.NamedTuple):
    result_type: typing.Literal["FOUND", "NO_RESPONSE", "AMBIGUOUS_RESPONSE"]
    geocode_point: GeocodePoint | None

class Geocoder:
    def __init__(self, api_key_environment_variable="GOOGLE_MAPS_API_KEY", dotenv_path="_env"):
        if dotenv:
            dotenv.load_dotenv("_env")
        google_maps_key = os.environ.get(api_key_environment_variable)
        assert google_maps_key is not None, f'The configured Google Maps API Key is not set. To set it, append "{api_key_environment_variable}=\<your_api_key>\" to the ntd_transit_facilities/env directory.'
        self.maps_client = googlemaps.Client(key=google_maps_key)
        
    def geocode_df(
            self, 
            df, 
            input_address_row_name, 
            output_geocode_success_name = "geocode_success",
            output_partial_match_name = "partial_match",
            output_location_type_name = "location_type"
        ):
        assert input_address_row_name in df.columns, f"{input_address_row_name} must be the name of a column in the DataFrame"
        assert output_geocode_success_name not in df.columns, f"{output_geocode_success_name} must not be a name of a column in the DataFrame"
        assert output_partial_match_name not in df.columns, f"{output_partial_match_name} must not be the name of a column in the DataFrame"
        assert output_location_type_name not in df.columns, f"{output_location_type_name} must not be the name of a column in the DataFrame"
        
        response_series = df[input_address_row_name].map(lambda address: self.geocode_to_point(address=address))
        
        df_copy = df.copy()
        df_copy[output_geocode_success_name] = response_series.map(lambda response: response.result_type)
        df_copy[output_partial_match_name] = response_series.map(
            lambda response: np.nan if response.result_type != "FOUND" else response.geocode_point.partial_match
        )
        df_copy[output_location_type_name] = response_series.map(
            lambda response: np.nan if response.result_type != "FOUND" else response.geocode_point.location_type
        )
        geometries = gpd.GeoSeries(
            response_series.map(lambda response: np.nan if response.result_type != "FOUND" else response.geocode_point.point),
            crs=4326
        )
        gdf = gpd.GeoDataFrame(
            df_copy,
            geometry=geometries
        )
        
        return gdf
        
    def geocode_to_point(self, address=None, components=None):
        response = self._geocode(address=address, components=components)
        
        if len(response) == 0:
            return GeocodePointResponse(result_type="NO_RESPONSE", geocode_point=None)
        if len(response) == 1:
            content = response[0]
            return GeocodePointResponse(
                result_type="FOUND",
                geocode_point=self._process_point_geocode(content)
            )
        else:
            return GeocodePointResponse(result_type="AMBIGUOUS_RESPONSE", geocode_point=None)

    @staticmethod    
    def _process_point_geocode(content):
        latitude = content["geometry"]["location"]["lat"]
        longitude = content["geometry"]["location"]["lng"]
        location_type = content["geometry"]["location_type"]
        partial_match = False if "partial_match" not in content else content["partial_match"]
        return GeocodePoint(
            point=shapely.Point([longitude, latitude]),
            location_type=location_type,
            partial_match=partial_match
        )
    
    def _geocode(self, address=None, components=None, **extra_args):
        dotenv.load_dotenv("_env")
        assert address is not None or components is not None, "Either an address string or components must be specified"
        args = {}
        if address is not None:
            args["address"] = address
        if components is not None:
            args["components"] = components
        geocode_response = self.maps_client.geocode(**args, **extra_args)
        return geocode_response