from setuptools import find_packages, setup

setup(
    name='shared_utils',
    packages=find_packages(),
    version='0.1.0',
    description='Shared utility functions for data analyses',
    author='Cal-ITP',
    license='Apache',
    include_package_data=True,
    package_dir={"_shared_utils": "shared_utils"},
    install_requires=[
        "calitp", "geopandas", "numpy", "pandas", 
        "altair", "matplotlib", "plotnine", "plotly", "seaborn", # charts
        "folium", "ipyleaflet", # maps
        "branca",  # colors
        "ipywidgets", "altair_saver", "vega", # supporting
        # To also include in Docker
        "pygeos", # pygeos has some compatibility issue with geopandas...one of them needs to be hardcoded and set to a version
        "rtree", "fiona", "shapely", "openpyxl", # spatial
        "python-dotenv", # env
    ],
)