from setuptools import find_packages, setup

setup(
    name="rt_analysis",
    packages=find_packages(),
    version="0.1.0",
    description="Module for GTFS-RT transit speed analysis",
    author="Cal-ITP",
    license="Apache",
    include_package_data=True,
    py_modules = ['rt_parser', 'rt_filter_map_plot'],
    package_dir={"rt_delay": "rt_analysis"}
)