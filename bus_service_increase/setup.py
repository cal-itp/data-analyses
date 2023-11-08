from setuptools import find_packages, setup

setup(
    name="bus_service_utils",
    packages=find_packages(),
    version="2.1",
    description="Shared utility functions for bus service data analyses",
    author="Cal-ITP",
    license="Apache",
    include_package_data=True,
    package_dir={"bus_service_utils": "bus_service_utils"},
)