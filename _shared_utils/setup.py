from setuptools import find_packages, setup

# https://stackoverflow.com/questions/52630814/how-to-include-and-access-data-files-in-python-distribution
setup(
    name="shared_utils",
    packages=find_packages(),
    version="2.5",
    description="Shared utility functions for data analyses",
    author="Cal-ITP",
    license="Apache",
    include_package_data=True,
    package_dir={"_shared_utils": "shared_utils"},
)
