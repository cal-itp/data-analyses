from setuptools import find_packages, setup

setup(
    name='shared_utils',
    packages=find_packages(),
    version='0.1.1',
    description='Shared utility functions for data analyses',
    author='Cal-ITP',
    license='Apache',
    include_package_data=True,
    package_dir={"_shared_utils": "shared_utils"},
)