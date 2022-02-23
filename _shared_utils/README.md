# README

For analysis, there are probably a set of steps in data cleaning or visualization that analysts repeat. We encounter them both *within* a research question and *across* research questions. Why reinvent the wheel?

These shared utility functions are quality-of-life improvements for analysts as they iterate over their data cleaning and visualization steps. The utility functions would be importable across all directories in the `data-analyses` repo and can be called within a Jupyter notebook or Python script.

## Getting Started

1. In terminal, change directory into `_shared_utils`: `cd data-analyses/_shared_utils`
1. Run the make command to further do the `pip install` and `conda install`: `make setup_env`
1. Do work in your project-subfolder. Ex: `cd ../bus_service_increase`
1. Within Jupyter Notebook or script: `import shared_utils`


**References**
1. City of LA [covid19-indicators](https://github.com/CityOfLosAngeles/covid19-indicators/tree/master/processing_utils)
1. City of LA [planning-entitlements](https://github.com/CityOfLosAngeles/planning-entitlements/tree/master/laplan)
