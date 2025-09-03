## Retrospective feed generation tool
0. Authenticate to the Caltrans DDS Warehouse. Instructions will vary based on your environment.
1. Run the scripts in `/conveyal_update` to download a GTFS-Schedule feed
2. From this directory, run `poetry install` to create a venv with dependencies.
3. Create a csv file with paths to existing feeds, using `test_files.csv` as an example.
4. Run feed generation with `poetry run python script_retrospective_feed_generation.py <your_input_here>.csv`
5. Obtain the output from the output path provided

### Testing
This project contains automated tests using pytest. From this directory, run `poetry run pytest` to run tests.