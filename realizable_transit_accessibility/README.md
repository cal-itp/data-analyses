## Retrospective feed generation tool
0. Use a Python environment as configured [here](https://github.com/cal-itp/data-infra/tree/main/images/jupyter-singleuser)
1. Run the scripts in `/conveyal_update` to download a GTFS-Schedule feed
2. From this directory, run `pip install -r requirements.txt`.
3. Update the constants in the second cell of `retrospective_feed_generation.ipynb`
4. Run all cells in that notebook
5. Download the output from the path provided

### Testing
This project contains automated tests using pytest. From this directory, run `pytest` to run tests.