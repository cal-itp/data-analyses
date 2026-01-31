## Retrospective feed generation tool
0. Authenticate to the Caltrans DDS Warehouse. Instructions will vary based on your environment.
1. From this directory, run `poetry install` to create a venv with dependencies.
    1. If you have previously run the script, you may need to remove your old venv as it will contain old versions of local packages. To do this, run `poetry env list` to get the name of your old venv, then `poetry env remove <env_name>`
2. Run feed generation with `poetry run python script_retrospective_feed_generation.py <target_date> <target_geography>`
    1. `target_date` must be a date present in `shared_utils.rt_dates`
    2. `target_geography` must be a geometry file within California
3. Obtain the outputs from `downloaded_schedule_feeds/` and `rt_generated_feeds/`

### Testing
This project contains automated tests using pytest. From this directory, run `poetry run pytest` to run tests.
