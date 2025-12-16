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

## Contributing
### Testing

Some code is this directory has test coverage and there's an intention to expand coverage as needed, particularly when changes are made to existing code.
These tests are an important step towards providing some regression proofing for our shared code. GitHub Actions will fail the build and alert us if changes
are made to code that has test coverage where assertions were not satisfied. At that point, you will need to either fix the code or, if new behavior is desired,
update the tests to match.

Many of these tests exercise code that requests data from BigQuery datasets. In order to make tests run quickly and not drive up costs, these tests use [pytest-recording](https://pypi.org/project/pytest-recording/)
to make recordings (cassettes) of the actual BigQuery requests and their responses. This allows us to reuse existing recordings when we run tests instead of
repeatedly having to make a network request. The tests are setup to provide a specific response when a matching request is made during the test run. If a request
is made that does not match the pre-recorded cassette, the test will fail. If you need to change some of the SQLAlchemy code and those changes result in a difference
in the outputted SQL, the corresponding test will fail due to it no longer matching the cassette. This is expected and you will need to record a new cassette.

If you need to make a new request recording or update an existing one, follow these steps

1. Set up the test data you need in BigQuery under our `cal-itp-data-infra-staging` project within the `test_shared_utils` dataset. To do this, you can
   * Think of this step as "assuming my DB table has this specific data in it, I expect to get this specific data in response to my query".
   2. Review the data that's already in `cal-itp-data-infra-staging.test_shared_utils` to see what you can use. You may find you can use what's there and simply add a row or modify an existing row, or you may be better off clearing the table and starting from scratch. The data in these `test_shared_utils` is not guaranteed to stick around for any length of time and is only useful for capturing cassette recordings.
   2. Review the data in the `cal-itp-data-infra-staging` datasets (like `mart_transit_database` or `mart_gtfs`) and tables to see what data is typical.
   2. Copy a sample of data that you need for your test cases from the relevant tables into a `test_shared_utils` table. For instance using `CREATE TABLE` or `INSERT INTO` statements
      ```bigquery
      CREATE TABLE cal-itp-data-infra-staging.test_shared_utils.fct_daily_schedule_feeds AS (
         SELECT * FROM cal-itp-data-infra-staging.mart_gtfs.fct_daily_schedule_feeds
         WHERE gtfs_dataset_name = "Metrolink Schedule"
         LIMIT 3
      )
      ```
1. Make a recording of the request and its response from BigQuery with `pytest-recording`
   2. Add the `@pytest.mark.vcr` annotation to the test that will call code that will make a BigQuery request.
   2. From the command line, run `pytest --record-mode=once tests/path_to_test_file.py -k "name_of_your_specific_test_function"`
   2. This will create a `*.yaml` file in the `tests/cassettes` recording the data from the request. You will want to add this file to your subsequent `git commit`.
   2. If you need to make changes, you may need to delete the cassette and run the step above again to get a fresh recording.
   2. After you make a recording that captures the setup your test needs you can run tests without the `record-mode` option to use the cassette without making a new request.
