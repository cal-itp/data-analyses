"""
Create the yamls for parameterized reports.

Since these yamls do not use sections, we can
generate them similarly using Makefile commands.

Try out typer to make CLI a little easier to use, since 
this only takes 1 argument with 2 possible values.
Base it off of this tutorial:
https://typer.tiangolo.com/tutorial/options/required/
"""
import pandas as pd
import typer

from pathlib import Path

from shared_utils import portfolio_utils
from rt_msa_utils import PREDICTIONS_GCS, RT_MSA_DICT

RT_TRIP_UPDATES_STOP_YAML = Path("../portfolio/sites/rt_trip_updates_stop_metrics.yml")
RT_TRIP_UPDATES_ROUTE_YAML = Path("../portfolio/sites/rt_trip_updates_route_metrics.yml")

app = typer.Typer()

excluded_operators = [
    "Bay Area 511 Regional Schedule"
]

@app.command()
def overwrite_yaml(
    name: str = typer.Argument(default="rt_msa")
):
    """
    Create yamls for portfolio.
    """
    if name == "rt_msa_stops":
        FILE = RT_MSA_DICT.rt_schedule_models.weekday_stop_grain
    
        df = pd.read_parquet(
            f"{PREDICTIONS_GCS}{FILE}.parquet",
            columns = ["schedule_name"],
            filters = [[ ("schedule_name", "not in", excluded_operators)]]
        ).drop_duplicates().dropna(
            subset="schedule_name" 
            # there shouldn't be occurrences of this, but there are, so check why
        ).rename(columns = {"schedule_name": "name"})
        
        portfolio_utils.create_portfolio_yaml_chapters_no_sections(
          RT_TRIP_UPDATES_STOP_YAML, 
          chapter_name = "name",
          chapter_values = sorted(list(df.name))
        )  
        
    elif name == "rt_msa_trips":
        FILE = RT_MSA_DICT.rt_trip_updates_models.weekday_route_direction_grain
    
        df = pd.read_parquet(
            f"{PREDICTIONS_GCS}{FILE}.parquet",
            columns = ["name"],
            filters = [[("name", "notin", excluded_operators)]]
        ).drop_duplicates()

        portfolio_utils.create_portfolio_yaml_chapters_no_sections(
          RT_TRIP_UPDATES_ROUTE_YAML, 
          chapter_name = "name",
          chapter_values = sorted(list(df.name))
        )  
      
    return


if __name__ == "__main__":
    app()
