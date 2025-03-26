"""
Return a source of truth with organization names that correspond with one another.
There are instances in which  schedule_gtfs_dataset_keys
have more than one organization_name that corresponds to it.
We only want to publish a page for a schedule_gtfs_dataset_key once,
so we need a crosswalk to show something like City of Simi Valley is published
under City of Camarillo.
"""
import pandas as pd
import yaml
import _operators_prep 

def df_to_yaml(df:pd.DataFrame, SITE_YML:str):
    """
    Dump Pandas Dataframe to a YAML.

    Parameters:
    df (pd.DataFrame): DataFrame with 'sched_rt_category' and 'organization_name' columns.

    Returns:
    yaml_str (str): YAML string representation of the input DataFrame.
    """
    # Initialize an empty dictionary to store the result
    result = {}

    # Iterate over unique 'sched_rt_category' values
    for category in df['sched_rt_category'].unique():
        # Filter the DataFrame for the current category
        category_df = df[df['sched_rt_category'] == category]

        # Create a list of 'organization_name' values for the current category
        organization_names = category_df['organization_name'].tolist()

        # Add the category and organization names to the result dictionary
        result[category] = organization_names

    # Save to YML
    with open(SITE_YML) as f:
        site_yaml_dict = yaml.load(f, yaml.Loader)
        
    output = yaml.dump(result)
    
    with open(SITE_YML, "w") as f:
        f.write(output)


if __name__ == "__main__":
    # Load the dataframe that holds all the operators
    all_categories, one_to_one_df, final = _operators_prep.operators_schd_vp_rt()

    # Subset
    df = all_categories[["sched_rt_category","organization_name",]]
    
    # Save to YAML
    SITE_YML = "schd_vp_cats.yml"
    df_to_yaml(df, SITE_YML)