"""
Take the analysis data,
created in `create_analysis_data.py`,
wrangle it to fit visualizations.

Generate any additional metrics.
"""
import geopandas as gpd
import intake
import pandas as pd

catalog = intake.open_catalog("./catalog.yml")

# Stop times by tract
def generate_stop_times_tract_data():
    df = catalog.bus_stop_times_by_tract.read()
    
    df = df.assign(
        num_arrivals = df.num_arrivals.fillna(0),
        num_jobs = df.num_jobs.fillna(0),
        stop_id = df.stop_id.fillna(0),
        itp_id = df.itp_id.fillna(0),
        num_pop_jobs = df.num_pop_jobs.fillna(0),
        popdensity_group = pd.qcut(df.pop_sq_mi, q=3, labels=False) + 1,
        jobdensity_group = pd.qcut(df.jobs_sq_mi, q=3, labels=False) + 1,
        popjobdensity_group = pd.qcut(df.popjobs_sq_mi, q=3, labels=False) + 1,
    )

    # These columns may result in NaNs becuase pop or jobs can be zero as denom
    # Let's keep it and allow arrivals_groups to be 0 (instead of 1-3)
    # Should only be a problem if pop OR jobs is zero or if pop AND jobs is zero.
    df = df.assign(
        arrivals_per_1k_p = (df.num_arrivals / df.Population) * 1_000,
        arrivals_per_1k_j = (df.num_arrivals / df.num_jobs) * 1_000,
        arrivals_per_1k_pj = (df.num_arrivals / df.num_pop_jobs) * 1_000,
    )

    df = df.assign(
        arrivals_group_p = pd.qcut(df.arrivals_per_1k_p, q=3, labels=False) + 1,
        arrivals_group_j =  pd.qcut(df.arrivals_per_1k_j, q=3, labels=False) + 1,
        arrivals_group_pj = pd.qcut(df.arrivals_per_1k_pj, q=3, labels=False) + 1,
    )
    
    round_me = [
        'pop_sq_mi', 'jobs_sq_mi', 'popjobs_sq_mi', 
        'overall_ptile', 'pollution_ptile', 'popchar_ptile',
        'arrivals_per_1k_p', 'arrivals_per_1k_j', 'arrivals_per_1k_pj',
    ]
    
    integrify_me = [
        'equity_group', 'num_jobs', 'num_arrivals', 
        'stop_id', 'itp_id',
        'arrivals_group_p', 'arrivals_group_j', 'arrivals_group_pj',
    ]
    
    df[round_me] = df[round_me].round(2)
    df[integrify_me] = df[integrify_me].fillna(0).astype(int)

    return df