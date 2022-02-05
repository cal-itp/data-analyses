import pandas as pd

def grab_legend_thresholds(df, plot_col, arrivals_group):
    cut1 = df[(df[arrivals_group]==1) & (df[plot_col] > 0)][plot_col].min()
    cut2 = df[df[arrivals_group]==2][plot_col].min()
    cut3 = df[df[arrivals_group]==3][plot_col].min()

    MIN_VALUE = df[plot_col].min()
    MAX_VALUE = df[plot_col].max()
    
    colormap_cutoff = [cut1, cut2, cut3]
    
    return colormap_cutoff, MIN_VALUE, MAX_VALUE