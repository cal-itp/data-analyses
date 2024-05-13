import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.colors import ListedColormap

import numpy as np
import pandas as pd
import seaborn as sns  # For visualization (optional)
from sklearn.metrics import confusion_matrix


holiday_columns = ["Holiday Schedule – Thanksgiving Day",
"Holiday Schedule – Christmas Day",
"Holiday Schedule – New Year's Day",
"Holiday Schedule – MLK Day",
"Holiday Schedule – Veterans Day (Observed)",
"Holiday Schedule – Veterans Day",
"Holiday Schedule – Day after Thanksgiving Day",
"Holiday Schedule – Christmas Eve",
"Holiday Schedule – New Year’s Eve",
"Holiday Schedule Notes"]

# Define holidays
holidays_plus_ref = [
    {
        'name': "Veterans Day (Observed)",
        'website_name': "Holiday Schedule – Veterans Day (Observed)",
        'date': '2023-11-10',
    }, {
        'name': "Veterans Day",
        'website_name': "Holiday Schedule – Veterans Day",
        'date': '2023-11-11',
    }, {
        'name': "Thanksgiving Day",
        'website_name': "Holiday Schedule – Thanksgiving Day",
        'date': '2023-11-23',
    }, {
        'name': "Day After Thanksgiving",
        'website_name': "Holiday Schedule – Day after Thanksgiving Day",
        'date': '2023-11-24',
    }, {
        'name': "Christmas Eve",
        'website_name': "Holiday Schedule – Christmas Eve",
        'date': '2023-12-24',
    }, {
        'name': "Christmas Day",
        'website_name': "Holiday Schedule – Christmas Day",
        'date': '2023-12-25',
    }, {
        'name': "New Year's Eve",
        'website_name': "Holiday Schedule – New Year’s Eve",
        'date': '2023-12-31',
    }, {
        'name': "New Year's Day",
        'website_name': "Holiday Schedule – New Year's Day",
        'date': '2024-01-01',
    }, {
        'name': "MLK Day",
        'website_name': "Holiday Schedule – MLK Day",
        'date': '2024-01-15',
    }, {
        'name': "Reference Weekday",
        'date': '2023-12-15',
    }, {
        'name': "Reference Saturday",
        'date': '2023-12-16',
    }, {
        'name': "Reference Sunday",
        'date': '2023-12-17',
    },
]

text_data_cols = [
"Holiday Schedule – Thanksgiving Day",
"Holiday Schedule – Christmas Day",
"Holiday Schedule – New Year's Day",
"Holiday Schedule – MLK Day",
"Holiday Schedule – Veterans Day (Observed)",
"Holiday Schedule – Veterans Day",
"Holiday Schedule – Day after Thanksgiving Day",
"Holiday Schedule – Christmas Eve", 
"Holiday Schedule – New Year’s Eve",
"Veterans Day (Observed)",
"Veterans Day", 
"Thanksgiving Day", 
"Day After Thanksgiving",
"Christmas Eve", 
"Christmas Day", 
"New Year's Eve", 
"New Year's Day",
"MLK Day"]

def plot_confusion_matrices(df, y_true, y_pred, title): 
    desired_order = ['No service', 'Reduced service', 'Regular service']
    x_desired_order = ['No service', 'Reduced service', 'Regular service']
    y_desired_order = [ 'Regular service', 'Reduced service', 'No service']
    cm = confusion_matrix(y_true=df[y_true], y_pred=df[y_pred], labels=desired_order)
    df_cm = pd.DataFrame(cm, index=desired_order, columns=desired_order)
    df_cm = df_cm.reindex(y_desired_order, axis=0)  # Rows
    df_cm = df_cm.reindex(x_desired_order, axis=1)  # Columns
    df_cm = (df_cm/df_cm.sum().sum()).round(2)*100 # Make cm based on percentages

    # https://stackoverflow.com/questions/64800003/seaborn-confusion-matrix-heatmap-2-color-schemes-correct-diagonal-vs-wrong-re
    vmin = np.min(cm)
    vmax = np.max(cm)
    #It might have been easier to make this manually :P. Make a diagonal matrix from upper left to lower right, then flip it on tahe horizontal.
    gtfs_matches_website = np.fliplr(np.eye(*cm.shape, dtype=bool, k=0))
    gtfs_greater_website = ([
        [True, True,  False],
        [True, False, False],
        [False, False, False]])
    gtfs_less_website = ([
        [False, False,  False],
        [False,  False, True],
        [False, True, True]])

    gtfs_greater_website = pd.DataFrame(gtfs_greater_website).to_numpy()
    gtfs_less_website = pd.DataFrame(gtfs_less_website).to_numpy()
    plt.rcParams.update({'font.size': 13})

    # Used https://redketchup.io/color-picker to match colors in comparison graph to make this heatmap
    color = mcolors.to_rgb('#1F77B4')
    blue_cmap = mcolors.ListedColormap([color])
    color = mcolors.to_rgb('#FF7F0E')
    orange_cmap = mcolors.ListedColormap([color])
    color = mcolors.to_rgb('#2CA02C')
    light_green_cmap = mcolors.ListedColormap([color])

    plt.figure(figsize=(8, 6))
    font_size = {"fontsize":18}
    sns.heatmap(df_cm, annot=True,  annot_kws=font_size, fmt='g', mask=~gtfs_matches_website, cmap=blue_cmap, vmin=0, vmax=1, cbar=False, linewidths=0.8, linecolor='k')
    sns.heatmap(df_cm, annot=True,  annot_kws=font_size, fmt='g', mask=~gtfs_greater_website, cmap=light_green_cmap, vmin=.5, vmax=.6, cbar=False, linewidths=0.8, linecolor='k')
    sns.heatmap(df_cm, annot=True,  annot_kws=font_size, fmt='g', mask=~gtfs_less_website, cmap=orange_cmap, vmin=.04, vmax=.06, cbar=False, linewidths=0.8, linecolor='k')

    #fontsize=14.0
    plt.xlabel('Service Level on Website (% of agencies)', fontweight='bold')
    plt.ylabel('GTFS Service Levels (% of agencies)', fontweight='bold')
    plt.title(title, fontweight='bold')
    file = title
    plt.savefig(f"plots/{file}.png")
    # return cm, df_cm

excel_col_order = ['Name', 'Notes', 'gtfs_dataset_name',
'Total VOMS (NTD) (from Provider)', 'Customer Facing',"name",
"Reference Saturday",
"Reference Sunday",
"Reference Weekday",
"Holiday Schedule – Thanksgiving Day",
"Thanksgiving Day",
"score - Thanksgiving Day",
"score_text - Thanksgiving Day",
"Holiday Schedule – Christmas Day",
"Christmas Day",
"score - Christmas Day",
"score_text - Christmas Day",
"Holiday Schedule – New Year's Day",
"New Year's Day",
"score - New Year's Day",
"score_text - New Year's Day",
"Holiday Schedule – MLK Day",
"MLK Day",
"score - MLK Day",
"score_text - MLK Day",
"Holiday Schedule – Veterans Day (Observed)",
"Veterans Day (Observed)",
"score - Veterans Day (Observed)",
"score_text - Veterans Day (Observed)",
"Holiday Schedule – Veterans Day",
"Veterans Day",
"score - Veterans Day",
"score_text - Veterans Day",
"Holiday Schedule – Day after Thanksgiving Day",
"Day After Thanksgiving",
"score - Day After Thanksgiving",
"score_text - Day After Thanksgiving",
"Holiday Schedule – Christmas Eve",
"Christmas Eve",
"score - Christmas Eve",
"score_text - Christmas Eve",
"Holiday Schedule – New Year’s Eve",
"New Year's Eve",
"score - New Year's Eve",
"score_text - New Year's Eve",
"Holiday Schedule Notes"]
