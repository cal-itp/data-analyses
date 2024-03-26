import matplotlib.pyplot as plt
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
        'date': '2023-12-14',
    }, {
        'name': "Reference Saturday",
        'date': '2023-11-25',
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
    cm = confusion_matrix(y_true=df[y_true], y_pred=df[y_pred], labels=desired_order)
    df_cm = pd.DataFrame(cm, index=desired_order, columns=desired_order)
    # df_cm = df_cm.reindex(desired_order, axis=0)  # Rows
    # df_cm = df_cm.reindex(desired_order, axis=1)  # Columns
    # https://stackoverflow.com/questions/64800003/seaborn-confusion-matrix-heatmap-2-color-schemes-correct-diagonal-vs-wrong-re
    vmin = np.min(cm)
    vmax = np.max(cm)
    off_diag_mask = np.eye(*cm.shape, dtype=bool)

    plt.figure(figsize=(8, 6))
    sns.heatmap(df_cm, annot=True,  fmt='g', mask=~off_diag_mask, cmap="Blues", vmin=0, vmax=1, cbar=False, linewidths=0.8, linecolor='k')
    sns.heatmap(df_cm, annot=True,  fmt='g', mask=off_diag_mask, cmap="OrRd", vmin=0, vmax=1, cbar=False, linewidths=0.8, linecolor='k')
    plt.xlabel('Service Level on Website')
    plt.ylabel('GTFS Service Levels')
    plt.title(title)
    # plt.show()
    file = title
    plt.savefig(f"plots/{file}.png")