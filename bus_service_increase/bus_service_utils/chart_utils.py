"""
Common chart / viz functions.
"""
import pandas as pd

def labeling(word: str, labeling_dict: dict) -> str:
    """
    Supply a labeling dictionary where 
    keys are existing column names and 
    values are what's to be displayed on visualization.
    """
    if word in labeling_dict.keys():
        word = labeling_dict[word]
    else: 
        word = word.replace('_', ' ').title()
    return word


def set_legend_order(label_dict: dict) -> list:
    """
    Returns the custom sort order for altair legend.
    Put the values in the dictionary in the order you want.
    """
    legend_order = list(label_dict.values())

    return legend_order 