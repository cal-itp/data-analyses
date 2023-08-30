import pandas as pd
from babel.numbers import format_currency

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/project_prioritization/"

# Wordcloud
import matplotlib.pyplot as plt  # plot package
import seaborn as sns  # statist graph package
import wordcloud  # will use for the word cloud plot
from wordcloud import (  # optional to filter out the stopwords
    STOPWORDS,
    ImageColorGenerator,
    WordCloud,
)

# Strings
import re
from collections import Counter
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize

# Generate wordcloud
# From: https://www.kaggle.com/code/olgaberezovsky/word-cloud-using-python-pandas/notebook
def wordcloud(df, desc_column: str, max_words: int, additional_stop_words:list):
    
    wordstring = " ".join(df[desc_column].str.lower())
    
    plt.figure(figsize=(15, 15))
    wc = WordCloud(
    background_color="white",
    stopwords=STOPWORDS.update(additional_stop_words),
    max_words=50,
    max_font_size=200,
    width=650,
    height=650,)
    
    wc.generate(wordstring)
    # plt.imshow(wc.recolor(colormap="tab10", random_state=30), interpolation="bilinear")
    plt.axis("off")
    
    return plt.imshow(wc.recolor(colormap="tab10", random_state=30), interpolation="bilinear")

# Grab a list of all the string that appear in a column
# Natalie's function
def get_list_of_words(df, col: str, additional_words_to_remove: list):

    # get just the one col
    column = df[[col]]
    # remove single-dimensional entries from the shape of an array
    col_text = column.squeeze()
    # get list of words
    text_list = col_text.tolist()

    # Join all the column into one large text blob, lower text
    text_list = " ".join(text_list).lower()

    # remove punctuation
    text_list = re.sub(r"[^\w\s]", "", text_list)

    # List of stopwords
    swords = [re.sub(r"[^A-z\s]", "", sword) for sword in stopwords.words("english")]

    # Append additionally words to remove from results
    swords.extend(additional_words_to_remove)

    # Remove stopwords
    clean_text_list = [
        word for word in word_tokenize(text_list.lower()) if word not in swords
    ]

    return clean_text_list

"""
After using the function get_list_of_words() to return a cleaned 
list of text, find the most common phrases that pop up in 
the projects' descriptions.
"""
def common_phrases(df, clean_text_list: list, phrase_length: int):

    c = Counter(
        [" ".join(y) for x in [phrase_length] for y in ngrams(clean_text_list, x)]
    )
    df = pd.DataFrame({"phrases": list(c.keys()), "total": list(c.values())})
    
    # Sort by most common phrases to least
    df = df.sort_values("total", ascending=False)
    
    # Filter out any phrases with less than 2 occurences
    df = (df.loc[df["total"] > 1]).reset_index()
    
    return df
