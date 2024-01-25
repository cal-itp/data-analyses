# Code Review 
[Google Drive notes](https://docs.google.com/document/d/12SLubjQoE8NLLOm8Nb5DYKzwdNS1WYOkvrK_HnR1-FY/) 

### README for methodology is good
* The methodology reads like a narrative, code can be the same!
* Use functions to break your code up into discrete steps.
```
def clean_funding(df: pd.DataFrame):
    # dollar sign stuff
    # maybe rounding
    # clean up missing values
    return

def clean_other_columns(df):
    return


def data_cleaning_tircp():
    df = pd.read_csv()
    df1 = clean_funding(df)
    df2 = clean_other_columns(df1)
    
    return df2
```


### Use `pandas` over `numpy` for now
* Generate a lot of your columns at once
* You can do group stats more easily with `pandas`
* With `numpy`, it's a little harder to include groupings in the stats you want
```
df2 = df.assign(
    # grouping over a column or list of columns
    # use transform() to get what you want
    group_mean = (df.groupby("shape_array_key")
            .service_hours
            .transform("mean")
           ),
    group_std = (df.groupby("shape_array_key")
           .service_hours
           .transform("std")
          )
)

df2 = df2.assign(
    z_score = (df2.service_hours - df2.group_mean) / df2.group_std
)
```


### Use a function to make a chart
* find what's in common -> can include those within the function
* what's not in common -> set as variable
```
def make_bar_chart(concat, y_col, chart_title):
    axis_labeling_dict = {
        "bus_count": "# of buses"
        "project_sponsor": "Transit Agencies"
    }
    
    #bar chart of highest bus count
    concat = (concat.sort_values(by=y_col, ascending=False)
     .head(10)
     .plot(x='project_sponsor', 
       y=y_col, kind='bar', color='skyblue')
    )
    plt.title(chart_title)
    plt.xlabel(axis_labeling_dict["project_sponsor"])
    plt.ylabel(axis_labeling_dict[y_col])
    # return plt?
    
    
c1 = make_bar_chart(concat, y_col = "bus_funds", chart_title = "First Title")
c2 = make_bar_chart(concat, y_col = "bus_count", chart_title = "Second Title")
```