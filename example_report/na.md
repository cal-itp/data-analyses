
## **Functions and Code that may be Helpful**

### Ipywidgets Tabs

Create tabs using Ipywidgets to switch between information for each of the project categories

Code help: https://stackoverflow.com/questions/50842160/how-to-display-matplotlib-plots-in-a-jupyter-tab-widget

As seen in [this parameterized report notebook](https://github.com/cal-itp/data-analyses/blob/main/dla/e76_obligated_funds/charting_function_work.ipynb):
```python
    out1 = widgets.Output()
    out2 = widgets.Output()
    out3 = widgets.Output()
    out4 = widgets.Output()
    out5 = widgets.Output()
    out6 = widgets.Output()
    out7 = widgets.Output()

    # children2 = [widgets.Text(description=name) for name in work_cat]
    # tab2 = widgets.Tab(children2)
    tab2 = widgets.Tab(children = [out1, out2, out3, out4, out5, out6, out7])

    tab2.set_title(0, "Active Transportation")
    tab2.set_title(1, "Transit")
    tab2.set_title(2, "Bridge")
    tab2.set_title(3, "Street")
    tab2.set_title(4, "Freeway")
    tab2.set_title(5, "Infrastructure & Emergency Relief")
    tab2.set_title(6, "Congestion Relief")

    with out1:
        _dla_utils.project_cat(df, 'active_transp') 
    with out2:
        _dla_utils.project_cat(df, 'transit')
    with out3: 
        _dla_utils.project_cat(df, 'bridge')
    with out4:
        _dla_utils.project_cat(df, 'street')
    with out5:
        _dla_utils.project_cat(df, 'freeway')
    with out6:
        _dla_utils.project_cat(df, 'infra_resiliency_er')
    with out7:
        _dla_utils.project_cat(df, 'congestion_relief')

    display(tab2)

```
### Table Styling 

reformatting the df table output allowing you to style and format in a variety of ways: https://pandas.pydata.org/pandas-docs/stable/user_guide/style.html



### Dates

#### Shift Cells 
Compare differences between rows: https://towardsdatascience.com/all-the-pandas-shift-you-should-know-for-data-analysis-791c1692b5e
* Use shift to calculate the number of days between obligations
```python
df['n_days_between'] = (df['prepared_date'] - df.shift(1)['prepared_date']).dt.days
```

#### Fiscal Year from Date
Calculate fiscal years from date: https://stackoverflow.com/questions/59181855/get-the-financial-year-from-a-date-in-a-pandas-dataframe-and-add-as-new-column
```python
df['financial_year'] = df['date'].map(lambda x: x.year if x.month > 3 else x.year-1)
```


### Formatting Numbers/Currency:
Reformat the output of numbers, currency and percentages: https://github.com/d3/d3-format
* using the d3 format, I reformatted the dollar amount from scientific notation to $M or $K 
ex: from my [notebook](https://github.com/cal-itp/data-analyses/blob/30de5cd2fed3a37e2c9cfb661929252ad76f6514/dla/e76_obligated_funds/_dla_utils.py#L221)
```python 
x=alt.X("Funding Amount", axis=alt.Axis(format="$.2s", title="Obligated Funding ($2021)"))            
```

### CPI Adjustment Function:
function to adjust values in money column. Converting to $2021.
```python
def adjust_prices(df):
    
    cols =  ["total_requested",
           "fed_requested",
           "ac_requested"]
    
    # Inflation table
    import cpi 
    
    def inflation_table(base_year):
        cpi.update()
        series_df = cpi.series.get(area="U.S. city average").to_dataframe()
        inflation_df = (series_df[series_df.year >= 2008]
                        .pivot_table(index='year', values='value', aggfunc='mean')
                        .reset_index()
                       )
        denominator = inflation_df.value.loc[inflation_df.year==base_year].iloc[0]

        inflation_df = inflation_df.assign(
        inflation = inflation_df.value.divide(denominator)
        )
    
        return inflation_df
    
    ##get cpi table 
    cpi = inflation_table(2021)
    cpi.update
    cpi = (cpi>>select(_.year, _.value))
    cpi_dict = dict(zip(cpi['year'], cpi['value']))
    
    
    for col in cols:
        multiplier = df["prepared_y"].map(cpi_dict)  
    
        ##using 270.97 for 2021 dollars
        df[f"adjusted_{col}"] = ((df[col] * 270.97) / multiplier)
    return df
```


### Add Tooltip:
Add tooltip to chart functions

```python
def add_tooltip(chart, tooltip1, tooltip2):
    chart = (
        chart.encode(tooltip= [tooltip1,tooltip2]))
    return chart
 ```
    







