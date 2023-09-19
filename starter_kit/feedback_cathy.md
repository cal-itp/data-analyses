# Feedback for Cathy
## Exercise 1
* Find the min/max weight by cylinder. Use a groupby to achieve this.
   * Modify this code you have and include weight:
     ```
     mtcars.groupby(['cyl']).agg({'mpg':'mean'}).reset_index()
     
     Adapt it like this:
     mtcars.groupby(['cyl']).agg(
        {'mpg': 'mean', 
         'weight': 'min'}
     ).reset_index()

     ```
    * A new column called `difference` would be created after the groupby/aggregation. The result df should have 3 rows, one for each cylinder group, and 4 columns (mean mph, min weight, max weight, difference in weight).
    
## Exercise 2
* Use `git rm CathyPractice1.ipynb` to remove unused practice exercise 1
* Do a fresh rerun of each exercise before submitting so that cells are run in the right order.
* Does this actually work? If df2 is `vehicles`, this merge should produce an error because it doesn't pass the validation.
  ```
  merge1 = pd.merge(df, df2, on = ['Agency','City', 'State', 'Legacy NTD ID', 'NTD ID'],
    how = 'inner', validate = 'm:1')
 ```
* String columns need to go through extra data cleaning before aggregation
   * `agg2=merge3.groupby(['State']).agg({'Total Operating Expenses':'sum'}).reset_index()` produces funky results because `Total Operating Expenses` column is still a string


## Exercise 3
* There are duplicates for LA Metro, `Mode = MB` because there is a column on which the values differ on. If `TOS` (type of service) was included, you would see one row is `DO = directly operated` and one is `PT = purchased transportation`. This means that bus service is provided by LA Metro and also they purchased it from a contracting bus company. 
* [NTD Glossary](https://www.transit.dot.gov/ntd/national-transit-database-ntd-glossary)
* Alternative to (this is correct, but be very careful doing the division): 
    ```
    cost=df1.groupby(['Agency']).agg({'Total_Operating_Expenses':'sum'}).reset_index()
    passenger=df1.groupby(['Agency']).agg({'Primary_UZA__Population':'sum'}).reset_index()
    
    # this keeps both summation columns in the same df
    agency_sum = df1.groupby(["Agency"]).agg(
        {"Total_Operating_Expenses": "sum",
        "Primary_UZA__Population": "sum"
        }.reset_index()
    )
    ```
* Watch out for what this is doing: `cost.Total_Operating_Expenses/passenger.Primary_UZA__Population`. A safer way is to merge the 2 dfs together, and use `Agency` as your merge variable. 
   * This simply takes each element in the column and divides...first element in cost divided by first element in passsenger. If `cost` and `passenger` have different lengths, then the division can be weird. To guarantee that the cost of LA Metro is divided by the passenger of LA Metro, a merge will make sure each row lines up correctly.
   * Where do the `inf` values come from? What is driving that? The numerator or denominator?
   * Where is `cost_per_passenger` saved in the df? This column should feed into the chart later. If you are grouping by `Agency` or `Mode`, then the bar chart will only show 1 bar for each value in the grouping column.
* Did you do the mode mapping to include `bus`, `rail`, and `other`? Where is the other category? Use a lambda function to do this and categorize the remaining values as `other`.
* You didn't use the bar chart function!
   * `df4.plot(x='Agency', y='Cost_per_Passenger', kind='bar')` makes a plot, but you didn't use the `make_bar_chart()`. It should be `chart = make_bar_chart(df4, x="Agency" or "Mode", y="Cost_per_Passenger")`
   * Make sure you use an aggregated df for the chart, otherwise your chart will have a lot of bars for each agency. 

## Exercise 4
* In this merge, are you expecting `COUNTY_NAME` to appear multiple times? If it does, show a cell where `COUNTY_NAME` has duplicates. If there are no duplicates, use `validate = '1:1'` 
    ```
    final_stops = pd.merge(counties1, stops_per_county, 
                on = 'COUNTY_NAME',
                how = 'inner', validate = 'm:1')
    ```
* Clean up formatting of Markdown cells, make use of various levels of heading. Use the [Markdown reference](https://www.datacamp.com/tutorial/markdown-in-jupyter-notebook) from exercise 3 to improve the formatting.