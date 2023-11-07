# Feedback

## Exercise 1
* Do a fresh rerun of the notebook when you're finished with the exercise
* `if/elif/else`.If you wanted a catch-all, you can use `else`.
   * `if` means every condition is checked.
   * `elif` means it only checks it if the previous `if` and `elif` conditions are false.
   * `else` is the catch-all category, if no other condition is met, then do this.
   ```
   def tag_cyl(row):
    if row.cyl == 6:
        return 'six'
    elif row.cyl == 4:
        return 'four'
    elif row.cyl == 8:
        return 'eight'
    else:
        return 'other'
   ```
   
## Exercise 2
* When merging, if there are commons in common from the left and right dfs, `pandas` will automatically add a suffix like `_x, _y`. You can adjust the merge from 
    ```
    # old merge
    merge1 = pd.merge(
        ntd_metrics_select,
        ntd_vehicles_select,
        on = 'ntd_id',
        how = 'left',
        validate = 'm:1'
    )

    # new merge - keep adding columns in the on= until all the x's and y's disappear
    merge1 = pd.merge(
        ntd_metrics_select,
        ntd_vehicles_select,
        on = ["ntd_id", "agency", "state","legacy_ntd_id", 
             "primary_uza\n_population"],
        how = "left"
    )
    ```
* Printing multiple outputs in a cell, by default, only the last ones will display. Writing this way will allow everything before the last ones to also display.
   ```
   print(len(df))
   display(df.head())
   
   print(len(df2))
   display(df2.head())
   ```
* Nullable integers: `pandas` allows for a column to be integers with NaNs. df = `df.astype({"this_col": "Int64"})`. Normally, if the column can be made an integer type, and there are no NaNs, you can use `df.astype({"this_col": "int64"})`. Capitalized `Int64` vs lowercase `int64`. You can view your df's data types with `df.dtypes`.

## Exercise 3
* Writing out the steps longhand is great for seeing patterns in your code. Once you start noticing something that's being repated, that's a good candidate for using a function.
* For example, this last cell making the chart, most of the code is repeated except for the y-column.
```
fares_per_passengerchart = alt.Chart(df3).encode(alt.X('Agency'),
                                                alt.Y('fares_per_passenger'), alt.Color('Mode')
                                               ).mark_bar()
fares_revenuechart = alt.Chart(df3).encode(alt.X('Agency'),
                                                alt.Y('Fare_Revenues_Earned'),alt.Color('Mode')
                                               ).mark_bar()
```

The next step is to convert this code into a function.


```
# Altair allows you to specify if a column is categorical or numeric or datetime
# It is specified like this: alt.X("Agency:O") or alt.X("Agency:N")
# N = nominal = unordered categorical
# O = ordinal = ordered categorical
# Q = quantitative = numeric
# T = datetime (can parse for year/month/hour/quarter, etc)
def base_chart(df):
   chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x = alt.X("Agency:N"),
                 color = alt.Color("Mode:N") 
             )
      )
      
    return chart
    
# this base chart can be used, with some additional customization for the y-column.

fares_per_passengerchart = base_chart(df3).encode(
    y = alt.Y('fares_per_passenger:Q', title = "Fare Revenues per Passenger")
)

fare_revenues_chart = base_chart(df3).encode(
    y = alt.Y('Fare_Revenues_Earned:Q', title = "Fare Revenues")
)

```

## Exercise 4
* Your output is now ready for a map. Try this:
```
# Merge back county geometry
gdf = pd.merge(
    CA_county[["COUNTY_NAME", "geometry"]],
    county_area,
    on = "COUNTY_NAME",
    how = "inner"
)


# Create a chloropleth map, one plotting absolute count of stops
# and one plotting counts per sq mi
gdf.explore("stop_id", tiles = "CartoDB Positron", cmap = "viridis")

gdf.explore("stop_sqmi", tiles = "CartoDB Positron", cmap = "viridis")
```