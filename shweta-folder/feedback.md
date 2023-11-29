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

## Exercise 5
* Go one step further in the functions, and see if you can use dictionaries, f-strings to populate more of the chart programmatically. The example below shows how you might prep the df a bit more, and then use an extra function to clean up words for displaying in a chart. 

```
df = df.rename(
   columns = {"stop_event_count": "total_stop_events",
              "COUNTY_NAME": "county"})



def make_chart(df, x_col, y_col, colorscale): 
    
    # defining a function inside of another means you can't access it 
    # outside of this function. 
    # if you define title_case outside of make_chart, you can use it in
    # other functions too.
    
    def title_case(word: str):
       return word.replace('_', '').title()
    
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(x_col, title=title_case(x_col)),
                 y=alt.Y(y_col, title=title_case(y_col)),
                 color = alt.Color(y_col,
                                   scale = alt.Scale(range=colorscale),
                                  ),
             ).properties(title=f"{title_case(y_col)} by County")
            )
    chart = styleguide.preset_chart_config(chart)
    display(chart)
```
* Take a look [in this notebook](https://github.com/cal-itp/data-analyses/blob/main/bus_service_increase/competitive-routes.ipynb) to see how you could also weave in HTML and Markdown with `display(HTML())` and `display(Markdown())` to programmatically generate captions. This is the Jupyter notebook equivalent of creating RMarkdown docs the way [Urban Institute creates their fact sheets](https://urban-institute.medium.com/iterated-fact-sheets-with-r-markdown-d685eb4eafce).

## Exercise 6
* Re-using the same function to make multiple charts. If it's the same kind of chart (bar chart), then you can probably set up the function differently to take different arguments. This takes a little practice.
    * This function, `make_chart`, takes Operators_Name as an argument, but I don't see it used anywhere inside the function. I see `"Operators_Name"`, which is a string of that exact phrase, but not a variable (no quotation marks).
    ```
    def make_chart(Operators_Name, colorscale): 
        chart = (alt.Chart(operators_county)
                 .mark_bar()
                 .encode(
                     x=alt.X("county_name", title="County"),
                     y=alt.Y("Operators_Name", title="Number of Operators"),
                     color = alt.Color("Operators_Name",
                                       scale = alt.Scale(range=colorscale),
                                      ),
                     tooltip = ["county_name", "Operators_Name"]
                 ).properties(title="Operators by County")
                 .interactive()
                )
        chart = styleguide.preset_chart_config(chart)
        display(chart)

    make_chart(operators_district, cp.CALITP_CATEGORY_BRIGHT_COLORS)
    ```
    * Instead, I would set up a `make_chart` function that takes a df and an x-column (district or county). Since you're plotting the count of operators for the chart, the y-column is shared. Look at all the places where `df`, `x_col` is used. Also, note how I specified `chart_title` within the function and used that variable later.
    ```
    def make_chart(df: pd.DataFrame, x_col: str):
        # Let's create chart_title as a variable, and we will use it later
        # we want 2 different titles depending on the kind of chart
        
        if x_col == "county_name":
            chart_title = "Operators by County"
        elif x_col == "Caltrans_District":
            chart_title = "Operators by District"
    
        chart = (alt.Chart(df)
                 .mark_bar()
                 .encode(
                     x=alt.X(x_col, title = f"{x_col}.replace('_', ' ').title()"),
                     y=alt.Y("Operators_Name", title="Number of Operators"),
                     color = alt.Color(
                         "Operators_Name",
                         scale = alt.Scale(range=cp.CALITP_CATEGORY_BRIGHT_COLORS),
                     ),
                     tooltip = [x_col "Operators_Name"]
                 ).properties(title = chart_title)
                 .interactive()
                )
                
        return chart
    
    district_chart = make_chart(operators_district, "Caltrans_District")
    
    county_chart = make_chart(operators_county, "county_name")
    ```
* Add a couple more things to the `altair` chart
    ```
    # adding tooltip (hover over to display the list of columns)
    # and interactive (can scroll with mouse to zoom)
    
    chart = (alt.Chart(operators_county)
             .mark_bar()
             .encode(
                 x=alt.X("county_name", title="County"),
                 y=alt.Y("Operators_Name", title="Number of Operators"),
                 color = alt.Color("Operators_Name",
                                   scale = alt.Scale(range=colorscale),
                                  ),
                 tooltip = ["county_name", "Operators_Name"]
             ).properties(title="Operators by County")
             .interactive()
            )
    ```
* You can make a grouped bar chart in `altair` with your `operators_caltransdistrict` df.
    ```
    # faceting is one way to get a grouped bar chart
    # https://github.com/altair-viz/altair/issues/1221
    # but the complicated part is that faceting is a more complex chart,
    # so will have to use `apply_chart_config(chart)` for a pared down 
    # chart formatting instead of 
    # `preset_chart_config(chart)`, which will error. you'll have to add 
    # additional things like sizing yourself at the end.
    
    chart = (alt.Chart(operators_caltransdistrict)
             .mark_bar()
             .encode(
                 x=alt.X("county_name:O", title=""),
                 y=alt.Y("Operators_Name:Q", title="Number of Operators"),
                 color = alt.Color("county_name:O", 
                                   scale = alt.Scale(
                                       range = cp.CALITP_CATEGORY_BOLD_COLORS)),
                 tooltip = ["county_name", "Operators_Name"]
             ).facet(
                 column = alt.Column('Caltrans_District', title = "District")
             ).properties(title="Operators by District and County")
             .resolve_scale(x="independent")
             .interactive()
            )
            
    chart = styleguide.apply_chart_config(chart)

    display(chart)
    ```

## Exercise 8
* Whenever you do spatial join, overlay, etc, you might get a new column called `index_right`, which I like to drop. We tend not to use the index anyway, and it's holding the index from your right df.
    ```
    railstops_ca = gpd.sjoin(rail_stops.to_crs("EPSG:2229"),
        ca.to_crs("EPSG:2229"),
        how = "inner",
        predicate = "intersects"
    ).drop(columns = "index_right")
    ```
* When you dissolve, you don't need to keep `Shape__Length` or `Shape__Area` from ESRI. Those units are usually not discernible (if it's in WGS 84, it's decimal degrees, and we won't use it anyway). Instead, generate your own `length` or `area`, because you would have projected the CRS and you would be clear whether the units are meters, feet, etc.
* I prefer this line: `overlay_percentage = overlay_dissolve.assign(percent = (overlay_dissolve.geom_length / overlay_dissolve.rail_length)*100)`....which basically gets your percent in one line, and you no longer need the step to create the `half_rail_length` (more roundabout).
   * Since you're just assigning a new column, you can keep doing stuff on `overlay_dissolve`.
   ```
   overlay_dissolve = overlay_dissolve.assign(
       percent = blah blah
   )
   
    # this column name is the same name as the function...avoid because it's sharing the same name
   overlay_dissolve['railroutescheck'] = overlay_dissolve.apply(railroutescheck, axis=1)

    # 
   alternate way to write your apply function is:
   overlay_dissolve = overlay_dissolve.assign(
       rail_route_category = overlay_dissolve.apply(
           lambda x: 'less than half' if (x.percent > 0 and x.percent < 50)
           else 'more than half' if x.percent >= 50
           else 'never intersects SHN', 
           axis=1)
   )
   ```

## Exercise 9