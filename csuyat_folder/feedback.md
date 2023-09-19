# Feedback for Christian

## Exercise 1
* Use `git mv` to change the path of where your current exercises in progress and future exercises live. Use `csuyat_folder` and not `csuyat_folder/python`.    
   * `YOURNAME_exercise1` can be interpreted as `christian_exercise1`...not literally as `CHRISTIAN_exercise1`
* Functions within scripts are imported if they are in the same directory. It is brittle to have too many nested folders for simpler projects.
   * Most folders within `data-analyses` follow this, except for larger projects like `rt_segment_speeds` and `dla`
   * For more complex projects, nested directories are used, but only in conjunction with installable packages.
* When submitting a finished exercise, use `Kernel > Restart Kernel and Run ALl Cells` to show a fresh run of code with outputs.

## Exercise 2
* Left join keeps all rows that show up in the left df, whether or not the right df has it.
   * If the right df has it, those columns will be populated. 
   * If the right df does not have those rows, those columns will be filled with NaNs / missing values. NaNs = not a number.
   * In this exercise, if there were 100 agencies in the left df, and 90 agencies in the right df:
      * A left merge would leave you with 100 agencies in your merged df. Those 10 agencies not present in the right df would have their `vehicles` columns populated with NaNs.
      * An inner merge would leave you with 90 agencies in your merged df. These are the 90 agencies are found in the left and the right dfs.
   * There are use cases for having a left merge or an inner merge. Sometimes you want agencies with complete information. Other times you want all the agencies to show up in your results and you want to present how many agencies have missing info.
* Modify this cell to another column that isn't categorical. 
    ```
    merge2.groupby('State').TOS.agg(['count', 'nunique', 'min']).head()    
    ```
   * Use `df.dtypes` to check what data types are. 
   * `TOS` = type of service, and it has 2 unique values, DO and PT (directly operated). `min(TOS) = DO` isn't that interpretable, since the min of a string is just the first one that appears in the alphabet.
* Add a line of code to rename columns where the new line character is cleaned up. Ex: `Population\n` becomes `Population` without the new line character.
* Do the challenge portion and provide not only aggregate stats for `service_vehicles`, but also `per capita service vehicles`. Plot these 2 charts side-by-side.

## Exercise 3
* The rows do differ outside of the `subset_cols` you've defined. For `mode = MB (bus)`, the 2 rows left for LA Metro are probably for `TOS = DO or PT` (directly operated or contracted out purchased transportation). 
* [NTD Glossary](https://www.transit.dot.gov/ntd/national-transit-database-ntd-glossary)
* When deciding whether to aggregate or deal with duplicates by dropping, it depends on the research question.
   * If you wanted to focus on directly operated bus service, it's possible to filter down to the point where you no longer have duplicates.
   * If you wanted to compare agencies, aggregation is usually the way to go. 
* Fix the dictionary for mapping `Mode` values. 
   * Check whether the `and` worked by comparing `df.mode_cat.value_counts()` with `df.mode_cat.value_counts(dropna=False)`
   * If there are unmapped modes, write out the dictionary in long form:
      ```
      mode_fill = {
          "HR": "Rail",
          "SR": "Rail",
          "AR": "Rail",
          "LR": "Light Rail"
      }
      ```
* Looping: `for c in some_list:` is how loops start. `c` is the variable that is injected into the later lines.
   * By convention, it's usually something related to what that variable means. `c` here is column. You can also use `for col in df.columns:` for something readable.
   * Within each loop, `c` is replaced by the variable.
   * When the loop goes through the first time, `c = Agency_VOMS`. The second time, `c = Mode_VOMS`
      * `df.Agency_VOMS = df.Agency_VOMS.str.replace(',', '').fillna('0').astype({"Agency_VOMS": int})`
      * `df.Mode_VOMS = df.Mode_VOMS.str.replace(',', '').fillna('0').astype({"Mode_VOMS": int})`
* Alternatively, for weighted averages, you can simply take the `sum(operating_expenses)` and `sum(vehicle_miles)` by state.
   * Then, your df with 5 rows (5 states), you can add a new column calculating state-level operating expenses per mile by dividing the 2 summation columns.
   * `df["operating_cost_per_mi"] = df.total_operating_expenses.divide(df.vehicle_miles)` or `df.total_operating_expenses / df.total_vehicle_miles`
* To show `altair` chart, first use the function to make the chart: `chart = make_bar_chart()`, followed by `chart` to print it, not `chart.show()`

## Exercise 4
* Challenge question: why does `stops_2229 = stops_ptg.assign(geometry=stops_ptg.geometry.to_crs('EPSG:2229'))` show results of area = 0 for all the rows?
   * Do points have area?
   * Do lines have area? 
   * Area can only be calculated for polygons. Lines and points do not have area, so if you try to calculate it, it will always return 0.
   * Lines have lengths, and so do polygons (circumference)! Points do not have length, so if you try to calculate length on a point, you'll also return 0.
* Diving into the swapping which df to put on the left, county or stops.
   * What is the active geometry column name and what does it reflect? Is it the left or right gdf's geometry?
   * It matters which column you're interested in attaching attributes to / wanting to aggregate. If you want to count how many stops are in a county, you first want to attach the county for each stop.
   * When you keep stops on the left, it's because you want to keep stop (point) geometry for plotting, and each dot on a map should be colored according to the county name. Most of the time you want points on the left.
   * If you want to keep county on the left and plot county boundaries, you can keep county (polygon) geometry on the left.
   * Most of the time, for point-in-polygon questions, like, which polygon does this point fall into, you want the point gdf on the left.
* Do the sq ft calculation on the county gdf, not the stops. Keep only 1 row for each county, and add the column for `county_sq_ft`, then another column converting `county_sq_ft` to `county_sq_mi`. 
   * When you have your results for the county, merge it onto your groupby results here: `stop_count = geojoin_stp_cnty.groupby('COUNTY_NAME')['stop_id'].count().reset_index()
)`. `stop_count` will then contain the number of stops as well as county polygon geometry, sq_ft, sq_mi, and a new column with stops_per_sq_mi.