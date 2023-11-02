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