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