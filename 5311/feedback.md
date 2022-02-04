# Feedback

## `data_prep.py`

#### [Improve function readability](https://github.com/cal-itp/data-analyses/blob/_5311/5311/data_prep.py#L65-L82)

Nothing techncially wrong with referencing `vehicles` as the df, and code should execute correctly. But it's confusing because it over-complicates.

The function as-is: 
* Define a function that applies to entire df.
* Define a function that applies to each row.
* Apply function to each row of df and create a new column.
* Now apply this entire function on the original df that executes a function on each row and creates a new column.
* Too many layers!

Improvement:
* Just apply the function that applies to each row on the df.
* Reference: [args, kwargs explanation](https://able.bio/rhett/python-functions-and-best-practices--78aclaa)


#### [Better column naming](https://github.com/cal-itp/data-analyses/blob/_5311/5311/data_prep.py#L91-L94)

* Opt for words, not characters...so `>=` can be `greaterthan` or `gt` or some variation. 
* When you need to reference the column later, you can use `df.gt1_gtfs` instead of `df[">=1_gtfs"]`, which gives it a cleaner look.

#### Practice "don't repeat yourself" or "write everything twice"....DRY/WET

Practice this in general, though not always strict adherence. Do so when work in notebooks is ready to be moved into scripts.

[DRY / WET explanation](https://dev.to/wuz/stop-trying-to-be-so-dry-instead-write-everything-twice-wet-5g33). 

The GCS file path is something repeated in a handful of times in the script. Can the file path be defined up and the top? What if someone changed the file path? 

Ex: remove the white space in `5311 ` to be `5311`...do you have to fix it once or fix it a handful of times? 

In notebooks, you should reference this file path using a variable `data_prep.GCS_FILE_PATH` or `GCS_FILE_PATH` instead of typing out the full path.


#### Clean up notebooks

Once stuff from notebooks is moved into scripts, clean up the exploratory notebooks. Keep some to track thought processes when debugging the data. No more duplicate notebooks showing the same thing.

Use naming scheme to separate tasks for collaboration. A1, A2, B1, B2, etc.

