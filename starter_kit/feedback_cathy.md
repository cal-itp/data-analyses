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