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
   