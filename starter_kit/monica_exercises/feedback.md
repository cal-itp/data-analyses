# Feedback for Monica

## Exercise 1
* Do a fresh run of the notebook when you're ready to submit. `Kernel > Restart Kernel and Run All` 
* Calculating the difference in min and max weight is done after the groupby.
   * If it's done across the entire df, then the difference is simply whatever max value you see subtracted by whatever min value you see.
   * Compare `avg_mpg_by_cyl = mtcars.groupby('cyl').agg('mpg').mean()` with `avg_mpg_by_cyl = mtcars.groupby('cyl').agg('mpg').mean().reset_index()` and display the outputs.
   * A difference after the groupby could be done using `avg_mpg_by_cyl` if you include a couple more columns in the groupby.
      ```
      avg_mph_by_cyl = (
              mtcars.groupby("cyl")
              .agg(
                  {"mpg": "mean",
                   "wt": "max",
                   "wt": "min"}
                  ).reset_index()
              )
      
    # Compare what the above looks vs after running this line
    avg_mpg_by_cyl = avg_mpg_by_cyl.droplevel(0, axis=1)
    
    avg_mpg_by_cyl["wt_difference"] = avg_mpg_by_cyl["max"] - avg_mpg_by_cyl["min"]
    ```

## Exercise 2
* Add a Markdown cell. When you open a new cell, navigate to the top bar, and where it says `Code`, change the dropdown to say `Markdown`. That changes the cell to be Markdown, which takes text, rather than Python code.
* Reference [this](https://www.datacamp.com/tutorial/markdown-in-jupyter-notebook), linked in exercise 2, to see how to add headers and various formatting to a Markdown cell.

