# Refactor Notes
* This is soft skill I want you to hone: what is the simplest way you can explain the analysis, and then back that statement up with code. 
* There is a lot of "variables"...aka code to generate the values you need in the summary text. The balance is heavily tipped that way. Basically 200 lines of code are dedicated to creating variables. There are 6 datasets that you read in, sometimes multiple times. 
   * Do you read each DGS dataset in twice because one is pre-cleaning, and one is post-cleaning? Same for FTA and TIRCP?
   * What is the 7th dataset that is the merged one?
* Challenge: for your Markdown paragraphs, can you write just 1 line of code for every variable you need? You can import your dfs once at the top. 
   * Refer to the Google Doc to see the desired table you want to create
   
## Outline
**Question:** how much do transit agencies pay to procure buses?
**Ideal Table:** transit agency / grant recipients with the bus types (size, propulsion), unit bus cost, and number of buses purchased.

Clear code can be read like a story. Each sentence can be developed with functions. The story of your analysis is roughly this:

1. I have 3 datasets, one for each grant, FTA, TIRCP, DGS.
   * Do these 3 datasets share columns? Can I put them all into 1 final table?
   * What column do I need to add to distiguish between them?
   * When I finish step 1, do I need to have 3 cleaned dfs or can I have just one?
   * If I need 3 cleaned dfs, can I use a naming pattern to easily grab them later? (`fta_cleaned, tircp_cleaned, dgs_cleaned`)
2. I have a long bus description of text, and I need to grab this information to populate my columns: bus size, bus propulsion, unit bus cost, number purchased.
   * clean up description, remove extraneous spaces, make it all lower case
   * I need a function (or two) for tagging all the bus propulsion types
   * I need a function (or two) for tagging all the bus size types
   * ... maybe for unit bus cost or number purchased?
   * I used the bus propulsion function and tagged out the 5 types
   * I used the bus size function and tagged out the 3 types.
3. I have to populate some numeric columns
4. I have to clean up outliers after finding z-scores.
   * Do you want to drop them? Do you want to keep them and add a column called `outlier`? How have you been using it in your summary statement?
5. Other cleaning, maybe agency names, etc etc 
6. Save out my cleaned df, and I will use this for all my charts, captions, paragraphs, etc.

---

## 6/24/24: Response to refactor notes and code review doc

Came back to this project with fresh-eyes after a couple of months and started to see where a lot of improvements can be made. Got a lot more comfortable descriibing what I wanted to do and coming up with code/functions in a more readable way.

It was easier to identify that I had a lot of circular dependencies amongst my cleaner scripts that would reference eachothers functions. I moved all the common functions to a new `bus_cost_utils` file to serve as an importable module for the other scripts.

Next, realized that I had a lot of variables for my f-string in the final `cost_per_bus_analysis` notebook. These variables resulted in a lot of redundent steps like reading in the same data multiple times and sometimes resulted in multiple variables producing the same results multiple times, making navigating the file very difficult. 

Taking a step back, I realized that a simpler, "stripped down" notebook will be easier to understand. Settled on replacing the variables with tables or pivot-tables and focusing on ZEB related metrics. This approach helped cut down the amount of variables needed and consolidate down the information since tables/pivot-tables can help answer multiple quesions at the same time.

## Overall steps taken this round of refactor
* created `bus_cost_utils` module to move all the common functions and variables.
* adjusted cleaner scripts to reference moduel.
* gave cleaned datasets consistent naming convention (raw, cleaned, bus only) and identical column names to merge on.
* final, merged dataset contains columnsfor z-score and an outlier flag. 1 set was saved with outliers, another saved with out outliers.
* used the merged dataset without outliers for the final analysis notebook to create all pivot tables, charts and variables.
* deleted old, initial exploratory notebooks.
* reorganized GCS folder by moving old initial exports to an `/old` folder.