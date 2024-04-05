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