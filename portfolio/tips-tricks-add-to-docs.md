# Tips and Tricks for Portfolio Deployment

## Headers
* **Parameterized Titles**: If you're parameterizing the notebook, the first Markdown cell must include parameters to inject. 

```
# district is set as a param in the yml 
# the url for this notebook in the portfolio will be district_x_analysis.html
# District {district} Analysis
```
* Headers must move consecutively. No skipping! 

```
# Notebook Title
## First Section
## Second Section 
### Another subheading
```

* Markdown cells can inject f-strings if it's plain Markdown (not a heading).

```
IPython.display import Markdown

display(Markdown(f"The value of {variable} is {value}."))
```

* If you're using a heading, you can either use HTML or capture the parameter and inject.
    * HTML - this option works when you run your notebook locally.
    
    ```
    from IPython.display import HTML

    display(HTML(f"<h3>Header with {variable}</h3>"))
    ```
    
    * Capture parameters - this option won't display locally in your notebook, but will display when the JupyterBook is built.
    
    ```
    %%capture_parameters
    
    district_number = f"{df.caltrans_district.iloc[0].split('-')[0].strip()}"
    ```
    
    Now, `district_number` is a parameter that can be injected within the notebook in Markdown cells.
    
    ```
    # In a Markdown cell
    
    ## District {district_number}
    ```