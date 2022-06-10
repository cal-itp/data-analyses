# Tips and Tricks for Portfolio Deployment

## Headers
* **Parameterized Titles**: If you're parameterizing the notebook, the first Markdown cell must include parameters to inject. 

```
## When district is a param in the yml:

# District {district} Analysis
```
    
    * Note: The site URL is constructed from the original notebok name and the parameter: `0_notebook_name__district_x_analysis.html`
    
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
    
    * Capture parameters - this option won't display locally in your notebook (it will still show `{district_number}`), but will be injected with the value when the JupyterBook is built.
    
    ```
    %%capture_parameters
    
    district_number = f"{df.caltrans_district.iloc[0].split('-')[0].strip()}"
    ```
    
    Now, `district_number` is a parameter that can be injected within the notebook in Markdown cells.
    
    ```
    # In a Markdown cell
    
    ## District {district_number}
    ```

* Suppress the warnings from importing packages (`shared_utils`)

```
# Include this in the cell where packages are imported

%%capture
import warnings
warnings.filterwarnings('ignore')
```

## Narrative
* Add a [parameters tag to the code cell](https://papermill.readthedocs.io/en/latest/usage-parameterize.html). 
* It's best to use f-strings to fill values, especially if they may change as you fine-tune the analysis. This allows you to use functions to grab those values for a specific entity (operator, district), rather than hard-coding the values into the narrative. 

```
num_routes = df[df.calitp_itp_id == itp_id].route_id.nunique()
num_parallel = df[(df.calitp_itp_id == itp_id) & (df.parallel==1)].route_id.nunique()

display(
    Markdown(
        f"**Bus routes in service: {num_routes}**"
        "<br>**Parallel routes** to State Highway Network (SHN): "
        f"**{num_parallel} routes**"
        )
)
```

* Stay away from loops if you need to headers. Caveat: You can get away with using `display(HTML())`, but you'll lose the table of contents navigation in the top right corner in the JupyterBook build.