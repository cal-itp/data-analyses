# Getting Notebooks Ready for the Portfolio

## Headers

### Parameterized Titles
* If you're parameterizing the notebook, the first Markdown cell must include parameters to inject. 
    * Ex: If `district` is one of the parameters in your `sites/my_report.yml`, a header Markdown cell could be `# District {district} Analysis`.
    * Note: The site URL is constructed from the original notebok name and the parameter in the JupyterBook build: `0_notebook_name__district_x_analysis.html`
    
### Consecutive Headers

* Headers must move consecutively in Markdown cells. No skipping! 

```
# Notebook Title
## First Section
## Second Section 
### Another subheading
```

* To get around consecutive headers, you can use `display(HTML())`.  

    ```
    display(HTML(<h1>First Header</h1>) display(HTML(<h3>Next Header</h3>))
    ```

### Capturing Parameters
* If you're using a heading, you can either use HTML or capture the parameter and inject.
* HTML - this option works when you run your notebook locally.

    ```
    from IPython.display import HTML

    display(HTML(f"<h3>Header with {variable}</h3>"))
    ```
    
* Capture parameters - this option won't display locally in your notebook (it will still show `{district_number}`), but will be injected with the value when the JupyterBook is built. 

    In a code cell:
    ```
    %%capture_parameters

    district_number = f"{df.caltrans_district.iloc[0].split('-')[0].strip()}"
    ```
    
    <br>
    
    In a Markdown cell:
    ```
    ## District {district_number}
    ```


### Suppress Warnings
* Suppress warnings from displaying in the portfolio site (`shared_utils`).

```
# Include this in the cell where packages are imported

%%capture
import warnings
warnings.filterwarnings('ignore')
```

## Narrative 
* Narrative content can be done in Markdown cells or code cells. 
    * Markdown cells should be used when there are no variables to inject.
    * Code cells should be used to write narrative whenever variables constructed from f-strings are used.
* For `papermill`, add a [parameters tag to the code cell](https://papermill.readthedocs.io/en/latest/usage-parameterize.html) 
    Note: Our portfolio uses a custom `papermill` engine and we can skip this step.
* Markdown cells can inject f-strings if it's plain Markdown (not a heading) using `display(Markdown())` in a code cell.

```
from IPython.display import Markdown

display(Markdown(f"The value of {variable} is {value}."))
```

* **Use f-strings to fill in variables and values instead of hard-coding them**
    * Turn anything that runs in a loop or relies on a function into a variable.
    * Use functions to grab those values for a specific entity (operator, district), rather than hard-coding the values into the narrative. 

```
n_routes = (df[df.calitp_itp_id == itp_id]
            .route_id.nunique()
            )


n_parallel = (df[
            (df.calitp_itp_id == itp_id) & 
            (df.parallel==1)]
            .route_id.nunique()
            )

display(
    Markdown(
        f"**Bus routes in service: {n_routes}**"
        "<br>**Parallel routes** to State Highway Network (SHN): "
        f"**{n_parallel} routes**"
        )
)
```

* Stay away from loops if you need to headers. 
    * You will need to create Markdown cells for headers or else JupyterBook will not build correctly. For parameterized notebooks, this is an acceptable trade-off.
    * For unparameterized notebooks, you may want use `display(HTML())`.
    * Caveat: Using `display(HTML())` means you'll lose the table of contents navigation in the top right corner in the JupyterBook build. 