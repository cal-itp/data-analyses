<br>
<br>

# My Group and Params Readme

This is an example of analyses with groups and params.

## Definitions

To create a site like this with groups and params, please refer to [this YAML file](https://github.com/cal-itp/data-analyses/blob/main/portfolio/sites/_group_and_params_analyses_test.yml) as an example.

You can also use `create_portfolio_yaml_chapters_with_groups` from `portfolio_utils.py` to generate the YAML file.

If the page header shows in duplicity you can use the flag `--hide-title-block` to build the site.

Example:

```bash
$ uv run python portfolio/portfolio.py build _group_and_params_analyses_test --hide-title-block
```
