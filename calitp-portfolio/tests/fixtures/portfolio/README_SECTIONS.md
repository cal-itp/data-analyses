<br>
<br>

# My Params Readme

This is an example of analyses with sections.

## Definitions

To create a site like this with sections, please refer to [this YAML file](https://github.com/cal-itp/data-analyses/blob/main/portfolio/sites/_section_analyses_test.yml) as an example.

You can also use `create_portfolio_yaml_chapters_with_sections` from `portfolio_utils.py` to generate the YAML file.

If the page header shows in duplicity you can use the flag `--hide-title-block` to build the site.

Example:

```bash
$ uv run python portfolio/portfolio.py build _section_analyses_test --hide-title-block
```
