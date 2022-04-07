# Rework Docs

* Analysts drop their own Markdown files within `data-analyses/example_report` for someone else to consolidate into 1 doc
    * highlight interesting tidbits of code, techniques, or StackOverflow/websites for figuring out very specific things (arrowizing, sorting legend order, inflation, etc)
    * also hold practice exercises
        * URLs redirecting to tutorials in `best-practices` will need to be fixed
        * Tutorials held in `example_report`
        * Analyst knowledge consolidation held in `data-infra/docs`
    * clean up `example_report`, potentially consolidating the `example_charts`, `example_maps` and `shared_utils_examples` into one notebook, which can be referenced in from docs. `style-guide-examples` can stand alone. standardize naming (underscores vs dashes).