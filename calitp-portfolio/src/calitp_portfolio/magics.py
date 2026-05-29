"""
Papermill helper magic for the portfolio build.

`capture_parameters` is cell-magic half between notebooks and
`EngineWithParameterizedMarkdown.`
`%%capture_parameters` runs silently and prints a JSON blob to stdout; the
engine reads that blob from the cell's outputs and merges it into `params`
for the site.yml.

-- registration happens inside the papermill kernel, when the notebook
imports a module that calls `register_cell_magic`.
"""

import json

from IPython import get_ipython
from IPython.core.magic import register_cell_magic


def capture_parameters(line, cell):
    shell = get_ipython()
    shell.run_cell(cell, silent=True)
    # We assume the last line is a tuple
    tup = [s.strip() for s in cell.strip().split("\n")[-1].split(",")]
    print(json.dumps({identifier: shell.user_ns[identifier] for identifier in tup if identifier}))


# Dont register outside a papermill kernal (pytest)
if get_ipython() is not None:
    register_cell_magic(capture_parameters)
