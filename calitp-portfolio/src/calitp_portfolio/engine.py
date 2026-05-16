import json

import humanize
from papermill.engines import NBClientEngine, papermill_engines


def district_name(district, **_):
    return humanize.apnumber(district).title()


RESOLVERS = [
    district_name,
]


class EngineWithParameterizedMarkdown(NBClientEngine):
    @classmethod
    def execute_managed_notebook(cls, nb_man, kernel_name, **kwargs):
        # call the papermill execution engine:
        super().execute_managed_notebook(nb_man, kernel_name, **kwargs)

        assert "original_parameters" in kwargs

        params = kwargs["original_parameters"]
        no_stderr = kwargs["no_stderr"]

        for func in RESOLVERS:
            try:
                params[func.__name__] = func(**kwargs["original_parameters"])
            except TypeError:
                pass

        for cell in nb_man.nb.cells:

            # display() calls for markdown break jupyterbook/sphinx
            # https://github.com/executablebooks/jupyter-book/issues/1610
            # so we have to manually parameterize headers in markdown cells; for example, "District {district}" in a
            # markdown cell vs "display(Markdown(f"## District: {district}))" in a code cell
            if cell.cell_type == "markdown":
                cell.source = cell.source.format(**params)

            # hide input (i.e. code) for all cells
            if cell.cell_type == "code":
                cell.metadata.tags.append("remove-input")

                # Consider importing this name from calitp.magics
                if "%%capture_parameters" in cell.source:
                    params = {**params, **json.loads(cell.outputs[0]["text"])}

                if "%%capture" in cell.source:
                    cell.outputs = []

                if no_stderr:
                    cell.outputs = [
                        output for output in cell.outputs if "name" not in output.keys() or output["name"] != "stderr"
                    ]


papermill_engines.register("markdown", EngineWithParameterizedMarkdown)
papermill_engines.register_entry_points()
