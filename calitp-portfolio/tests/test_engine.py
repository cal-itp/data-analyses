from pathlib import Path
from types import SimpleNamespace

import nbformat
from papermill.engines import NBClientEngine

from calitp_portfolio.engine import EngineWithParameterizedMarkdown

NOTEBOOKS = Path(__file__).parent / "fixtures" / "portfolio"


def _load_nb_man(notebook_name: str):
    """Wrap a notebook in a minimal nb_man stand-in so the engine can mutate it without papermill running."""
    nb = nbformat.read(NOTEBOOKS / notebook_name, as_version=4)
    return SimpleNamespace(nb=nb)


def _run_engine(nb_man, params, no_stderr=True, mocker=None):
    """Stub the parent's execute (which would start a real kernel) and invoke just the engine's post-processing."""
    mocker.patch.object(NBClientEngine, "execute_managed_notebook")
    EngineWithParameterizedMarkdown.execute_managed_notebook(
        nb_man, "python3", original_parameters=params, no_stderr=no_stderr
    )


def test_every_code_cell_gets_remove_input_tag(mocker):
    nb_man = _load_nb_man("notebook_with_params_1.ipynb")
    # cell 2 uses %%capture_parameters which reads outputs[0]["text"]; seed it so we don't crash there
    nb_man.nb.cells[2].outputs = [{"output_type": "stream", "name": "stdout", "text": '{"greetings": "Foo"}'}]

    _run_engine(nb_man, params={"greetings": "Foo"}, mocker=mocker)

    code_cells = [c for c in nb_man.nb.cells if c.cell_type == "code"]
    assert code_cells, "fixture should have at least one code cell"
    for cell in code_cells:
        assert "remove-input" in cell.metadata.tags


def test_markdown_cells_get_param_substituted(mocker):
    nb_man = _load_nb_man("notebook_with_params_1.ipynb")
    nb_man.nb.cells[2].outputs = [{"output_type": "stream", "name": "stdout", "text": '{"greetings": "Foo"}'}]

    _run_engine(nb_man, params={"greetings": "Foo"}, mocker=mocker)

    # cell 3 is "# {greetings}" — should be substituted
    assert nb_man.nb.cells[3].source == "# Foo"


def test_capture_parameters_cell_updates_params_for_later_markdown(mocker):
    """%%capture_parameters output gets merged into params *before* later markdown cells render."""
    nb_man = _load_nb_man("notebook_with_params_1.ipynb")
    nb_man.nb.cells[2].outputs = [{"output_type": "stream", "name": "stdout", "text": '{"greetings": "Captured!"}'}]

    _run_engine(nb_man, params={"greetings": "Original"}, mocker=mocker)

    # markdown cell 3 ("# {greetings}") should use the *captured* value, not the original
    assert nb_man.nb.cells[3].source == "# Captured!"


def test_capture_clears_outputs(mocker):
    nb_man = _load_nb_man("notebook_with_params_1.ipynb")
    nb_man.nb.cells[2].outputs = [{"output_type": "stream", "name": "stdout", "text": '{"greetings": "Foo"}'}]
    # cell 1 has %%capture; seed it with an output that should get cleared
    nb_man.nb.cells[1].outputs = [{"output_type": "stream", "name": "stdout", "text": "should be cleared"}]

    _run_engine(nb_man, params={"greetings": "Foo"}, mocker=mocker)

    assert nb_man.nb.cells[1].outputs == []


def test_no_stderr_strips_stderr_outputs(mocker):
    nb_man = _load_nb_man("notebook_with_params_1.ipynb")
    nb_man.nb.cells[2].outputs = [{"output_type": "stream", "name": "stdout", "text": '{"greetings": "Foo"}'}]
    # cell 0 is a plain code cell — mixed stdout + stderr
    nb_man.nb.cells[0].outputs = [
        {"output_type": "stream", "name": "stdout", "text": "keep me"},
        {"output_type": "stream", "name": "stderr", "text": "strip me"},
    ]

    _run_engine(nb_man, params={"greetings": "Foo"}, no_stderr=True, mocker=mocker)

    names = [o.get("name") for o in nb_man.nb.cells[0].outputs]
    assert "stderr" not in names
    assert "stdout" in names


def test_no_stderr_false_keeps_stderr(mocker):
    nb_man = _load_nb_man("notebook_with_params_1.ipynb")
    nb_man.nb.cells[2].outputs = [{"output_type": "stream", "name": "stdout", "text": '{"greetings": "Foo"}'}]
    nb_man.nb.cells[0].outputs = [
        {"output_type": "stream", "name": "stderr", "text": "still here"},
    ]

    _run_engine(nb_man, params={"greetings": "Foo"}, no_stderr=False, mocker=mocker)

    assert nb_man.nb.cells[0].outputs[0]["name"] == "stderr"


def test_resolver_typeerror_is_swallowed(mocker):
    """district_name(**params) raises TypeError if params lacks 'district'; the engine should keep going."""
    nb_man = _load_nb_man("notebook_with_params_1.ipynb")
    nb_man.nb.cells[2].outputs = [{"output_type": "stream", "name": "stdout", "text": '{"greetings": "Foo"}'}]

    # params has no "district" — district_name will TypeError; engine must not propagate it
    _run_engine(nb_man, params={"greetings": "Foo"}, mocker=mocker)

    # post-condition: cell mutations still happened
    assert "remove-input" in nb_man.nb.cells[0].metadata.tags


def test_resolver_fires_when_params_match():
    """When params include 'district', district_name resolver should compute the humanized name."""
    from calitp_portfolio.engine import district_name

    assert district_name(district=3) == "Three"
    assert district_name(district=12) == "12"  # humanize.apnumber returns digits for >10


def test_capture_parameters_cell_with_no_outputs_does_not_crash(mocker):
    """%%capture_parameters cell errors, dont crash."""
    nb_man = _load_nb_man("notebook_with_params_1.ipynb")
    nb_man.nb.cells[2].outputs = []  # capture_parameters cell ended up with no outputs

    _run_engine(nb_man, params={"greetings": "Original"}, mocker=mocker)

    # Original param value is preserved; markdown cell 3 ("# {greetings}") uses it
    assert nb_man.nb.cells[3].source == "# Original"


def test_capture_parameters_cell_with_malformed_output_does_not_crash(mocker):
    """%%capture_parameters cell doesn't contain parseable JSON."""
    nb_man = _load_nb_man("notebook_with_params_1.ipynb")
    nb_man.nb.cells[2].outputs = [{"output_type": "stream", "name": "stdout", "text": "not valid json"}]

    _run_engine(nb_man, params={"greetings": "Original"}, mocker=mocker)

    assert nb_man.nb.cells[3].source == "# Original"
