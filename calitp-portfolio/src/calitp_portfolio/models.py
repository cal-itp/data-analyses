from pathlib import Path
from typing import Any, Dict, List, Optional

import papermill as pm
import typer
import yaml
from papermill import PapermillExecutionError
from pydantic import BaseModel, Field, field_serializer, field_validator
from slugify import slugify


def slugify_params(params: Dict) -> str:
    return "__".join(f"{k}_{slugify(str(v))}" for k, v in params.items())


def parameterize_filename(i: int, old_path: Path, params: Dict) -> Path:
    assert old_path.suffix == ".ipynb"

    return Path(str(i).zfill(2) + "__" + old_path.stem + "__" + slugify_params(params) + old_path.suffix)


class YamlPartialDumper(yaml.Dumper):

    def increase_indent(self, flow=False, indentless=False):
        return super(YamlPartialDumper, self).increase_indent(flow, False)


class DeployTargets(BaseModel):
    staging: str
    prod: Optional[str] = None


class Chapter(BaseModel):
    caption: Optional[Any] = None
    notebook: Optional[Path] = None
    params: Dict = {}
    sections: List[Dict] = []
    site: "Site" = Field(default=None, exclude=True, repr=False)
    part: "Part" = Field(default=None, exclude=True, repr=False)

    @property
    def resolved_notebook(self):
        return self.notebook or self.part.notebook or self.part.site.notebook

    @property
    def resolved_params(self):
        part_params = self.part.params if self.part else {}
        return {**part_params, **self.params}

    @property
    def slug(self):
        return slugify_params(self.resolved_params)

    @property
    def identifier(self) -> str:
        """Unique CLI handle for `list` output and `build --only` filtering.
        Matches the parameterized notebook filename stem so the slug an analyst
        sees in `list` is the same string they'd see in build artifacts."""
        if self.sections:
            return self.slug
        return parameterize_filename(0, self.resolved_notebook, self.resolved_params).stem

    @property
    def output_dir(self):
        return self.part.site.output_dir if self.part else self.site.output_dir

    @property
    def path(self):
        return self.output_dir / Path(self.slug)

    def generate(
        self, execute_papermill=True, continue_on_error=False, **papermill_kwargs
    ) -> List[PapermillExecutionError]:
        errors = []
        self.path.mkdir(parents=True, exist_ok=True)

        if self.sections:
            fname = self.output_dir / f"{self.slug}.md"
            with open(fname, "w") as f:
                typer.secho(f"writing readme to {fname}", fg=typer.colors.GREEN)
                f.write(f"# {self.caption}")

            for i, section in enumerate(self.sections):
                two_digit_i = str(i).format(width=2)
                params = {**self.resolved_params, **section}
                notebook = section.get("notebook") or self.resolved_notebook

                if not notebook:
                    raise ValueError("no notebook found at any level")

                if isinstance(notebook, str):
                    notebook = Path(notebook)

                parameterized_path = self.path / Path(parameterize_filename(two_digit_i, notebook, params))

                typer.secho(f"parameterizing {notebook} => {parameterized_path}", fg=typer.colors.GREEN)

                if execute_papermill:
                    try:
                        pm.execute_notebook(
                            input_path=notebook,
                            output_path=parameterized_path,
                            parameters=params,
                            cwd=notebook.parent,
                            engine_name="markdown",
                            report_mode=True,
                            original_parameters=params,
                            **papermill_kwargs,
                        )
                    except PapermillExecutionError as e:
                        if continue_on_error:
                            typer.secho(
                                f"papermill error in {notebook} at cell In[{e.exec_count}]: {e.ename}: {e.evalue}",
                                fg=typer.colors.RED,
                            )
                            errors.append(e)
                        else:
                            raise
                else:
                    typer.secho(
                        f"execute_papermill={execute_papermill} so we are skipping actual execution",
                        fg=typer.colors.YELLOW,
                    )

        else:
            notebook = self.resolved_notebook

            if not notebook:
                raise ValueError("no notebook found at any level")

            if isinstance(notebook, str):
                notebook = Path(notebook)

            parameterized_path = self.path / Path(parameterize_filename("00", notebook, self.resolved_params))

            typer.secho(f"parameterizing {notebook} => {parameterized_path}", fg=typer.colors.GREEN)

            if execute_papermill:
                try:
                    pm.execute_notebook(
                        input_path=notebook,
                        output_path=parameterized_path,
                        parameters=self.resolved_params,
                        cwd=notebook.parent,
                        engine_name="markdown",
                        report_mode=True,
                        original_parameters=self.resolved_params,
                        **papermill_kwargs,
                    )
                except PapermillExecutionError as e:
                    if continue_on_error:
                        typer.secho(
                            f"papermill error in {notebook} at cell In[{e.exec_count}]: {e.ename}: {e.evalue}",
                            fg=typer.colors.RED,
                        )
                        errors.append(e)
                    else:
                        raise
            else:
                typer.secho(
                    f"execute_papermill={execute_papermill} so we are skipping actual execution", fg=typer.colors.YELLOW
                )

        return errors

    @property
    def toc(self):
        if self.sections:
            if self.caption:
                return {
                    "title": f"{self.caption}",
                    "file": f"{self.slug}.md",
                    "children": [
                        {
                            "pattern": f"{self.slug}/*",
                        }
                    ],
                }
            else:
                return {
                    "file": f"{self.slug}.md",
                    "children": [
                        {
                            "glob": f"{self.slug}/*",
                        }
                    ],
                }

        folder = f"{self.slug}/" if self.slug else ""

        if self.caption:
            return {
                "title": f"{self.caption}",
                "file": f"{folder}{parameterize_filename('00', self.resolved_notebook, self.resolved_params)}",
            }
        else:
            return {"file": f"{folder}{parameterize_filename('00', self.resolved_notebook, self.resolved_params)}"}


class Part(BaseModel):
    caption: Optional[Any] = None
    notebook: Optional[Path] = None
    params: Dict = {}
    chapters: List[Chapter] = []
    site: "Site" = Field(default=None, exclude=True, repr=False)

    def __init__(self, **data):
        super().__init__(**data)

        for chapter in self.chapters:
            chapter.part = self

    @property
    def slug(self) -> str:
        return slugify_params(self.params)

    @property
    def to_toc(self):
        return {"title": self.caption} if self.caption else {}


class Site(BaseModel):
    output_dir: Path = Field(exclude=True)
    name: str = Field(exclude=True)
    title: str
    directory: Path
    readme: Optional[Path] = "README.md"
    notebook: Optional[Path] = None
    parts: List[Part] = []
    prepare_only: bool = False
    deploy: DeployTargets

    def __init__(self, **data):
        super().__init__(**data)

        for part in self.parts:
            part.site = self

    @field_validator("readme", mode="before", check_fields=False)
    @classmethod
    def default_readme(cls, v, info):
        if "./" in v:
            return Path(v)
        else:
            directory = info.data["directory"]
            return directory / Path(v)

    # str(Path("./foo")) drops the "./" prefix; re-add it so the validator
    # above doesn't double-prefix when this readme is reloaded from yml.
    @field_serializer("readme")
    def _serialize_readme(self, v: Optional[Path]) -> Optional[str]:
        if v is None:
            return None
        s = str(v)
        return s if s.startswith(("/", "./")) else f"./{s}"

    @property
    def slug(self) -> str:
        return slugify(self.title)

    @property
    def toc_yaml(self) -> str:
        toc = [{"file": self.readme.name}]
        for part in self.parts:
            if part.chapters:
                if part.to_toc:
                    children = {"children": [chapter.toc for chapter in part.chapters]}
                    toc.append(part.to_toc | children)
                else:
                    for chapter in part.chapters:
                        toc.append(chapter.toc)

        return yaml.dump(
            {"toc": toc},
            indent=4,
            Dumper=YamlPartialDumper,
        )

    def to_yaml(self) -> str:
        """Serialize this Site to a portfolio site.yml string (canonical form, no disk I/O)."""
        data = self.model_dump(mode="json", exclude_none=True, exclude_defaults=True)
        return yaml.dump(data, sort_keys=False, Dumper=YamlPartialDumper)

    def write_yaml(self, path: Path) -> None:
        """Write `self.to_yaml()` to `path`."""
        Path(path).write_text(self.to_yaml())


class PortfolioConfig(BaseModel):
    sites: List[Site]


def load_site(yml_path: Path, output_dir: Optional[Path] = None) -> Site:
    """Load a Site from yml.

    `output_dir` precedence (highest first):
      1. explicit `output_dir` argument
      2. yml's `output_dir:` field, resolved relative to the yml's directory
      3. default: `<yml dir>/<yml stem>`
    """
    yml_path = Path(yml_path)
    with open(yml_path) as f:
        data = yaml.safe_load(f)

    yml_output_dir = data.pop("output_dir", None)
    resolved_output_dir = output_dir or (
        (yml_path.parent / yml_output_dir) if yml_output_dir else (yml_path.parent / yml_path.stem)
    )
    return Site(
        output_dir=resolved_output_dir,
        name=yml_path.stem,
        **data,
    )
