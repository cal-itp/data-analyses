from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, field_validator
from slugify import slugify


def slugify_params(params: Dict) -> str:
    return "__".join(f"{k}_{slugify(str(v))}" for k, v in params.items())


class DeployTargets(BaseModel):
    staging: str
    prod: Optional[str] = None


class Chapter(BaseModel):
    caption: Optional[Any] = None
    notebook: Optional[Path] = None
    params: Dict = {}
    sections: List[Dict] = []
    site: "Site" = None
    part: "Part" = None

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
    def output_dir(self):
        return self.part.site.output_dir if self.part else self.site.output_dir

    @property
    def path(self):
        return self.output_dir / Path(self.slug)


class Part(BaseModel):
    caption: Optional[Any] = None
    notebook: Optional[Path] = None
    params: Dict = {}
    chapters: List[Chapter] = []
    site: "Site" = None

    def __init__(self, **data):
        super().__init__(**data)

        for chapter in self.chapters:
            chapter.part = self

    @property
    def slug(self) -> str:
        return slugify_params(self.params)


class Site(BaseModel):
    output_dir: Path
    name: str
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

    @property
    def slug(self) -> str:
        return slugify(self.title)


class PortfolioConfig(BaseModel):
    sites: List[Site]
