from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pydantic import BaseModel

from calitp_portfolio.models import DeployTargets

TEMPLATES_DIR = Path(__file__).parent / "templates"


class SiteEntry(BaseModel):
    title: str
    name: str
    source: str


class SitesManifest(BaseModel):
    deploy: DeployTargets
    sites: list[SiteEntry]
    test_sites: list[SiteEntry] = []


def load_manifest(path: Path) -> SitesManifest:
    with open(path) as f:
        return SitesManifest(**yaml.safe_load(f))


def render_index(manifest: SitesManifest, target: str = "staging") -> str:
    env = Environment(
        loader=FileSystemLoader(TEMPLATES_DIR),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template("index.html")
    test_sites = manifest.test_sites if target == "staging" else []
    return template.render(
        sites=manifest.sites,
        test_sites=test_sites,
        google_analytics_id="",
    )
