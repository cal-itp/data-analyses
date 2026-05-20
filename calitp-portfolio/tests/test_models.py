from pathlib import Path

import pytest
import yaml

from calitp_portfolio.models import DeployTargets, Site, load_site

SITE_FIXTURES = Path(__file__).parent / "fixtures" / "sites"

ALL_TEST_YMLS = [
    "_basic_analyses_test.yml",
    "_group_analyses_test.yml",
    "_group_and_params_analyses_test.yml",
    "_param_analyses_test.yml",
    "_param_manual_title_analyses_test.yml",
    "_readme_only_analyses_test.yml",
    "_section_analyses_test.yml",
]


def _load_site(yml_name: str) -> Site:
    path = SITE_FIXTURES / yml_name
    name = path.stem
    with open(path) as f:
        return Site(output_dir=Path("./portfolio") / name, name=name, **yaml.safe_load(f))


@pytest.mark.parametrize("yml_name", ALL_TEST_YMLS)
def test_all_fixtures_parse(yml_name):
    site = _load_site(yml_name)
    assert site.title
    assert site.directory == Path("./tests/fixtures/portfolio/")


def test_basic_fixture_has_flat_shape_with_chapter_level_notebooks():
    """_basic_analyses_test: flat — no part caption, chapters carry their own notebooks."""
    site = _load_site("_basic_analyses_test.yml")
    assert len(site.parts) == 1
    part = site.parts[0]
    assert part.caption is None
    assert part.notebook is None
    assert all(ch.notebook is not None for ch in part.chapters)


def test_group_fixture_exposes_part_caption():
    """_group_analyses_test: a part with a caption groups its chapters."""
    site = _load_site("_group_analyses_test.yml")
    assert site.parts[0].caption == "Introduction"


def test_group_and_params_fixture_combines_part_caption_with_chapter_params():
    """_group_and_params_analyses_test: each part has a caption, each chapter has params."""
    site = _load_site("_group_and_params_analyses_test.yml")
    assert all(part.caption for part in site.parts)
    assert all(ch.params for part in site.parts for ch in part.chapters)


def test_param_fixture_chapter_slug_is_driven_by_params():
    """_param_analyses_test: chapters have no caption; slugs are derived from params."""
    site = _load_site("_param_analyses_test.yml")
    chapter = site.parts[0].chapters[0]
    assert chapter.caption is None
    assert chapter.params
    assert chapter.slug == "greetings_hi-so-happy-to-see-you-here"
    assert chapter.resolved_notebook == site.notebook


def test_param_manual_title_fixture_keeps_explicit_chapter_caption():
    """_param_manual_title_analyses_test: explicit chapter caption coexists with params."""
    site = _load_site("_param_manual_title_analyses_test.yml")
    captions = [ch.caption for ch in site.parts[0].chapters]
    assert captions == ["Hi!", "Bye!"]


def test_readme_only_fixture_has_no_parts():
    """_readme_only_analyses_test: a site can be just a README, no parts."""
    site = _load_site("_readme_only_analyses_test.yml")
    assert site.parts == []
    assert site.readme == Path("tests/fixtures/portfolio/README.md")


def test_section_fixture_chapters_carry_sections():
    """_section_analyses_test: chapters can have a sections: list (sub-chapter structure)."""
    site = _load_site("_section_analyses_test.yml")
    chapter = site.parts[0].chapters[0]
    assert chapter.caption == "Daily Greetings"
    assert chapter.sections == [{"greetings": "Good Morning!"}, {"greetings": "Good Afternoon!"}]


def test_back_references_wired_after_init():
    """Cross-cutting: part.site and chapter.part are populated by Site.__init__."""
    site = _load_site("_group_analyses_test.yml")
    part = site.parts[0]
    assert part.site is site
    assert all(ch.part is part for ch in part.chapters)


def test_deploy_block_parses_into_submodel():
    site = _load_site("_basic_analyses_test.yml")
    assert isinstance(site.deploy, DeployTargets)
    assert site.deploy.staging == "gs://calitp-analysis-staging/_basic_analyses_test"


def test_deploy_block_is_required():
    """Every site yml must declare a deploy: block (with at least staging)."""
    yml_text = (
        "title: No Deploy\ndirectory: ./tests/fixtures/portfolio/\nreadme: ./tests/fixtures/portfolio/README.md\n"
    )
    with pytest.raises(Exception):
        Site(output_dir=Path("./portfolio/no_deploy"), name="no_deploy", **yaml.safe_load(yml_text))


def test_load_site_default_output_dir_is_yml_parent_stem(tmp_path):
    yml = tmp_path / "s.yml"
    yml.write_text((SITE_FIXTURES / "_readme_only_analyses_test.yml").read_text())

    site = load_site(yml)
    assert site.output_dir == tmp_path / "s"


def test_load_site_resolves_yml_output_dir_relative_to_yml_parent(tmp_path):
    """Optional `output_dir:` in the yml overrides the <stem> default, anchored at yml.parent."""
    yml = tmp_path / "s.yml"
    base = (SITE_FIXTURES / "_readme_only_analyses_test.yml").read_text()
    yml.write_text(base + "output_dir: custom\n")

    site = load_site(yml)
    assert site.output_dir == tmp_path / "custom"


def test_load_site_explicit_arg_overrides_yml_output_dir(tmp_path):
    """Explicit `output_dir` arg trumps the yml's `output_dir:` field."""
    yml = tmp_path / "s.yml"
    base = (SITE_FIXTURES / "_readme_only_analyses_test.yml").read_text()
    yml.write_text(base + "output_dir: from-yml\n")

    site = load_site(yml, output_dir=tmp_path / "from-arg")
    assert site.output_dir == tmp_path / "from-arg"


def test_site_base_url_derived_from_deploy_staging():
    """Site.base_url returns `/<prefix>` from the path component of `deploy.staging`."""
    site = _load_site("_readme_only_analyses_test.yml")
    assert site.base_url == "/_readme_only_analyses_test"
