"""Pure Site -> Site helpers that populate `parts:` from Python data.

Replace `_shared_utils.portfolio_utils.create_portfolio_yaml_chapters_*`. The
helpers do not touch disk; pair with `load_site` / `Site.write_yaml` for the
prepare-script (load -> mutate -> save) workflow.
"""

from typing import Any, Mapping, Sequence, TypedDict

from calitp_portfolio.models import Chapter, Part, Site


def generate_parts_flat(site: Site, *, param_key: str, values: Sequence[Any]) -> Site:
    """One Part with N Chapters, each Chapter has params={param_key: str(value)}."""
    chapters = [Chapter(params={param_key: str(v)}) for v in values]
    return site.model_copy(update={"parts": [Part(chapters=chapters)]})


def generate_parts_grouped(
    site: Site,
    *,
    param_key: str,
    groups: Mapping[str, Sequence[Any]],
) -> Site:
    """N Parts (dict key = caption, in insertion order), each with M Chapters of params={param_key: str(value)}."""
    parts = [
        Part(
            caption=caption,
            chapters=[Chapter(params={param_key: str(v)}) for v in values],
        )
        for caption, values in groups.items()
    ]
    return site.model_copy(update={"parts": parts})


class ChapterSpec(TypedDict):
    caption: str
    sections: Sequence[Any]


def generate_parts_sections(
    site: Site,
    *,
    chapter_key: str,
    section_key: str,
    chapters: Mapping[str, ChapterSpec],
) -> Site:
    """One Part with N Chapters, each Chapter has caption + params={chapter_key: key} + sections=[{section_key: v}, ...]."""
    chapter_objs = [
        Chapter(
            caption=spec["caption"],
            params={chapter_key: str(key)},
            sections=[{section_key: str(s)} for s in spec["sections"]],
        )
        for key, spec in chapters.items()
    ]
    return site.model_copy(update={"parts": [Part(chapters=chapter_objs)]})
