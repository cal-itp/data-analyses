from pathlib import Path

from calitp_portfolio.models import Site, load_site
from calitp_portfolio.mutations import generate_parts_flat, generate_parts_grouped, generate_parts_sections

SITE_FIXTURES = Path(__file__).parent / "fixtures" / "sites"


def _strip_parts(site: Site) -> Site:
    return site.model_copy(update={"parts": []})


# --- generate_parts_flat ---


def test_generate_parts_flat_matches_fixture():
    expected = load_site(SITE_FIXTURES / "_param_analyses_test.yml")
    result = generate_parts_flat(
        _strip_parts(expected),
        param_key="greetings",
        values=["Hi! So happy to see you here!!", "Bye! See you soon!!"],
    )
    assert result.to_yaml() == expected.to_yaml()


def test_generate_parts_flat_round_trips_through_yaml(tmp_path):
    expected = load_site(SITE_FIXTURES / "_param_analyses_test.yml")
    result = generate_parts_flat(
        _strip_parts(expected),
        param_key="greetings",
        values=["Hi! So happy to see you here!!", "Bye! See you soon!!"],
    )
    out = tmp_path / "rendered.yml"
    result.write_yaml(out)
    reloaded = load_site(out, output_dir=expected.output_dir)
    assert reloaded.to_yaml() == expected.to_yaml()


def test_generate_parts_flat_coerces_non_string_values():
    base = load_site(SITE_FIXTURES / "_param_analyses_test.yml")
    result = generate_parts_flat(_strip_parts(base), param_key="district", values=[1, 2, 3])
    assert [c.params for c in result.parts[0].chapters] == [
        {"district": "1"},
        {"district": "2"},
        {"district": "3"},
    ]


def test_generate_parts_flat_preserves_duplicates_and_order():
    base = load_site(SITE_FIXTURES / "_param_analyses_test.yml")
    result = generate_parts_flat(_strip_parts(base), param_key="x", values=["b", "a", "b"])
    assert [c.params["x"] for c in result.parts[0].chapters] == ["b", "a", "b"]


def test_generate_parts_flat_empty_values():
    base = load_site(SITE_FIXTURES / "_param_analyses_test.yml")
    result = generate_parts_flat(_strip_parts(base), param_key="x", values=[])
    assert result.parts == [result.parts[0]]
    assert result.parts[0].chapters == []


def test_generate_parts_flat_replaces_existing_parts():
    base = load_site(SITE_FIXTURES / "_param_analyses_test.yml")
    assert base.parts
    result = generate_parts_flat(base, param_key="x", values=["only"])
    assert len(result.parts) == 1
    assert result.parts[0].chapters[0].params == {"x": "only"}


# --- generate_parts_grouped ---


def test_generate_parts_grouped_matches_fixture():
    expected = load_site(SITE_FIXTURES / "_group_and_params_analyses_test.yml")
    result = generate_parts_grouped(
        _strip_parts(expected),
        param_key="greetings",
        groups={
            "District 01 Eureka": [
                "Humboldt Transit Authority",
                "Lake Transit Authority",
                "Mendocino Transit Authority",
                "Redwood Coast Transit Authority",
            ],
            "District 02 Redding": [
                "Redding Area Bus Authority",
                "Tehama County",
            ],
        },
    )
    assert result.to_yaml() == expected.to_yaml()


def test_generate_parts_grouped_round_trips_through_yaml(tmp_path):
    expected = load_site(SITE_FIXTURES / "_group_and_params_analyses_test.yml")
    result = generate_parts_grouped(
        _strip_parts(expected),
        param_key="greetings",
        groups={
            "District 01 Eureka": [
                "Humboldt Transit Authority",
                "Lake Transit Authority",
                "Mendocino Transit Authority",
                "Redwood Coast Transit Authority",
            ],
            "District 02 Redding": [
                "Redding Area Bus Authority",
                "Tehama County",
            ],
        },
    )
    out = tmp_path / "rendered.yml"
    result.write_yaml(out)
    reloaded = load_site(out, output_dir=expected.output_dir)
    assert reloaded.to_yaml() == expected.to_yaml()


def test_generate_parts_grouped_preserves_dict_order():
    base = load_site(SITE_FIXTURES / "_group_and_params_analyses_test.yml")
    result = generate_parts_grouped(
        _strip_parts(base),
        param_key="x",
        groups={"Z": ["a"], "A": ["b"], "M": ["c"]},
    )
    assert [p.caption for p in result.parts] == ["Z", "A", "M"]


def test_generate_parts_grouped_coerces_non_string_values():
    base = load_site(SITE_FIXTURES / "_group_and_params_analyses_test.yml")
    result = generate_parts_grouped(
        _strip_parts(base),
        param_key="district",
        groups={"Group A": [1, 2], "Group B": [3]},
    )
    assert [c.params for c in result.parts[0].chapters] == [{"district": "1"}, {"district": "2"}]
    assert [c.params for c in result.parts[1].chapters] == [{"district": "3"}]


# --- generate_parts_sections ---


def test_generate_parts_sections_matches_fixture():
    expected = load_site(SITE_FIXTURES / "_section_analyses_test.yml")
    result = generate_parts_sections(
        _strip_parts(expected),
        chapter_key="day_or_night",
        section_key="greetings",
        chapters={
            "01 - Day": {
                "caption": "Daily Greetings",
                "sections": ["Good Morning!", "Good Afternoon!"],
            },
            "02 - Night": {
                "caption": "Night Greetings",
                "sections": ["Sleep well!"],
            },
        },
    )
    assert result.to_yaml() == expected.to_yaml()


def test_generate_parts_sections_round_trips_through_yaml(tmp_path):
    expected = load_site(SITE_FIXTURES / "_section_analyses_test.yml")
    result = generate_parts_sections(
        _strip_parts(expected),
        chapter_key="day_or_night",
        section_key="greetings",
        chapters={
            "01 - Day": {
                "caption": "Daily Greetings",
                "sections": ["Good Morning!", "Good Afternoon!"],
            },
            "02 - Night": {
                "caption": "Night Greetings",
                "sections": ["Sleep well!"],
            },
        },
    )
    out = tmp_path / "rendered.yml"
    result.write_yaml(out)
    reloaded = load_site(out, output_dir=expected.output_dir)
    assert reloaded.to_yaml() == expected.to_yaml()


def test_generate_parts_sections_preserves_dict_order():
    base = load_site(SITE_FIXTURES / "_section_analyses_test.yml")
    result = generate_parts_sections(
        _strip_parts(base),
        chapter_key="k",
        section_key="s",
        chapters={
            "z": {"caption": "Z", "sections": ["one"]},
            "a": {"caption": "A", "sections": ["two"]},
        },
    )
    assert [c.params["k"] for c in result.parts[0].chapters] == ["z", "a"]
    assert [c.caption for c in result.parts[0].chapters] == ["Z", "A"]
