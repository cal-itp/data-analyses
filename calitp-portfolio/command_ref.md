Index page

uv run calitp-portfolio index tests/fixtures/sites/sites.yml --output /tmp/index.html

# Basic site
uv run calitp-portfolio build tests/fixtures/sites/_basic_analyses_test.yml --output-dir /tmp/build-basic-real

# README-only site
uv run calitp-portfolio build tests/fixtures/sites/_readme_only_analyses_test.yml --output-dir /tmp/build-readme --no-execute

# Section chapters
uv run calitp-portfolio build tests/fixtures/sites/_section_analyses_test.yml --output-dir /tmp/build-sections --no-execute
ls /tmp/build-sections/   # day_or_night_01-day.md, day_or_night_02-night.md

# --hide-title-block diff
uv run calitp-portfolio build tests/fixtures/sites/_param_manual_title_analyses_test.yml --output-dir /tmp/build-on --no-execute --hide-title-block
