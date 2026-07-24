# Run this in data-analyses
# To specify different Makefile: make some_command -f Makefile2

# IMPORTANT!
# If you are using `Prototype Image - 2026.3.18, Python 3.11` in JupyterHub, you need to add `uv run ` to the beginning of the commands in order to work.
# For example: Instead of `python portfolio/portfolio.py clean $(site)`, run `uv run python portfolio/portfolio.py clean $(site)`.

add_precommit:
	uv run pre-commit install
	#pre-commit run --all-files

# Add to _.bash_profile outside of data-analyses
#alias go='cd ~/data-analyses && uv sync'

install_env:
	# "_shared_utils", "rt_segment_speeds", and "rt_delay" are already installed when `uv sync` runs.
	uv sync --all-groups
	make add_precommit
