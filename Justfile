# Run tests and linters
@default: test lint

# Run pytest with supplied options
@test *options:
  uv run pytest {{options}}

@run *options:
  uv run -- {{options}}

# Run linters: black, flake8, mypy, cog
@lint:
  just run black . --check
  uv run flake8
  uv run mypy sqlite_utils tests
  uv run cog --check README.md docs/*.rst
  uv run --group docs codespell docs/*.rst --ignore-words docs/codespell-ignore-words.txt

# Rebuild docs with cog
@cog:
  uv run --group docs cog -r README.md docs/*.rst

# Serve live docs on localhost:8000
@docs: cog
  #!/usr/bin/env bash
  cd docs
  uv run --group docs make livehtml


# Apply Black
@black:
  uv run black .
