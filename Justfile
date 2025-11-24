# Run tests and linters
@default: test lint

# Run pytest with supplied options
@test *options:
  just run pytest {{options}}

@run *options:
  uv run --isolated --with-editable '.[test,mypy,flake8,docs]' -- {{options}}

# Run linters: black, flake8, mypy, cog
@lint:
  just run black . --check
  just run flake8
  just run mypy sqlite_utils tests
  just run cog --check README.md docs/*.rst
  just run codespell docs/*.rst --ignore-words docs/codespell-ignore-words.txt

# Rebuild docs with cog
@cog:
  just run cog -r README.md docs/*.rst

# Serve live docs on localhost:8000
@docs: cog
  #!/usr/bin/env bash
  cd docs
  uv run --isolated --with-editable '../.[test,docs]' make livehtml


# Apply Black
@black:
  just run black .
