# Run tests and linters
@default: test lint

# Setup project
@init:
  pipenv run pip install -e '.[test,docs,mypy,flake8]'

# Run pytest with supplied options
@test *options:
  pipenv run pytest {{options}}

# Run linters: black, flake8, mypy, cog
@lint:
  pipenv run black . --check
  pipenv run flake8
  pipenv run mypy sqlite_utils tests
  pipenv run cog --check README.md docs/*.rst

# Rebuild docs with cog
@cog:
  pipenv run cog -r README.md docs/*.rst

# Serve live docs on localhost:8000
@docs: cog
  cd docs && pipenv run make livehtml

# Apply Black
@black:
  pipenv run black .
