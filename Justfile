# Run tests and linters
@default: test lint

# Run pytest with supplied options
@test *options:
  pipenv run pytest {{options}}

# Run linters: black, flake8, mypy, cog
@lint:
  pipenv run black . --check
  pipenv run flake8
  pipenv run mypy sqlite_utils tests
  cog --check README.md docs/*.rst

# Rebuild docs with cog
@cog:
  cog -r README.md docs/*.rst

@docs: cog
  cd docs && pipenv run make livehtml

# Apply Black
@black:
  pipenv run black .
