@default: test lint

@test *options:
  pipenv run pytest {{options}}

@lint:
  pipenv run black . --check
  pipenv run flake8
  pipenv run mypy sqlite_utils tests
  cog --check README.md docs/*.rst

@cog:
  cog -r README.md docs/*.rst

@black:
  pipenv run black .
