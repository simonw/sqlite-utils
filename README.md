# sqlite-utils

[![PyPI](https://img.shields.io/pypi/v/sqlite-utils.svg)](https://pypi.org/project/sqlite-utils/)
[![Changelog](https://img.shields.io/github/v/release/simonw/sqlite-utils?include_prereleases&label=changelog)](https://sqlite-utils.readthedocs.io/en/stable/changelog.html)
[![Python 3.x](https://img.shields.io/pypi/pyversions/sqlite-utils.svg?logo=python&logoColor=white)](https://pypi.org/project/sqlite-utils/)
[![Tests](https://github.com/simonw/sqlite-utils/workflows/Test/badge.svg)](https://github.com/simonw/sqlite-utils/actions?query=workflow%3ATest)
[![Documentation Status](https://readthedocs.org/projects/sqlite-utils/badge/?version=latest)](http://sqlite-utils.readthedocs.io/en/latest/?badge=latest)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/simonw/sqlite-utils/blob/main/LICENSE)

Python CLI utility and library for manipulating SQLite databases.

Read more on my blog: [
sqlite-utils: a Python library and CLI tool for building SQLite databases](https://simonwillison.net/2019/Feb/25/sqlite-utils/) and other [entries tagged sqliteutils](https://simonwillison.net/tags/sqliteutils/).

## Installation

    pip install sqlite-utils

## Using as a CLI tool

Now you can do things with the CLI utility like this:

    $ sqlite-utils tables dogs.db --counts
    [{"table": "dogs", "count": 2}]

    $ sqlite-utils dogs.db "select * from dogs"
    [{"id": 1, "age": 4, "name": "Cleo"},
     {"id": 2, "age": 2, "name": "Pancakes"}]

    $ sqlite-utils dogs.db "select * from dogs" --csv
    id,age,name
    1,4,Cleo
    2,2,Pancakes

    $ sqlite-utils dogs.db "select * from dogs" --table
      id    age  name
    ----  -----  --------
       1      4  Cleo
       2      2  Pancakes

You can even import data into a new database table like this:

    $ curl https://api.github.com/repos/simonw/sqlite-utils/releases \
        | sqlite-utils insert releases.db releases - --pk id

Full CLI documentation: https://sqlite-utils.readthedocs.io/en/stable/cli.html

## Using as a library

You can also `import sqlite_utils` and use it as a Python library like this:

```python
import sqlite_utils
db = sqlite_utils.Database("demo_database.db")
# This line creates a "dogs" table if one does not already exist:
db["dogs"].insert_all([
    {"id": 1, "age": 4, "name": "Cleo"},
    {"id": 2, "age": 2, "name": "Pancakes"}
], pk="id")
```

Full library documentation: https://sqlite-utils.readthedocs.io/en/stable/python-api.html

## Related projects

* [Datasette](https://github.com/simonw/datasette): A tool for exploring and publishing data
* [csvs-to-sqlite](https://github.com/simonw/csvs-to-sqlite): Convert CSV files into a SQLite database
* [db-to-sqlite](https://github.com/simonw/db-to-sqlite): CLI tool for exporting a MySQL or PostgreSQL database as a SQLite file
* [dogsheep](https://dogsheep.github.io/): A family of tools for personal analytics, built on top of `sqlite-utils`
