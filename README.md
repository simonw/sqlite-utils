# sqlite-utils

[![PyPI](https://img.shields.io/pypi/v/sqlite-utils.svg)](https://pypi.org/project/sqlite-utils/)
[![Changelog](https://img.shields.io/github/v/release/simonw/sqlite-utils?include_prereleases&label=changelog)](https://sqlite-utils.readthedocs.io/en/latest/changelog.html)
[![Python 3.x](https://img.shields.io/pypi/pyversions/sqlite-utils.svg?logo=python&logoColor=white)](https://pypi.org/project/sqlite-utils/)
[![Tests](https://github.com/simonw/sqlite-utils/workflows/Test/badge.svg)](https://github.com/simonw/sqlite-utils/actions?query=workflow%3ATest)
[![Documentation Status](https://readthedocs.org/projects/sqlite-utils/badge/?version=latest)](http://sqlite-utils.readthedocs.io/en/latest/?badge=latest)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/simonw/sqlite-utils/blob/main/LICENSE)

Python CLI utility and library for manipulating SQLite databases.

## Some feature highlights

- [Pipe JSON](https://sqlite-utils.readthedocs.io/en/stable/cli.html#inserting-json-data) (or [CSV or TSV](https://sqlite-utils.readthedocs.io/en/stable/cli.html#inserting-csv-or-tsv-data)) directly into a new SQLite database file, automatically creating a table with the appropriate schema
- [Configure SQLite full-text search](https://sqlite-utils.readthedocs.io/en/stable/cli.html#configuring-full-text-search) against your database tables and run search queries against them, ordered by relevance
- Run [transformations against your tables](https://sqlite-utils.readthedocs.io/en/stable/cli.html#transforming-tables) to make schema changes that SQLite `ALTER TABLE` does not directly support, such as dropping columns
- [Extract columns](https://sqlite-utils.readthedocs.io/en/stable/cli.html#extracting-columns-into-a-separate-table) into separate tables to better normalize your existing data

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

See the [full CLI documentation](https://sqlite-utils.readthedocs.io/en/stable/cli.html) for comprehensive coverage of many more commands.

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

Check out the [full library documentation](https://sqlite-utils.readthedocs.io/en/stable/python-api.html) for everything else you can do with the Python library.

## Related projects

* [Datasette](https://github.com/simonw/datasette): A tool for exploring and publishing data
* [csvs-to-sqlite](https://github.com/simonw/csvs-to-sqlite): Convert CSV files into a SQLite database
* [db-to-sqlite](https://github.com/simonw/db-to-sqlite): CLI tool for exporting a MySQL or PostgreSQL database as a SQLite file
* [dogsheep](https://dogsheep.github.io/): A family of tools for personal analytics, built on top of `sqlite-utils`
