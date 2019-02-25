# sqlite-utils

[![PyPI](https://img.shields.io/pypi/v/sqlite-utils.svg)](https://pypi.org/project/sqlite-utils/)
[![Travis CI](https://travis-ci.com/simonw/sqlite-utils.svg?branch=master)](https://travis-ci.com/simonw/sqlite-utils)
[![Documentation Status](https://readthedocs.org/projects/sqlite-utils/badge/?version=latest)](http://sqlite-utils.readthedocs.io/en/latest/?badge=latest)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/simonw/sqlite-utils/blob/master/LICENSE)

Python CLI utility and library for manipulating SQLite databases.

Read more on my blog: [
sqlite-utils: a Python library and CLI tool for building SQLite databases](https://simonwillison.net/2019/Feb/25/sqlite-utils/)

Install it like this:

    pip3 install sqlite-utils

Now you can do things like this:

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

Full documentation: https://sqlite-utils.readthedocs.io/

Related projects:

* [Datasette](https://github.com/simonw/datasette): A tool for exploring and publishing data
* [csvs-to-sqlite](https://github.com/simonw/csvs-to-sqlite): Convert CSV files into a SQLite database
