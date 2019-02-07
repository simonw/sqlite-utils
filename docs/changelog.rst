===========
 Changelog
===========

.. _v0_10:

0.10 (2019-02-06)
-----------------

Handle ``datetime.date`` and ``datetime.time`` values.

New option for efficiently inserting rows from a CSV:
::

    sqlite-utils insert db.db foo - --csv

.. _v0_9:

0.9 (2019-01-27)
----------------

Improved support for newline-delimited JSON.

``sqlite-utils insert`` has two new command-line options:

* ``--nl`` means "expect newline-delimited JSON". This is an extremely efficient way of loading in large amounts of data, especially if you pipe it into standard input.
* ``--batch-size=1000`` lets you increase the batch size (default is 100). A commit will be issued every X records. This also control how many initial records are considered when detecting the desired SQL table schema for the data.

In the Python API, the ``table.insert_all(...)`` method can now accept a generator as well as a list of objects. This will be efficiently used to populate the table no matter how many records are produced by the generator.

The ``Database()`` constructor can now accept a ``pathlib.Path`` object in addition to a string or an existing SQLite connection object.

.. _v0_8:

0.8 (2019-01-25)
----------------

Two new commands: ``sqlite-utils csv`` and ``sqlite-utils json``

These commands execute a SQL query and return the results as CSV or JSON. See :ref:`cli_csv` and :ref:`cli_json` for more details.

::

    $ sqlite-utils json --help
    Usage: sqlite-utils json [OPTIONS] PATH SQL

      Execute SQL query and return the results as JSON

    Options:
      --nl      Output newline-delimited JSON
      --arrays  Output rows as arrays instead of objects
      --help    Show this message and exit.

    $ sqlite-utils csv --help
    Usage: sqlite-utils csv [OPTIONS] PATH SQL

      Execute SQL query and return the results as CSV

    Options:
      --no-headers  Exclude headers from CSV output
      --help        Show this message and exit.

.. _v0_7:

0.7 (2019-01-24)
----------------

This release implements the ``sqlite-utils`` command-line tool with a number of useful subcommands.

- ``sqlite-utils tables demo.db`` lists the tables in the database
- ``sqlite-utils tables demo.db --fts4`` shows just the FTS4 tables
- ``sqlite-utils tables demo.db --fts5`` shows just the FTS5 tables
- ``sqlite-utils vacuum demo.db`` runs VACUUM against the database
- ``sqlite-utils optimize demo.db`` runs OPTIMIZE against all FTS tables, then VACUUM
- ``sqlite-utils optimize demo.db --no-vacuum`` runs OPTIMIZE but skips VACUUM

The two most useful subcommands are ``upsert`` and ``insert``, which allow you to ingest JSON files with one or more records in them, creating the corresponding table with the correct columns if it does not already exist. See :ref:`cli_inserting_data` for more details.

- ``sqlite-utils insert demo.db dogs dogs.json --pk=id`` inserts new records from ``dogs.json`` into the ``dogs`` table
- ``sqlite-utils upsert demo.db dogs dogs.json --pk=id`` upserts records, replacing any records with duplicate primary keys


One backwards incompatible change: the ``db["table"].table_names`` property is now a method:

- ``db["table"].table_names()`` returns a list of table names
- ``db["table"].table_names(fts4=True)`` returns a list of just the FTS4 tables
- ``db["table"].table_names(fts5=True)`` returns a list of just the FTS5 tables

A few other changes:

- Plenty of updated documentation, including full coverage of the new command-line tool
- Allow column names to be reserved words (use correct SQL escaping)
- Added automatic column support for bytes and datetime.datetime

.. _v0_6:

0.6 (2018-08-12)
----------------

- ``.enable_fts()`` now takes optional argument ``fts_version``, defaults to ``FTS5``. Use ``FTS4`` if the version of SQLite bundled with your Python does not support FTS5
- New optional ``column_order=`` argument to ``.insert()`` and friends for providing a partial or full desired order of the columns when a database table is created
- :ref:`New documentation <python_api>` for ``.insert_all()`` and ``.upsert()`` and ``.upsert_all()``

.. _v0_5:

0.5 (2018-08-05)
----------------

- ``db.tables`` and ``db.table_names`` introspection properties
- ``db.indexes`` property for introspecting indexes
- ``table.create_index(columns, index_name)`` method
- ``db.create_view(name, sql)`` method
- Table methods can now be chained, plus added ``table.last_id`` for accessing the last inserted row ID

0.4 (2018-07-31)
----------------

- ``enable_fts()``, ``populate_fts()`` and ``search()`` table methods
