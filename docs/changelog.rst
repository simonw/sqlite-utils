===========
 Changelog
===========

.. _v2_1:

2.1 (2020-01-30)
----------------

New feature: ``conversions={...}`` can be passed to the ``.insert()`` family of functions to specify SQL conversions that should be applied to values that are being inserted or updated. See :ref:`python_api_conversions` . (`#77 <https://github.com/simonw/sqlite-utils/issues/73>`__).


.. _v2_0_1:

2.0.1 (2020-01-05)
------------------

The ``.upsert()`` and ``.upsert_all()`` methods now raise a ``sqlite_utils.db.PrimaryKeyRequired`` exception if you call them without specifying the primary key column using ``pk=`` (`#73 <https://github.com/simonw/sqlite-utils/issues/73>`__).

.. _v2:

2.0 (2019-12-29)
----------------

This release changes the behaviour of ``upsert``. It's a breaking change, hence ``2.0``.

The ``upsert`` command-line utility and the ``.upsert()`` and ``.upsert_all()`` Python API methods have had their behaviour altered. They used to completely replace the affected records: now, they update the specified values on existing records but leave other columns unaffected.

See :ref:`Upserting data using the Python API <python_api_upsert>` and :ref:`Upserting data using the CLI <cli_upsert>` for full details.

If you want the old behaviour - where records were completely replaced - you can use ``$ sqlite-utils insert ... --replace`` on the command-line and ``.insert(..., replace=True)`` and ``.insert_all(..., replace=True)`` in the Python API. See :ref:`Insert-replacing data using the Python API <python_api_insert_replace>` and :ref:`Insert-replacing data using the CLI <cli_insert_replace>` for more.

For full background on this change, see `issue #66 <https://github.com/simonw/sqlite-utils/issues/66>`__.

.. _v1_12_1:

1.12.1 (2019-11-06)
-------------------

- Fixed error thrown when ``.insert_all()`` and ``.upsert_all()`` were called with empty lists (`#52 <https://github.com/simonw/sqlite-utils/issues/52>`__)

.. _v1_12:

1.12 (2019-11-04)
-----------------

Python library utilities for deleting records (`#62 <https://github.com/simonw/sqlite-utils/issues/62>`__)

- ``db["tablename"].delete(4)`` to delete by primary key, see :ref:`python_api_delete`
- ``db["tablename"].delete_where("id > ?", [3])`` to delete by a where clause, see :ref:`python_api_delete_where`

.. _v1_11:

1.11 (2019-09-02)
-----------------

Option to create triggers to automatically keep FTS tables up-to-date with newly inserted, updated and deleted records. Thanks, Amjith Ramanujam! (`#57 <https://github.com/simonw/sqlite-utils/pull/57>`__)

- ``sqlite-utils enable-fts ... --create-triggers`` - see :ref:`Configuring full-text search using the CLI <cli_fts>`
- ``db["tablename"].enable_fts(..., create_triggers=True)`` - see :ref:`Configuring full-text search using the Python library <python_api_fts>`
- Support for introspecting triggers for a database or table - see :ref:`python_api_introspection` (`#59 <https://github.com/simonw/sqlite-utils/issues/59>`__)

.. _v1_10:

1.10 (2019-08-23)
-----------------

Ability to introspect and run queries against views (`#54 <https://github.com/simonw/sqlite-utils/issues/54>`__)

- ``db.view_names()`` method and and ``db.views`` property
- Separate ``View`` and ``Table`` classes, both subclassing new ``Queryable`` class
- ``view.drop()`` method

See :ref:`python_api_views`.

.. _v1_9:

1.9 (2019-08-04)
----------------

- ``table.m2m(...)`` method for creating many-to-many relationships: :ref:`python_api_m2m` (`#23 <https://github.com/simonw/sqlite-utils/issues/23>`__)

.. _v1_8:

1.8 (2019-07-28)
----------------

- ``table.update(pk, values)`` method: :ref:`python_api_update` (`#35 <https://github.com/simonw/sqlite-utils/issues/35>`__)

.. _v1_7_1:

1.7.1 (2019-07-28)
------------------

- Fixed bug where inserting records with 11 columns in a batch of 100 triggered a "too many SQL variables" error (`#50 <https://github.com/simonw/sqlite-utils/issues/50>`__)
- Documentation and tests for ``table.drop()`` method: :ref:`python_api_drop`

.. _v1_7:

1.7 (2019-07-24)
----------------

Support for lookup tables.

- New ``table.lookup({...})`` utility method for building and querying lookup tables - see :ref:`python_api_lookup_tables` (`#44 <https://github.com/simonw/sqlite-utils/issues/44>`__)
- New ``extracts=`` table configuration option, see :ref:`python_api_extracts` (`#46 <https://github.com/simonw/sqlite-utils/issues/46>`__)
- Use `pysqlite3 <https://github.com/coleifer/pysqlite3>`__ if it is available, otherwise use ``sqlite3`` from the standard library
- Table options can now be passed to the new ``db.table(name, **options)`` factory function in addition to being passed to ``insert_all(records, **options)`` and friends - see :ref:`python_api_table_configuration`
- In-memory databases can now be created using ``db = Database(memory=True)``

.. _v1_6:

1.6 (2019-07-18)
----------------

- ``sqlite-utils insert`` can now accept TSV data via the new ``--tsv`` option (`#41 <https://github.com/simonw/sqlite-utils/issues/41>`__)

.. _v1_5:

1.5 (2019-07-14)
----------------

- Support for compound primary keys (`#36 <https://github.com/simonw/sqlite-utils/issues/36>`__)

  - Configure these using the CLI tool by passing ``--pk`` multiple times
  - In Python, pass a tuple of columns to the ``pk=(..., ...)`` argument: :ref:`python_api_compound_primary_keys`

- New ``table.get()`` method for retrieving a record by its primary key: :ref:`python_api_get` (`#39 <https://github.com/simonw/sqlite-utils/issues/39>`__)

.. _v1_4_1:

1.4.1 (2019-07-14)
------------------

- Assorted minor documentation fixes: `changes since 1.4 <https://github.com/simonw/sqlite-utils/compare/1.4...1.4.1>`__

.. _v1_4:

1.4 (2019-06-30)
----------------

- Added ``sqlite-utils index-foreign-keys`` command (:ref:`docs <cli_index_foreign_keys>`) and ``db.index_foreign_keys()`` method (:ref:`docs <python_api_index_foreign_keys>`) (`#33 <https://github.com/simonw/sqlite-utils/issues/33>`__)

.. _v1_3:

1.3 (2019-06-28)
----------------

- New mechanism for adding multiple foreign key constraints at once: :ref:`db.add_foreign_keys() documentation <python_api_add_foreign_keys>` (`#31 <https://github.com/simonw/sqlite-utils/issues/31>`__)

.. _v1_2_2:

1.2.2 (2019-06-25)
------------------

- Fixed bug where ``datetime.time`` was not being handled correctly

.. _v1_2_1:

1.2.1 (2019-06-20)
------------------

- Check the column exists before attempting to add a foreign key (`#29 <https://github.com/simonw/sqlite-utils/issues/29>`__)

.. _v1_2:

1.2 (2019-06-12)
----------------

- Improved foreign key definitions: you no longer need to specify the ``column``, ``other_table`` AND ``other_column`` to define a foreign key - if you omit the ``other_table`` or ``other_column`` the script will attempt to guess the correct values by instrospecting the database. See :ref:`python_api_add_foreign_key` for details. (`#25 <https://github.com/simonw/sqlite-utils/issues/25>`__)
- Ability to set ``NOT NULL`` constraints and ``DEFAULT`` values when creating tables (`#24 <https://github.com/simonw/sqlite-utils/issues/24>`__). Documentation: :ref:`Setting defaults and not null constraints (Python API) <python_api_defaults_not_null>`, :ref:`Setting defaults and not null constraints (CLI) <cli_defaults_not_null>`
- Support for ``not_null_default=X`` / ``--not-null-default`` for setting a ``NOT NULL DEFAULT 'x'`` when adding a new column. Documentation: :ref:`Adding columns (Python API) <python_api_add_column>`, :ref:`Adding columns (CLI) <cli_add_column>`

.. _v1_1:

1.1 (2019-05-28)
----------------

- Support for ``ignore=True`` / ``--ignore`` for ignoring inserted records if the primary key alread exists (`#21 <https://github.com/simonw/sqlite-utils/issues/21>`__) - documentation: :ref:`Inserting data (Python API) <python_api_bulk_inserts>`, :ref:`Inserting data (CLI) <cli_inserting_data>`
- Ability to add a column that is a foreign key reference using ``fk=...`` / ``--fk`` (`#16 <https://github.com/simonw/sqlite-utils/issues/16>`__) - documentation: :ref:`Adding columns (Python API) <python_api_add_column>`, :ref:`Adding columns (CLI) <cli_add_column>`

.. _v1_0_1:

1.0.1 (2019-05-27)
------------------

- ``sqlite-utils rows data.db table --json-cols`` - fixed bug where ``--json-cols`` was not obeyed

.. _v1_0:

1.0 (2019-05-24)
----------------

- Option to automatically add new columns if you attempt to insert or upsert data with extra fields:
   ``sqlite-utils insert ... --alter`` - see :ref:`Adding columns automatically with the sqlite-utils CLI <cli_add_column_alter>`

   ``db["tablename"].insert(record, alter=True)`` - see :ref:`Adding columns automatically using the Python API <python_api_add_column_alter>`
- New ``--json-cols`` option for outputting nested JSON, see :ref:`cli_json_values`

.. _v0_14:

0.14 (2019-02-24)
-----------------

- Ability to create unique indexes: ``db["mytable"].create_index(["name"], unique=True)``
- ``db["mytable"].create_index(["name"], if_not_exists=True)``
- ``$ sqlite-utils create-index mydb.db mytable col1 [col2...]``, see :ref:`cli_create_index`
- ``table.add_column(name, type)`` method, see :ref:`python_api_add_column`
- ``$ sqlite-utils add-column mydb.db mytable nameofcolumn``, see :ref:`cli_add_column` (CLI)
- ``db["books"].add_foreign_key("author_id", "authors", "id")``, see :ref:`python_api_add_foreign_key`
- ``$ sqlite-utils add-foreign-key books.db books author_id authors id``, see :ref:`cli_add_foreign_key` (CLI)
- Improved (but backwards-incompatible) ``foreign_keys=`` argument to various methods, see :ref:`python_api_foreign_keys`

.. _v0_13:

0.13 (2019-02-23)
-----------------

- New ``--table`` and ``--fmt`` options can be used to output query results in a variety of visual table formats, see :ref:`cli_query_table`
- New ``hash_id=`` argument can now be used for :ref:`python_api_hash`
- Can now derive correct column types for numpy int, uint and float values
- ``table.last_id`` has been renamed to ``table.last_rowid``
- ``table.last_pk`` now contains the last inserted primary key, if ``pk=`` was specified
- Prettier indentation in the ``CREATE TABLE`` generated schemas

.. _v0_12:

0.12 (2019-02-22)
-----------------

- Added ``db[table].rows`` iterator - see :ref:`python_api_rows`
- Replaced ``sqlite-utils json`` and ``sqlite-utils csv`` with a new default subcommand called ``sqlite-utils query`` which defaults to JSON and takes formatting options ``--nl``, ``--csv`` and ``--no-headers`` - see :ref:`cli_query_json` and :ref:`cli_query_csv`
- New ``sqlite-utils rows data.db name-of-table`` command, see :ref:`cli_rows`
- ``sqlite-utils table`` command now takes options ``--counts`` and ``--columns`` plus the standard output format options, see :ref:`cli_tables`

.. _v0_11:

0.11 (2019-02-07)
-----------------

New commands for enabling FTS against a table and columns::

    sqlite-utils enable-fts db.db mytable col1 col2

See :ref:`cli_fts`.

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

These commands execute a SQL query and return the results as CSV or JSON. See :ref:`cli_query_csv` and :ref:`cli_query_json` for more details.

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
