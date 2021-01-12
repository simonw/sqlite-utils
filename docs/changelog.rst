===========
 Changelog
===========

.. _v3_2_1:

3.2.1 (2021-01-12)
------------------

- Fixed a bug where ``.add_missing_columns()`` failed to take case insensitive column names into account. (`#221 <https://github.com/simonw/sqlite-utils/issues/221>`__)

.. _v3_2:

3.2 (2021-01-03)
----------------

This release introduces a new mechanism for speeding up ``count(*)`` queries using cached table counts, stored in a ``_counts`` table and updated by triggers. This mechanism is described in :ref:`python_api_cached_table_counts`, and can be enabled using Python API methods or the new ``enable-counts`` CLI command. (`#212 <https://github.com/simonw/sqlite-utils/issues/212>`__)

- ``table.enable_counts()`` method for enabling these triggers on a specific table.
- ``db.enable_counts()`` method for enabling triggers on every table in the database. (`#213 <https://github.com/simonw/sqlite-utils/issues/213>`__)
- New ``sqlite-utils enable-counts my.db`` command for enabling counts on all or specific tables, see :ref:`cli_enable_counts`. (`#214 <https://github.com/simonw/sqlite-utils/issues/214>`__)
- New ``sqlite-utils triggers`` command for listing the triggers defined for a database or specific tables, see :ref:`cli_triggers`. (`#218 <https://github.com/simonw/sqlite-utils/issues/218>`__)
- New ``db.use_counts_table`` property which, if ``True``, causes ``table.count`` to read from the ``_counts`` table. (`#215 <https://github.com/simonw/sqlite-utils/issues/215>`__)
- ``table.has_counts_triggers`` property revealing if a table has been configured with the new ``_counts`` database triggers.
- ``db.reset_counts()`` method and ``sqlite-utils reset-counts`` command for resetting the values in the ``_counts`` table. (`#219 <https://github.com/simonw/sqlite-utils/issues/219>`__)
- The previously undocumented ``db.escape()`` method has been renamed to ``db.quote()`` and is now covered by the documentation: :ref:`python_api_quote`. (`#217 <https://github.com/simonw/sqlite-utils/issues/217>`__)
- New ``table.triggers_dict`` and ``db.triggers_dict`` introspection properties. (`#211 <https://github.com/simonw/sqlite-utils/issues/211>`__, `#216 <https://github.com/simonw/sqlite-utils/issues/216>`__)
- ``sqlite-utils insert`` now shows a more useful error message for invalid JSON. (`#206 <https://github.com/simonw/sqlite-utils/issues/206>`__)

.. _v3_1_1:

3.1.1 (2021-01-01)
------------------

- Fixed failing test caused by ``optimize`` sometimes creating larger database files. (`#209 <https://github.com/simonw/sqlite-utils/issues/209>`__)
- Documentation now lives on https://sqlite-utils.datasette.io/
- README now includes ``brew install sqlite-utils`` installation method.

.. _v3_1:

3.1 (2020-12-12)
----------------

- New command: ``sqlite-utils analyze-tables my.db`` outputs useful information about the table columns in the database, such as the number of distinct values and how many rows are null. See :ref:`cli_analyze_tables` for documentation. (`#207 <https://github.com/simonw/sqlite-utils/issues/207>`__)
- New ``table.analyze_column(column)`` Python method used by the ``analyze-tables`` command - see :ref:`python_api_analyze_column`.
- The ``table.update()`` method now correctly handles values that should be stored as JSON. Thanks, Andreas Madsack. (`#204 <https://github.com/simonw/sqlite-utils/pull/204>`__)

.. _v3_0:

3.0 (2020-11-08)
----------------

This release introduces a new ``sqlite-utils search`` command for searching tables, see :ref:`cli_search`. (`#192 <https://github.com/simonw/sqlite-utils/issues/192>`__)

The ``table.search()`` method has been redesigned, see :ref:`python_api_fts_search`. (`#197 <https://github.com/simonw/sqlite-utils/issues/197>`__)

The release includes minor backwards-incompatible changes, hence the version bump to 3.0. Those changes, which should not affect most users, are:

- The ``-c`` shortcut option for outputting CSV is no longer available. The full ``--csv`` option is required instead.
- The ``-f`` shortcut for ``--fmt`` has also been removed - use ``--fmt``.
- The ``table.search()`` method now defaults to sorting by relevance, not sorting by ``rowid``. (`#198 <https://github.com/simonw/sqlite-utils/issues/198>`__)
- The ``table.search()`` method now returns a generator over a list of Python dictionaries. It previously returned a list of tuples.

Also in this release:

- The ``query``, ``tables``, ``rows`` and ``search`` CLI commands now accept a new ``--tsv`` option which outputs the results in TSV. (`#193 <https://github.com/simonw/sqlite-utils/issues/193>`__)
- A new ``table.virtual_table_using`` property reveals if a table is a virtual table, and returns the upper case type of virtual table (e.g. ``FTS4`` or ``FTS5``) if it is. It returns ``None`` if the table is not a virtual table. (`#196 <https://github.com/simonw/sqlite-utils/issues/196>`__)
- The new ``table.search_sql()`` method returns the SQL for searching a table, see :ref:`python_api_fts_search_sql`.
- ``sqlite-utils rows`` now accepts multiple optional ``-c`` parameters specifying the columns to return. (`#200 <https://github.com/simonw/sqlite-utils/issues/200>`__)

Changes since the 3.0a0 alpha release:

- The ``sqlite-utils search`` command now defaults to returning every result, unless you add a ``--limit 20`` option.
- The ``sqlite-utils search -c`` and ``table.search(columns=[])`` options are now fully respected. (`#201 <https://github.com/simonw/sqlite-utils/issues/201>`__)

.. _v2_23:

2.23 (2020-10-28)
-----------------

- ``table.m2m(other_table, records)`` method now takes any iterable, not just a list or tuple. Thanks, Adam Wolf. (`#189 <https://github.com/simonw/sqlite-utils/pull/189>`__)
- ``sqlite-utils insert`` now displays a progress bar for CSV or TSV imports. (`#173 <https://github.com/simonw/sqlite-utils/issues/173>`__)
- New ``@db.register_function(deterministic=True)`` option for registering deterministic SQLite functions in Python 3.8 or higher. (`#191 <https://github.com/simonw/sqlite-utils/issues/191>`__)

.. _v2_22:

2.22 (2020-10-16)
-----------------

- New ``--encoding`` option for processing CSV and TSV files that use a non-utf-8 encoding, for both the ``insert`` and ``update`` commands. (`#182 <https://github.com/simonw/sqlite-utils/issues/182>`__)
- The ``--load-extension`` option is now available to many more commands. (`#137 <https://github.com/simonw/sqlite-utils/issues/137>`__)
- ``--load-extension=spatialite`` can be used to load SpatiaLite from common installation locations, if it is available. (`#136 <https://github.com/simonw/sqlite-utils/issues/136>`__)
- Tests now also run against Python 3.9. (`#184 <https://github.com/simonw/sqlite-utils/issues/184>`__)
- Passing ``pk=["id"]`` now has the same effect as passing ``pk="id"``. (`#181 <https://github.com/simonw/sqlite-utils/issues/181>`__)

.. _v2_21:

2.21 (2020-09-24)
-----------------

- ``table.extract()`` and ``sqlite-utils extract`` now apply much, much faster - one example operation reduced from twelve minutes to just four seconds! (`#172 <https://github.com/simonw/sqlite-utils/issues/172>`__)
- ``sqlite-utils extract`` no longer shows a progress bar, because it's fast enough not to need one.
- New ``column_order=`` option for ``table.transform()`` which can be used to alter the order of columns in a table. (`#175 <https://github.com/simonw/sqlite-utils/issues/175>`__)
- ``sqlite-utils transform --column-order=`` option (with a ``-o`` shortcut) for changing column order. (`#176 <https://github.com/simonw/sqlite-utils/issues/176>`__)
- The ``table.transform(drop_foreign_keys=)`` parameter and the ``sqlite-utils transform --drop-foreign-key`` option have changed. They now accept just the name of the column rather than requiring all three of the column, other table and other column. This is technically a backwards-incompatible change but I chose not to bump the major version number because the transform feature is so new. (`#177 <https://github.com/simonw/sqlite-utils/issues/177>`__)
- The table ``.disable_fts()``, ``.rebuild_fts()``, ``.delete()``, ``.delete_where()`` and ``.add_missing_columns()`` methods all now ``return self``, which means they can be chained together with other table operations.

.. _v2_20:

2.20 (2020-09-22)
-----------------

This release introduces two key new capabilities: **transform** (`#114 <https://github.com/simonw/sqlite-utils/issues/114>`__) and **extract** (`#42 <https://github.com/simonw/sqlite-utils/issues/42>`__).

Transform
~~~~~~~~~

SQLite's ALTER TABLE has `several documented limitations <https://sqlite.org/lang_altertable.html>`__. The ``table.transform()`` Python method and ``sqlite-utils transform`` CLI command work around these limitations using a pattern where a new table with the desired structure is created, data is copied over to it and the old table is then dropped and replaced by the new one.

You can use these tools to change column types, rename columns, drop columns, add and remove ``NOT NULL`` and defaults, remove foreign key constraints and more. See the :ref:`transforming tables (CLI) <cli_transform_table>` and :ref:`transforming tables (Python library) <python_api_transform>` documentation for full details of how to use them.

Extract
~~~~~~~

Sometimes a database table - especially one imported from a CSV file - will contain duplicate data. A ``Trees`` table may include a ``Species`` column with only a few dozen unique values, when the table itself contains thousands of rows.

The ``table.extract()`` method and ``sqlite-utils extract`` commands can extract a column - or multiple columns - out into a separate lookup table, and set up a foreign key relationship from the original table.

The Python library :ref:`extract() documentation <python_api_extract>` describes how extraction works in detail, and :ref:`cli_extract` in the CLI documentation includes a detailed example.

Other changes
~~~~~~~~~~~~~

- The ``@db.register_function`` decorator can be used to quickly register Python functions as custom SQL functions, see :ref:`python_api_register_function`. (`#162 <https://github.com/simonw/sqlite-utils/issues/162>`__)
- The ``table.rows_where()`` method now accepts an optional ``select=`` argument for specifying which columns should be selected, see :ref:`python_api_rows`.

.. _v2_19:

2.19 (2020-09-20)
-----------------

- New ``sqlite-utils add-foreign-keys`` command for :ref:`cli_add_foreign_keys`. (`#157 <https://github.com/simonw/sqlite-utils/issues/157>`__)
- New ``table.enable_fts(..., replace=True)`` argument for replacing an existing FTS table with a new configuration. (`#160 <https://github.com/simonw/sqlite-utils/issues/160>`__)
- New ``table.add_foreign_key(..., ignore=True)`` argument for ignoring a foreign key if it already exists. (`#112 <https://github.com/simonw/sqlite-utils/issues/112>`__)

.. _v2_18:

2.18 (2020-09-08)
-----------------

- ``table.rebuild_fts()`` method for rebuilding a FTS index, see :ref:`python_api_fts_rebuild`. (`#155 <https://github.com/simonw/sqlite-utils/issues/155>`__)
- ``sqlite-utils rebuild-fts data.db`` command for rebuilding FTS indexes across all tables, or just specific tables. (`#155 <https://github.com/simonw/sqlite-utils/issues/155>`__)
- ``table.optimize()`` method no longer deletes junk rows from the ``*_fts_docsize`` table. This was added in 2.17 but it turns out running ``table.rebuild_fts()`` is a better solution to this problem.
- Fixed a bug where rows with additional columns that are inserted after the first batch of records could cause an error due to breaking SQLite's maximum number of parameters. Thanks, Simon Wiles. (`#145 <https://github.com/simonw/sqlite-utils/issues/145>`__)

.. _v2_17:

2.17 (2020-09-07)
-----------------

This release handles a bug where replacing rows in FTS tables could result in growing numbers of unneccessary rows in the associated ``*_fts_docsize`` table. (`#149 <https://github.com/simonw/sqlite-utils/issues/149>`__)

- ``PRAGMA recursive_triggers=on`` by default for all connections. You can turn it off with ``Database(recursive_triggers=False)``. (`#152 <https://github.com/simonw/sqlite-utils/issues/152>`__)
- ``table.optimize()`` method now deletes unnecessary rows from the ``*_fts_docsize`` table. (`#153 <https://github.com/simonw/sqlite-utils/issues/153>`__)
- New tracer method for tracking underlying SQL queries, see :ref:`python_api_tracing`. (`#150 <https://github.com/simonw/sqlite-utils/issues/150>`__)
- Neater indentation for schema SQL. (`#148 <https://github.com/simonw/sqlite-utils/issues/148>`__)
- Documentation for ``sqlite_utils.AlterError`` exception thrown by in ``add_foreign_keys()``.

.. _v2_16_1:

2.16.1 (2020-08-28)
-------------------

- ``insert_all(..., alter=True)`` now works for columns introduced after the first 100 records. Thanks, Simon Wiles! (`#139 <https://github.com/simonw/sqlite-utils/issues/139>`__)
- Continuous Integration is now powered by GitHub Actions. (`#143 <https://github.com/simonw/sqlite-utils/issues/143>`__)

.. _v2_16:

2.16 (2020-08-21)
-----------------

- ``--load-extension`` option for ``sqlite-utils query`` for loading SQLite extensions. (`#134 <https://github.com/simonw/sqlite-utils/issues/134>`__)
- New ``sqlite_utils.utils.find_spatialite()`` function for finding SpatiaLite in common locations. (`#135 <https://github.com/simonw/sqlite-utils/issues/135>`__)

.. _v2_15_1:

2.15.1 (2020-08-12)
-------------------

- Now available as a ``sdist`` package on PyPI in addition to a wheel. (`#133 <https://github.com/simonw/sqlite-utils/issues/133>`__)

.. _v2_15:

2.15 (2020-08-10)
-----------------

- New ``db.enable_wal()`` and ``db.disable_wal()`` methods for enabling and disabling `Write-Ahead Logging <https://www.sqlite.org/wal.html>`__ for a database file - see :ref:`python_api_wal` in the Python API documentation.
- Also ``sqlite-utils enable-wal file.db`` and ``sqlite-utils disable-wal file.db`` commands for doing the same thing on the command-line, see :ref:`WAL mode (CLI) <cli_wal>`. (`#132 <https://github.com/simonw/sqlite-utils/issues/132>`__)

.. _v2_14_1:

2.14.1 (2020-08-05)
-------------------

- Documentation improvements.

.. _v2_14:

2.14 (2020-08-01)
-----------------

- The :ref:`insert-files command <cli_insert_files>` can now read from standard input: ``cat dog.jpg | sqlite-utils insert-files dogs.db pics - --name=dog.jpg``. (`#127 <https://github.com/simonw/sqlite-utils/issues/127>`__)
- You can now specify a full-text search tokenizer using the new ``tokenize=`` parameter to :ref:`enable_fts() <python_api_fts>`. This means you can enable Porter stemming on a table by running ``db["articles"].enable_fts(["headline", "body"], tokenize="porter")``. (`#130 <https://github.com/simonw/sqlite-utils/issues/130>`__)
- You can also set a custom tokenizer using the :ref:`sqlite-utils enable-fts <cli_fts>` CLI command, via the new ``--tokenize`` option.

.. _v2_13:

2.13 (2020-07-29)
-----------------

- ``memoryview`` and ``uuid.UUID`` objects are now supported. ``memoryview`` objects will be stored using ``BLOB`` and ``uuid.UUID`` objects will be stored using ``TEXT``. (`#128 <https://github.com/simonw/sqlite-utils/issues/128>`__)

.. _v2_12:

2.12 (2020-07-27)
-----------------

The theme of this release is better tools for working with binary data. The new ``insert-files`` command can be used to insert binary files directly into a database table, and other commands have been improved with better support for BLOB columns.

- ``sqlite-utils insert-files my.db gifs *.gif`` can now insert the contents of files into a specified table. The columns in the table can be customized to include different pieces of metadata derived from the files. See :ref:`cli_insert_files`. (`#122 <https://github.com/simonw/sqlite-utils/issues/122>`__)
- ``--raw`` option to ``sqlite-utils query`` - for outputting just a single raw column value - see :ref:`cli_query_raw`. (`#123 <https://github.com/simonw/sqlite-utils/issues/123>`__)
- JSON output now encodes BLOB values as special base64 obects - see :ref:`cli_query_json`. (`#125 <https://github.com/simonw/sqlite-utils/issues/125>`__)
- The same format of JSON base64 objects can now be used to insert binary data - see :ref:`cli_inserting_data`. (`#126 <https://github.com/simonw/sqlite-utils/issues/126>`__)
- The ``sqlite-utils query`` command can now accept named parameters, e.g. ``sqlite-utils :memory: "select :num * :num2" -p num 5 -p num2 6`` - see :ref:`cli_query_json`. (`#124 <https://github.com/simonw/sqlite-utils/issues/124>`__)

.. _v2_11:

2.11 (2020-07-08)
-----------------

- New ``--truncate`` option to ``sqlite-utils insert``, and ``truncate=True`` argument to ``.insert_all()``. Thanks, Thomas Sibley. (`#118 <https://github.com/simonw/sqlite-utils/pull/118>`__)
- The ``sqlite-utils query`` command now runs updates in a transaction. Thanks, Thomas Sibley. (`#120 <https://github.com/simonw/sqlite-utils/pull/120>`__)

.. _v2_10_1:

2.10.1 (2020-06-23)
-------------------

- Added documentation for the ``table.pks`` introspection property. (`#116 <https://github.com/simonw/sqlite-utils/issues/116>`__)

.. _v2_10:

2.10 (2020-06-12)
-----------------

- The ``sqlite-utils`` command now supports UPDATE/INSERT/DELETE in addition to SELECT. (`#115 <https://github.com/simonw/sqlite-utils/issues/115>`__)

.. _v2_9_1:

2.9.1 (2020-05-11)
------------------

- Added custom project links to the `PyPI listing <https://pypi.org/project/sqlite-utils/>`__.

.. _v2_9:

2.9 (2020-05-10)
----------------

- New ``sqlite-utils drop-table`` command, see :ref:`cli_drop_table`. (`#111 <https://github.com/simonw/sqlite-utils/issues/111>`__)
- New ``sqlite-utils drop-view`` command, see :ref:`cli_drop_view`.
- Python ``decimal.Decimal`` objects are now stored as ``FLOAT``. (`#110 <https://github.com/simonw/sqlite-utils/issues/110>`__)

.. _v2_8:

2.8 (2020-05-03)
----------------

- New ``sqlite-utils create-table`` command, see :ref:`cli_create_table`. (`#27 <https://github.com/simonw/sqlite-utils/issues/27>`__)
- New ``sqlite-utils create-view`` command, see :ref:`cli_create_view`. (`#107 <https://github.com/simonw/sqlite-utils/issues/107>`__)

.. _v2_7.2:

2.7.2 (2020-05-02)
------------------

- ``db.create_view(...)`` now has additional parameters ``ignore=True`` or ``replace=True``, see :ref:`python_api_create_view`. (`#106 <https://github.com/simonw/sqlite-utils/issues/106>`__)

.. _v2_7.1:

2.7.1 (2020-05-01)
------------------

- New ``sqlite-utils views my.db`` command for listing views in a database, see :ref:`cli_views`. (`#105 <https://github.com/simonw/sqlite-utils/issues/105>`__)
- ``sqlite-utils tables`` (and ``views``) has a new ``--schema`` option which outputs the table/view schema, see :ref:`cli_tables`. (`#104 <https://github.com/simonw/sqlite-utils/issues/104>`__)
- Nested structures containing invalid JSON values (e.g. Python bytestrings) are now serialized using ``repr()`` instead of throwing an error. (`#102 <https://github.com/simonw/sqlite-utils/issues/102>`__)

.. _v2_7:

2.7 (2020-04-17)
----------------

- New ``columns=`` argument for the ``.insert()``, ``.insert_all()``, ``.upsert()`` and ``.upsert_all()`` methods, for over-riding the auto-detected types for columns and specifying additional columns that should be added when the table is created. See :ref:`python_api_custom_columns`. (`#100 <https://github.com/simonw/sqlite-utils/issues/100>`__)

.. _v2_6:

2.6 (2020-04-15)
----------------

- New ``table.rows_where(..., order_by="age desc")`` argument, see :ref:`python_api_rows`. (`#76 <https://github.com/simonw/sqlite-utils/issues/76>`__)

.. _v2_5:

2.5 (2020-04-12)
----------------

- Panda's Timestamp is now stored as a SQLite TEXT column. Thanks, b0b5h4rp13! (`#96 <https://github.com/simonw/sqlite-utils/issues/96>`__)
- ``table.last_pk`` is now only available for inserts or upserts of a single record. (`#98 <https://github.com/simonw/sqlite-utils/issues/98>`__)
- New ``Database(filepath, recreate=True)`` parameter for deleting and recreating the database. (`#97 <https://github.com/simonw/sqlite-utils/issues/97>`__)

.. _v2_4_4:

2.4.4 (2020-03-23)
------------------

- Fixed bug where columns with only null values were not correctly created. (`#95 <https://github.com/simonw/sqlite-utils/issues/95>`__)

.. _v2_4_3:

2.4.3 (2020-03-23)
------------------

- Column type suggestion code is no longer confused by null values. (`#94 <https://github.com/simonw/sqlite-utils/issues/94>`__)

.. _v2_4_2:

2.4.2 (2020-03-14)
------------------

- ``table.column_dicts`` now works with all column types - previously it would throw errors on types other than ``TEXT``, ``BLOB``, ``INTEGER`` or ``FLOAT``. (`#92 <https://github.com/simonw/sqlite-utils/issues/92>`__)
- Documentation for ``NotFoundError`` thrown by ``table.get(pk)`` - see :ref:`python_api_get`.

.. _v2_4_1:

2.4.1 (2020-03-01)
------------------

- ``table.enable_fts()`` now works with columns that contain spaces. (`#90 <https://github.com/simonw/sqlite-utils/issues/90>`__)

.. _v2_4:

2.4 (2020-02-26)
----------------

- ``table.disable_fts()`` can now be used to remove FTS tables and triggers that were created using ``table.enable_fts(...)``. (`#88 <https://github.com/simonw/sqlite-utils/issues/88>`__)
- The ``sqlite-utils disable-fts`` command can be used to remove FTS tables and triggers from the command-line. (`#88 <https://github.com/simonw/sqlite-utils/issues/88>`__)
- Trying to create table columns with square braces ([ or ]) in the name now raises an error. (`#86 <https://github.com/simonw/sqlite-utils/issues/86>`__)
- Subclasses of ``dict``, ``list`` and ``tuple`` are now detected as needing a JSON column. (`#87 <https://github.com/simonw/sqlite-utils/issues/87>`__)

.. _v2_3_1:

2.3.1 (2020-02-10)
------------------

``table.create_index()`` now works for columns that contain spaces. (`#85 <https://github.com/simonw/sqlite-utils/issues/85>`__)

.. _v2_3:

2.3 (2020-02-08)
----------------

``table.exists()`` is now a method, not a property. This was not a documented part of the API before so I'm considering this a non-breaking change. (`#83 <https://github.com/simonw/sqlite-utils/issues/83>`__)

.. _v2_2_1:

2.2.1 (2020-02-06)
------------------

Fixed a bug where ``.upsert(..., hash_id="pk")`` threw an error (`#84 <https://github.com/simonw/sqlite-utils/issues/84>`__).

.. _v2_2:

2.2 (2020-02-01)
----------------

New feature: ``sqlite_utils.suggest_column_types([records])`` returns the suggested column types for a list of records. See :ref:`python_api_suggest_column_types`. (`#81 <https://github.com/simonw/sqlite-utils/issues/81>`__).

This replaces the undocumented ``table.detect_column_types()`` method.

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
