.. _upgrading:

=====================
 Upgrading to 4.0
=====================

sqlite-utils 4.0 includes several breaking changes. This page describes what has changed and how to update your code.

Python library changes
======================

db["name"] only returns tables
------------------------------

In previous versions, ``db["table_or_view_name"]`` would return either a :ref:`Table <reference_db_table>` or :ref:`View <reference_db_view>` object depending on what existed in the database.

In 4.0, this syntax **only returns Table objects**. Attempting to use it with a view name will raise a ``sqlite_utils.db.NoTable`` exception.

**Before (3.x):**

.. code-block:: python

    # This could return either a Table or View
    obj = db["my_view"]
    obj.drop()

**After (4.0):**

.. code-block:: python

    # Use db.view() explicitly for views
    view = db.view("my_view")
    view.drop()

    # db["name"] now only works with tables
    table = db["my_table"]

This change improves type safety since views lack methods like ``.insert()`` that are available on tables.

db.table() raises NoTable for views
-----------------------------------

The ``db.table(name)`` method now raises ``sqlite_utils.db.NoTable`` if the name refers to a view. Use ``db.view(name)`` instead.

Default floating point type is REAL
-----------------------------------

When inserting data with auto-detected column types, floating point values now create columns with type ``REAL`` instead of ``FLOAT``. ``REAL`` is the correct SQLite affinity for floating point values.

This affects the schema of newly created tables but does not change how data is stored or queried.

convert() no longer skips False values
--------------------------------------

The ``table.convert()`` method previously skipped rows where the column value evaluated to ``False`` (including ``0``, empty strings, and ``None``). This behavior has been removed.

**Before (3.x):**

.. code-block:: python

    # Rows with falsey values were skipped by default
    # --skip-false was needed to process all rows
    table.convert("column", lambda x: x.upper(), skip_false=False)

**After (4.0):**

.. code-block:: python

    # All rows are now processed, including those with falsey values
    table.convert("column", lambda x: x.upper() if x else x)

Table schemas use double quotes
-------------------------------

Tables created by sqlite-utils now use ``"double-quotes"`` for table and column names in the schema instead of ``[square-braces]``. Both are valid SQL, but double quotes are the SQL standard.

This only affects how the schema is written. Existing tables are not modified.

Upsert uses modern SQLite syntax
--------------------------------

Upsert operations now use SQLite's ``INSERT ... ON CONFLICT SET`` syntax on SQLite versions 3.24.0 and later. The previous implementation used ``INSERT OR IGNORE`` followed by ``UPDATE``.

To use the old behavior, pass ``use_old_upsert=True`` to the ``Database()`` constructor:

.. code-block:: python

    db = Database("my.db", use_old_upsert=True)

CLI changes
===========

Type detection is now the default
---------------------------------

When importing CSV or TSV data with the ``insert`` or ``upsert`` commands, sqlite-utils now automatically detects column types. Previously all columns were treated as ``TEXT`` unless ``--detect-types`` was passed.

**Before (3.x):**

.. code-block:: bash

    # Types were detected only with --detect-types
    sqlite-utils insert data.db mytable data.csv --csv --detect-types

**After (4.0):**

.. code-block:: bash

    # Types are detected by default
    sqlite-utils insert data.db mytable data.csv --csv

    # Use --no-detect-types to treat all columns as TEXT
    sqlite-utils insert data.db mytable data.csv --csv --no-detect-types

The ``SQLITE_UTILS_DETECT_TYPES`` environment variable has been removed.

convert --skip-false removed
----------------------------

The ``--skip-false`` option for ``sqlite-utils convert`` has been removed. All rows are now processed regardless of whether the column value is falsey.

sqlite-utils tui is now a plugin
--------------------------------

The ``sqlite-utils tui`` command has been moved to a separate plugin. Install it with:

.. code-block:: bash

    sqlite-utils install sqlite-utils-tui

Python version requirements
===========================

sqlite-utils 4.0 requires Python 3.10 or higher. Python 3.8 and 3.9 are no longer supported.
