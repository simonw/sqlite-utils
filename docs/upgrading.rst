.. _upgrading:

===========
 Upgrading
===========

This page describes the changes you may need to make to your own code or scripts when upgrading between major versions of ``sqlite-utils``.

For the full list of changes in every release see the :ref:`changelog`.

.. _upgrading_3_to_4:

Upgrading from 3.x to 4.0
=========================

Requirements
------------

- Python 3.10 or higher is required.
- The ``click`` dependency must be version 8.3.1 or later.

Command-line changes
--------------------

**Type detection is now the default for CSV and TSV imports.** ``sqlite-utils insert`` and ``sqlite-utils upsert`` now detect column types when importing CSV or TSV data - previously every column was created as ``TEXT`` unless you passed ``--detect-types``. To restore the old behavior pass the new ``--no-detect-types`` flag:

.. code-block:: bash

    sqlite-utils insert data.db rows data.csv --csv --no-detect-types

Two related things have been removed:

- The ``SQLITE_UTILS_DETECT_TYPES`` environment variable.
- The old ``-d/--detect-types`` flag itself. Since detection is now the default the flag did nothing - remove it from any scripts that used it.

**The convert command no longer skips falsey values.** ``sqlite-utils convert`` previously skipped values that evaluated to ``False`` (empty strings, ``0``) unless you passed ``--no-skip-false``. All values are now converted and the ``--no-skip-false`` flag has been removed.

**drop-table and drop-view check the object type.** ``sqlite-utils drop-table`` now refuses to drop a view, and ``drop-view`` refuses to drop a table. Previously each would silently drop the wrong type of object if the name matched. If you relied on that (unlikely), use the matching command instead.

**sqlite-utils tui has moved to a plugin.** The optional terminal interface is now provided by the `sqlite-utils-tui <https://github.com/simonw/sqlite-utils-tui>`__ plugin:

.. code-block:: bash

    sqlite-utils install sqlite-utils-tui

Python API changes
------------------

**db.table() no longer returns views.** ``db.table(name)`` now raises a ``sqlite_utils.db.NoTable`` exception if ``name`` is a SQL view. Use the new ``db.view(name)`` method for views:

.. code-block:: python

    table = db.table("my_table")
    view = db.view("my_view")

``db["name"]`` still returns either a ``Table`` or a ``View`` depending on what exists in the database.

**db.query() executes immediately.** ``db.query(sql)`` previously returned a generator that did not execute the SQL until you started iterating over it. The SQL now runs as soon as the method is called - rows are still fetched lazily. Two consequences:

- Errors in your SQL now raise at the ``db.query()`` call site rather than on first iteration.
- Passing a statement that returns no rows - such as an ``INSERT`` or ``UPDATE`` without a ``RETURNING`` clause - previously did nothing at all, silently. It now raises a ``ValueError``, and the statement is rolled back so it has no effect on the database. Use ``db.execute()`` for statements that do not return rows.

**Upserts use INSERT ... ON CONFLICT.** Upsert operations now use SQLite's ``INSERT ... ON CONFLICT SET`` syntax rather than the previous ``INSERT OR IGNORE`` followed by ``UPDATE``. If your code depends on the old behavior, pass ``use_old_upsert=True`` to the ``Database()`` constructor - see :ref:`python_api_old_upsert`.

**Upsert records must include their primary keys.** ``table.upsert()`` and ``table.upsert_all()`` now raise ``sqlite_utils.db.PrimaryKeyRequired`` if a record is missing a value for any primary key column (or has ``None`` for one). Previously such records were quietly inserted as new rows. Relatedly, ``pk=`` is now optional when the table already exists with a primary key - it is detected automatically.

**Floating point columns are now REAL.** Auto-detected floating point columns are created with the correct SQLite type ``REAL`` instead of ``FLOAT``. Code that inspects column types should expect ``REAL``.

**Generated schemas use double quotes.** Tables created by this library now wrap table and column names in ``"double-quotes"`` where they previously used ``[square-braces]``. If you compare ``table.schema`` strings against expected values you will need to update them.

**table.convert() no longer skips falsey values.** Matching the CLI change above, ``table.convert()`` now converts every value. The ``skip_false`` parameter has been removed - previously it defaulted to ``True``, skipping empty strings and other falsey values.

**View.enable_fts() has been removed.** The ``View`` class previously had an ``enable_fts()`` method that existed only to raise ``NotImplementedError`` - full-text search is not supported for views. Calling it now raises ``AttributeError`` like any other missing method.

**Validation errors raise ValueError.** Invalid arguments to Python API methods - for example ``create_table()`` with no columns, or ``ignore=True`` together with ``replace=True`` - now raise ``ValueError``. They previously raised ``AssertionError`` from bare ``assert`` statements, which were silently skipped under ``python -O``.

**Transaction behavior is now well-defined.** 4.0 introduces the :ref:`db.atomic() <python_api_atomic>` context manager and uses it consistently for every write operation - the full model is described in :ref:`python_api_transactions`. Changes you may notice:

- Write statements executed with raw ``db.execute()`` calls now commit automatically, unless a transaction is already open in which case they join it. Previously they opened an implicit transaction that nothing committed - if your code used ``db.execute()`` for writes and relied on ``db.conn.rollback()`` to undo them, open an explicit transaction with the new ``db.begin()`` method first.
- Multi-step operations such as ``table.transform()`` no longer commit an existing transaction you have open - they use savepoints inside it instead.
- ``db.enable_wal()`` and ``db.disable_wal()`` raise a ``sqlite_utils.db.TransactionError`` if called while a transaction is open, instead of silently committing it.
- Using ``Database`` as a context manager (``with Database(path) as db:``) closes the connection on exit *without* committing - a transaction you explicitly opened with ``db.begin()`` and did not commit is rolled back.
- ``Database()`` rejects connections created with the Python 3.12+ ``sqlite3.connect(..., autocommit=True)`` or ``autocommit=False`` options, raising ``sqlite_utils.db.TransactionError``. On those connections every write the library made was silently discarded when the connection closed.

Packaging changes
-----------------

- ``sqlite-utils`` now uses ``pyproject.toml`` in place of ``setup.py``.
- ``pip`` is now a runtime dependency, used by the ``sqlite-utils install`` and ``uninstall`` commands.

New features to be aware of
---------------------------

Not breaking changes, but new in 4.0 and worth knowing about when you upgrade:

- A :ref:`database migrations system <migrations>`, incorporating the functionality of the ``sqlite-migrate`` plugin. If you used that plugin, the built-in system reads the same ``_sqlite_migrations`` table - your applied migrations will not run again. Update your migration files to use ``from sqlite_utils import Migrations``.
- :ref:`db.atomic() <python_api_atomic>` for nested transaction support.
- ``table.insert_all()`` and ``table.upsert_all()`` accept an iterator of lists or tuples as an alternative to dictionaries - see :ref:`python_api_insert_lists`.

.. _upgrading_2_to_3:

Upgrading from 2.x to 3.0
=========================

The 3.0 release redesigned search. The breaking changes were minor:

- ``table.search()`` returns a generator of dictionaries, sorted by relevance. It previously returned a list of tuples sorted by ``rowid``.
- The ``-c`` shortcut for ``--csv`` and the ``-f`` shortcut for ``--fmt`` were removed from the CLI - use the full option names.

.. _upgrading_1_to_2:

Upgrading from 1.x to 2.0
=========================

The 2.0 release changed the meaning of *upsert*. In 1.x, ``table.upsert()`` and ``table.upsert_all()`` actually performed ``INSERT OR REPLACE`` operations - entirely replacing the existing row. Since 2.0 an upsert updates only the columns you provide, leaving other columns untouched.

If you want the 1.x behavior, use ``table.insert(..., replace=True)`` or ``table.insert_all(..., replace=True)`` instead.
