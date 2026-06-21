.. _migrations:

=====================
 Database migrations
=====================

``sqlite-utils`` includes a migration system for applying repeatable changes to SQLite database files.

A migration is a Python function that receives a :class:`sqlite_utils.Database` instance and then executes Python code to modify that database - creating or transforming tables, adding indexes, inserting rows, or any other operation suppored by SQLite.

Migrations are grouped into named sets using the :class:`sqlite_utils.Migrations` class, and each applied migration is recorded in the ``_sqlite_migrations`` table in that database.

This means you can run the migrate operation multiple times and it will only apply migrations that have not previously been recorded.

.. _migrations_define:

Defining migrations
===================

Ordered migration sets are defined by first creating a :class:`sqlite_utils.Migrations` object.

Individual migrations are Python functions that are then registered with that migration set. Each migration function is passed a single argument that is a :ref:`sqlite_utils.Database <reference_db_database>` instance.

The name passed to ``Migrations("creatures")`` identifies that set of migrations. Use a name that is unique for your project, since multiple migration sets can be applied to the same database.

Here is a simple example of a ``migrations.py`` file which creates a table, then adds an extra column to that table in a second migration:

.. code-block:: python

    from sqlite_utils import Database, Migrations

    migrations = Migrations("creatures")

    @migrations()
    def create_table(db):
        db["creatures"].create(
            {"id": int, "name": str, "species": str},
            pk="id",
        )

    @migrations()
    def add_weight(db):
        db["creatures"].add_column("weight", float)

.. _migrations_python:

Applying migrations in Python
=============================

Once you have a ``Migrations(name)`` collection with one or more migrations registered to it, you can eexcute them in Python code like this:

.. code-block:: python

    db = Database("creatures.db")
    migrations.apply(db)

Running ``migrations.apply(db)`` repeatedly is safe. Migrations that already have a matching ``migration_set`` and ``name`` row in ``_sqlite_migrations`` will be skipped.

Migration functions are applied in the order that they were registered. The function name is used as the migration name unless you pass one explicitly:

.. code-block:: python

    @migrations(name="001_create_table")
    def create_table(db):
        db["creatures"].create({"id": int, "name": str}, pk="id")

When you apply a sit of migrations you can stop part way through by specifying a ``stop_before=`` migration name:

.. code-block:: python

    migrations.apply(db, stop_before="add_weight")

Applying migrations using the CLI
=================================

Run migrations using the ``sqlite-utils migrate`` command:

.. code-block:: bash

    sqlite-utils migrate creatures.db path/to/migrations.py

The first argument is the database file. The remaining arguments can be paths to migration files or directories containing migration files.

If you omit migration paths, ``sqlite-utils`` searches the current directory and subdirectories for files called ``migrations.py``:

.. code-block:: bash

    sqlite-utils migrate creatures.db

You can also pass a directory. Every ``migrations.py`` file in that directory tree will be considered:

.. code-block:: bash

    sqlite-utils migrate creatures.db path/to/project/

Running the command repeatedly is safe. Migrations that already have a matching ``migration_set`` and ``name`` row in ``_sqlite_migrations`` will be skipped.

Listing migrations
==================

Use ``--list`` to show applied and pending migrations without running them:

.. code-block:: bash

    sqlite-utils migrate creatures.db --list

Example output:

.. code-block:: output

    Migrations for: creatures

      Applied:
        create_table - 2026-06-09 17:23:12.048092+00:00
        add_weight - 2026-06-09 17:23:12.051249+00:00

      Pending:
        add_age

Stopping before a migration
===========================

When applying a single migration file, you can stop before a named migration:

.. code-block:: bash

    sqlite-utils migrate creatures.db path/to/migrations.py --stop-before add_weight

This applies any pending migrations before ``add_weight`` and leaves ``add_weight`` and later migrations pending.

Verbose output
==============

Use ``--verbose`` or ``-v`` to show the schema before and after migrations are applied, plus a unified diff when the schema changes:

.. code-block:: bash

    sqlite-utils migrate creatures.db --verbose

Migrating from sqlite-migrate
=============================

This system uses the same migration table format as the older `sqlite-migrate <https://github.com/simonw/sqlite-migrate>`__ package. To use existing migration files directly with ``sqlite-utils``, update their import from ``sqlite_migrate`` to ``sqlite_utils``:

.. code-block:: python

    from sqlite_utils import Migrations

    migration = Migrations("creatures")

    @migration()
    def create_table(db):
        db["creatures"].create({"id": int, "name": str}, pk="id")

Python API
==========

.. autoclass:: sqlite_utils.migrations.Migrations
   :members:
   :undoc-members:
   :exclude-members: _Migration, _AppliedMigration
