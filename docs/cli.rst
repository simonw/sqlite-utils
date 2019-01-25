.. _python_api:

================================
 sqlite-utils command-line tool
================================

The ``sqlite-utils`` command-line tool can be used to manipulate SQLite databases in a number of different ways.

Listing tables
==============

You can list the names of tables in a database using the ``table_names`` subcommand::

    $ sqlite-utils table_names mydb.db
    dogs
    cats
    chickens

If you just want to see the FTS4 tables, you can use ``--fts4`` (or ``--fts5`` for FTS5 tables)::

    $ sqlite-utils table_names --fts4 docs.db
    docs_fts

Vacuum
======

You can run VACUUM to optimize your database like so::

    $ sqlite-utils vacuum mydb.db
