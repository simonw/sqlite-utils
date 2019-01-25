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
