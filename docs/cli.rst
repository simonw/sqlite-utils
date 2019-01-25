.. _python_api:

================================
 sqlite-utils command-line tool
================================

The ``sqlite-utils`` command-line tool can be used to manipulate SQLite databases in a number of different ways.

Running queries and returning CSV
=================================

You can execute a SQL query against a database and get the results back as CSV like this::

    $ sqlite-utils csv docs.db "select id, title, author from documents"

This will default to including the column names as a header row. To exclude the headers, use ``--no-headers``:

    $ sqlite-utils csv docs.db "select id, title, author from documents" --no-headers

Listing tables
==============

You can list the names of tables in a database using the ``tables`` subcommand::

    $ sqlite-utils tables mydb.db
    dogs
    cats
    chickens

If you just want to see the FTS4 tables, you can use ``--fts4`` (or ``--fts5`` for FTS5 tables)::

    $ sqlite-utils tables --fts4 docs.db
    docs_fts

.. _cli_inserting_data:

Inserting data
==============

If you have data as JSON, you can use ``sqlite-utils insert tablename`` to insert it into a database. The table will be created with the correct (automatically detected) columns if it does not already exist.

You can pass in a single JSON object or a list of JSON objects, either as a filename or piped directly to standard-in (by using ``-`` as the filename).

Here's the simplest possible example::

    $ echo '{"name": "Cleo", "age": 4}' | sqlite-utils insert dogs.db dogs -

To specify a column as the primary key, use ``--pk=column_name``.

If you feed it a JSON list it will insert multiple records. For example, if ``dogs.json`` looks like this::

    [
        {
            "id": 1,
            "name": "Cleo",
            "age": 4
        },
        {
            "id": 2,
            "name": "Pancakes",
            "age": 2
        },
        {
            "id": 3,
            "name": "Toby",
            "age": 6
        }
    ]

You can import all three records into an automatically created ``dogs`` table and set the ``id`` column as the primary key like so::

    $ sqlite-utils insert dogs.db dogs dogs.json --pk=id


Upserting data
==============

Upserting works exactly like inserting, with the exception that if your data has a primary key that matches an already exsting record that record will be replaced with the new data.

After running the above ``dogs.json`` example, try running this::

    $ echo '{"id": 2, "name": "Pancakes", "age": 3}' | \
        sqlite-utils upsert dogs.db dogs - --pk=id

This will replace the record for id=2 (Pancakes) with a new record with an updated age.

Vacuum
======

You can run VACUUM to optimize your database like so::

    $ sqlite-utils vacuum mydb.db

Optimize
========

The optimize command can dramatically reduce the size of your database if you are using SQLite full-text search. It runs OPTIMIZE against all of our FTS4 and FTS5 tables, then runs VACUUM.

If you just want to run OPTIMIZE without the VACUUM, use the ``--no-vacuum`` flag.

::

    # Optimize all FTS tables and then VACUUM
    $ sqlite-utils optimize mydb.db

    # Optimize but skip the VACUUM
    $ sqlite-utils optimize --no-vacuum mydb.db
