.. _cli:

================================
 sqlite-utils command-line tool
================================

The ``sqlite-utils`` command-line tool can be used to manipulate SQLite databases in a number of different ways.

.. _cli_query_json:

Running queries and returning JSON
==================================

You can execute a SQL query against a database and get the results back as JSON like this::

    $ sqlite-utils query dogs.db "select * from dogs"
    [{"id": 1, "age": 4, "name": "Cleo"},
     {"id": 2, "age": 2, "name": "Pancakes"}]

This is the default subcommand for ``sqlite-utils``, so you can instead use this::

    $ sqlite-utils dogs.db "select * from dogs"

Use ``--nl`` to get back newline-delimited JSON objects::

    $ sqlite-utils dogs.db "select * from dogs" --nl
    {"id": 1, "age": 4, "name": "Cleo"}
    {"id": 2, "age": 2, "name": "Pancakes"}

You can use ``--arrays`` to request ararys instead of objects::

    $ sqlite-utils dogs.db "select * from dogs" --arrays
    [[1, 4, "Cleo"],
     [2, 2, "Pancakes"]]

You can also combine ``--arrays`` and ``--nl``::

    $ sqlite-utils dogs.db "select * from dogs" --arrays --nl 
    [1, 4, "Cleo"]
    [2, 2, "Pancakes"]

If you want to pretty-print the output further, you can pipe it through ``python -mjson.tool``::

    $ sqlite-utils dogs.db "select * from dogs" | python -mjson.tool
    [
        {
            "id": 1,
            "age": 4,
            "name": "Cleo"
        },
        {
            "id": 2,
            "age": 2,
            "name": "Pancakes"
        }
    ]

.. _cli_json_values:

Nested JSON values
------------------

If one of your columns contains JSON, by default it will be returned as an escaped string::

    $ sqlite-utils dogs.db "select * from dogs" | python -mjson.tool
    [
        {
            "id": 1,
            "name": "Cleo",
            "friends": "[{\"name\": \"Pancakes\"}, {\"name\": \"Bailey\"}]"
        }
    ]

You can use the ``--json-cols`` option to automatically detect these JSON columns and output them as nested JSON data::

    $ sqlite-utils dogs.db "select * from dogs" --json-cols | python -mjson.tool
    [
        {
            "id": 1,
            "name": "Cleo",
            "friends": [
                {
                    "name": "Pancakes"
                },
                {
                    "name": "Bailey"
                }
            ]
        }
    ]

.. _cli_query_csv:

Running queries and returning CSV
=================================

You can use the ``--csv`` option (or ``-c`` shortcut) to return results as CSV::

    $ sqlite-utils dogs.db "select * from dogs" --csv
    id,age,name
    1,4,Cleo
    2,2,Pancakes

This will default to including the column names as a header row. To exclude the headers, use ``--no-headers``::

    $ sqlite-utils dogs.db "select * from dogs" --csv --no-headers
    1,4,Cleo
    2,2,Pancakes

.. _cli_query_table:

Running queries and outputting a table
======================================

You can use the ``--table`` option (or ``-t`` shortcut) to output query results as a table::

    $ sqlite-utils dogs.db "select * from dogs" --table
      id    age  name
    ----  -----  --------
       1      4  Cleo
       2      2  Pancakes

You can use the ``--fmt`` (or ``-f``) option to specify different table formats, for example ``rst`` for reStructuredText::

    $ sqlite-utils dogs.db "select * from dogs" --table --fmt rst
    ====  =====  ========
      id    age  name
    ====  =====  ========
       1      4  Cleo
       2      2  Pancakes
    ====  =====  ========

For a full list of table format options, run ``sqlite-utils query --help``.

.. _cli_rows:

Returning all rows in a table
=============================

You can return every row in a specified table using the ``rows`` subcommand::

    $ sqlite-utils rows dogs.db dogs
    [{"id": 1, "age": 4, "name": "Cleo"},
     {"id": 2, "age": 2, "name": "Pancakes"}]

This command accepts the same output options as ``query`` - so you can pass ``--nl``, ``--csv``, ``--no-headers``, ``--table`` and ``--fmt``.

.. _cli_tables:

Listing tables
==============

You can list the names of tables in a database using the ``tables`` subcommand::

    $ sqlite-utils tables mydb.db
    [{"table": "dogs"},
     {"table": "cats"},
     {"table": "chickens"}]

You can output this list in CSV using the ``-csv`` option::

    $ sqlite-utils tables mydb.db --csv --no-headers
    dogs
    cats
    chickens

If you just want to see the FTS4 tables, you can use ``--fts4`` (or ``--fts5`` for FTS5 tables)::

    $ sqlite-utils tables docs.db --fts4
    [{"table": "docs_fts"}]

Use ``--counts`` to include a count of the number of rows in each table::

    $ sqlite-utils tables mydb.db --counts
    [{"table": "dogs", "count": 12},
     {"table": "cats", "count": 332},
     {"table": "chickens", "count": 9}]

Use ``--columns`` to include a list of columns in each table::

    $ sqlite-utils tables dogs.db --counts --columns
    [{"table": "Gosh", "count": 0, "columns": ["c1", "c2", "c3"]},
     {"table": "Gosh2", "count": 0, "columns": ["c1", "c2", "c3"]},
     {"table": "dogs", "count": 2, "columns": ["id", "age", "name"]}]

The ``--nl``, ``--csv`` and ``--table`` options are all available.

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

You can skip inserting any records that have a primary key that already exists using ``--ignore``::

    $ sqlite-utils insert dogs.db dogs dogs.json --ignore

You can also import newline-delimited JSON using the ``--nl`` option. Since `Datasette <https://datasette.readthedocs.io/>`__ can export newline-delimited JSON, you can combine the two tools like so::

    $ curl -L "https://latest.datasette.io/fixtures/facetable.json?_shape=array&_nl=on" \
        | sqlite-utils insert nl-demo.db facetable - --pk=id --nl

This also means you pipe ``sqlite-utils`` together to easily create a new SQLite database file containing the results of a SQL query against another database::

    $ sqlite-utils json sf-trees.db \
        "select TreeID, qAddress, Latitude, Longitude from Street_Tree_List" --nl \
      | sqlite-utils insert saved.db trees - --nl
    # This creates saved.db with a single table called trees:
    $ sqlite-utils csv saved.db "select * from trees limit 5"
    TreeID,qAddress,Latitude,Longitude
    141565,501X Baker St,37.7759676911831,-122.441396661871
    232565,940 Elizabeth St,37.7517102172731,-122.441498017841
    119263,495X Lakeshore Dr,,
    207368,920 Kirkham St,37.760210314285,-122.47073935813
    188702,1501 Evans Ave,37.7422086702947,-122.387293152263

Upserting data
==============

Upserting works exactly like inserting, with the exception that if your data has a primary key that matches an already exsting record that record will be replaced with the new data.

After running the above ``dogs.json`` example, try running this::

    $ echo '{"id": 2, "name": "Pancakes", "age": 3}' | \
        sqlite-utils upsert dogs.db dogs - --pk=id

This will replace the record for id=2 (Pancakes) with a new record with an updated age.

.. _cli_add_column:

Adding columns
==============

You can add a column using the ``add-column`` command::

    $ sqlite-utils add-column mydb.db mytable nameofcolumn text

The last argument here is the type of the column to be created. You can use one of ``text``, ``integer``, ``float`` or ``blob``. If you leave it off, ``text`` will be used.

You can add a column that is a foreign key reference to another table using the ``--fk`` option::

    $ sqlite-utils add-column mydb.db dogs species_id --fk species

This will automatically detect the name of the primary key on the species table and use that (and its type) for the new column.

You can explicitly specify the column you wish to reference using ``--fk-col``::

    $ sqlite-utils add-column mydb.db dogs species_id --fk species --fk-col ref

You can set a ``NOT NULL DEFAULT 'x'`` constraint on the new column using ``--not-null-default``::

    $ sqlite-utils add-column mydb.db dogs friends_count integer --not-null-default 0

.. _cli_add_column_alter:

Adding columns automatically on insert/update
=============================================

You can use the ``--alter`` option to automatically add new columns if the data you are inserting or upserting is of a different shape::

    $ sqlite-utils insert dogs.db dogs new-dogs.json --pk=id --alter

.. _cli_add_foreign_key:

Adding foreign key constraints
==============================

The ``add-foreign-key`` command can be used to add new foreign key references to an existing table - something which SQLite's ``ALTER TABLE`` command does not support.

To add a foreign key constraint pointing the ``books.author_id`` column to ``authors.id`` in another table, do this::

    $ sqlite-utils add-foreign-key books.db books author_id authors id

If you omit the other table and other column references ``sqlite-utils`` will attempt to guess them - so the above example could instead look like this::

    $ sqlite-utils add-foreign-key books.db books author_id

See :ref:`python_api_add_foreign_key` in the Python API documentation for further details, including how the automatic table guessing mechanism works.

.. _cli_defaults_not_null:

Setting defaults and not null constraints
=========================================

You can use the ``--not-null`` and ``--default`` options (to both ``insert`` and ``upsert``) to specify columns that should be ``NOT NULL`` or to set database defaults for one or more specific columns::

    $ sqlite-utils insert dogs.db dogs_with_scores dogs-with-scores.json \
        --not-null=age \
        --not-null=name \
        --default age 2 \
        --default score 5

.. _cli_create_index:

Creating indexes
================

You can add an index to an existing table using the ``create-index`` subcommand::

    $ sqlite-utils create-index mydb.db mytable col1 [col2...]

This can be used to create indexes against a single column or multiple columns.

The name of the index will be automatically derived from the table and columns. To specify a different name, use ``--name=name_of_index``.

Use the ``--unique`` option to create a unique index.

Use ``--if-not-exists`` to avoid attempting to create the index if one with that name already exists.

.. _cli_fts:

Configuring full-text search
============================

You can enable SQLite full-text search on a table and a set of columns like this::

    $ sqlite-utils enable-fts mydb.db documents title summary

This will use SQLite's FTS5 module by default. Use ``--fts4`` if you want to use FTS4::

    $ sqlite-utils enable-fts mydb.db documents title summary --fts4

The ``enable-fts`` command will populate the new index with all existing documents. If you later add more documents you will need to use ``populate-fts`` to cause them to be indexed as well::

    $ sqlite-utils populate-fts mydb.db documents title summary

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
