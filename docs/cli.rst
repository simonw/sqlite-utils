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

This is the default command for ``sqlite-utils``, so you can instead use this::

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

Binary strings are not valid JSON, so BLOB columns containing binary data will be returned as a JSON object containing base64 encoded data, that looks like this::

    $ sqlite-utils dogs.db "select name, content from images" | python -mjson.tool
    [
        {
            "name": "smile.gif",
            "content": {
                "$base64": true,
                "encoded": "eJzt0c1x..."
            }
        }
    ]

If you execute an ``UPDATE``, ``INSERT`` or ``DELETE`` query the comand will return the number of affected rows::

    $ sqlite-utils dogs.db "update dogs set age = 5 where name = 'Cleo'"   
    [{"rows_affected": 1}]

You can run queries against a temporary in-memory database by passing ``:memory:`` as the filename::

    $ sqlite-utils :memory: "select sqlite_version()"
    [{"sqlite_version()": "3.29.0"}]

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

.. _cli_query_raw:

Returning raw data from a query, such as binary content
=======================================================

If your table contains binary data in a ``BLOB`` you can use the ``--raw`` option to output specific columns directly to standard out.

For example, to retrieve a binary image from a ``BLOB`` column and store it in a file you can use the following::

    $ sqlite-utils photos.db "select contents from photos where id=1" --raw > myphoto.jpg

.. _cli_rows:

Returning all rows in a table
=============================

You can return every row in a specified table using the ``rows`` command::

    $ sqlite-utils rows dogs.db dogs
    [{"id": 1, "age": 4, "name": "Cleo"},
     {"id": 2, "age": 2, "name": "Pancakes"}]

This command accepts the same output options as ``query`` - so you can pass ``--nl``, ``--csv``, ``--no-headers``, ``--table`` and ``--fmt``.

.. _cli_tables:

Listing tables
==============

You can list the names of tables in a database using the ``tables`` command::

    $ sqlite-utils tables mydb.db
    [{"table": "dogs"},
     {"table": "cats"},
     {"table": "chickens"}]

You can output this list in CSV using the ``--csv`` option::

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

Use ``--schema`` to include the schema of each table::

    $ sqlite-utils tables dogs.db --schema --table
    table    schema
    -------  -----------------------------------------------
    Gosh     CREATE TABLE Gosh (c1 text, c2 text, c3 text)
    Gosh2    CREATE TABLE Gosh2 (c1 text, c2 text, c3 text)
    dogs     CREATE TABLE [dogs] (
               [id] INTEGER,
               [age] INTEGER,
               [name] TEXT)

The ``--nl``, ``--csv`` and ``--table`` options are all available.

.. _cli_views:

Listing views
=============

The `views` command shows any views defined in the database::

    $ sqlite-utils views sf-trees.db --table --counts --columns --schema
    view         count  columns               schema
    ---------  -------  --------------------  --------------------------------------------------------------
    demo_view   189144  ['qSpecies']          CREATE VIEW demo_view AS select qSpecies from Street_Tree_List
    hello            1  ['sqlite_version()']  CREATE VIEW hello as select sqlite_version()

It takes the same options as the ``tables`` command:

* ``--columns``
* ``--schema``
* ``--counts``
* ``--nl``
* ``--csv``
* ``--table``

.. _cli_inserting_data:

Inserting JSON data
===================

If you have data as JSON, you can use ``sqlite-utils insert tablename`` to insert it into a database. The table will be created with the correct (automatically detected) columns if it does not already exist.

You can pass in a single JSON object or a list of JSON objects, either as a filename or piped directly to standard-in (by using ``-`` as the filename).

Here's the simplest possible example::

    $ echo '{"name": "Cleo", "age": 4}' | sqlite-utils insert dogs.db dogs -

To specify a column as the primary key, use ``--pk=column_name``.

To create a compound primary key across more than one column, use ``--pk`` multiple times.

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

You can delete all the existing rows in the table before inserting the new records using ``--truncate``::

    $ sqlite-utils insert dogs.db dogs dogs.json --truncate

You can also import newline-delimited JSON using the ``--nl`` option. Since `Datasette <https://datasette.readthedocs.io/>`__ can export newline-delimited JSON, you can combine the two tools like so::

    $ curl -L "https://latest.datasette.io/fixtures/facetable.json?_shape=array&_nl=on" \
        | sqlite-utils insert nl-demo.db facetable - --pk=id --nl

This also means you pipe ``sqlite-utils`` together to easily create a new SQLite database file containing the results of a SQL query against another database::

    $ sqlite-utils sf-trees.db \
        "select TreeID, qAddress, Latitude, Longitude from Street_Tree_List" --nl \
      | sqlite-utils insert saved.db trees - --nl
    # This creates saved.db with a single table called trees:
    $ sqlite-utils saved.db "select * from trees limit 5" --csv
    TreeID,qAddress,Latitude,Longitude
    141565,501X Baker St,37.7759676911831,-122.441396661871
    232565,940 Elizabeth St,37.7517102172731,-122.441498017841
    119263,495X Lakeshore Dr,,
    207368,920 Kirkham St,37.760210314285,-122.47073935813
    188702,1501 Evans Ave,37.7422086702947,-122.387293152263

Inserting CSV or TSV data
=========================

If your data is in CSV format, you can insert it using the ``--csv`` option::

    $ sqlite-utils insert dogs.db dogs docs.csv --csv

For tab-delimited data, use ``--tsv``::

    $ sqlite-utils insert dogs.db dogs docs.tsv --tsv

.. _cli_insert_replace:

Insert-replacing data
=====================

Insert-replacing works exactly like inserting, with the exception that if your data has a primary key that matches an already existing record that record will be replaced with the new data.

After running the above ``dogs.json`` example, try running this::

    $ echo '{"id": 2, "name": "Pancakes", "age": 3}' | \
        sqlite-utils insert dogs.db dogs - --pk=id --replace

This will replace the record for id=2 (Pancakes) with a new record with an updated age.

.. _cli_upsert:

Upserting data
==============

Upserting is update-or-insert. If a row exists with the specified primary key the provided columns will be updated. If no row exists that row will be created.

Unlike ``insert --replace``, an upsert will ignore any column values that exist but are not present in the upsert document.

For example::

    $ echo '{"id": 2, "age": 4}' | \
        sqlite-utils upsert dogs.db dogs - --pk=id

This will update the dog with id=2 to have an age of 4, creating a new record (with a null name) if one does not exist. If a row DOES exist the name will be left as-is.

The command will fail if you reference columns that do not exist on the table. To automatically create missing columns, use the ``--alter`` option.

.. note::
    ``upsert`` in sqlite-utils 1.x worked like ``insert ... --replace`` does in 2.x. See `issue #66 <https://github.com/simonw/sqlite-utils/issues/66>`__ for details of this change.


.. _cli_create_table:

Creating tables
===============

Most of the time creating tables by inserting example data is the quickest approach. If you need to create an empty table in advance of inserting data you can do so using the ``create-table`` command::

    $ sqlite-utils create-table mydb.db mytable id integer name text --pk=id

This will create a table called ``mytable`` with two columns - an integer ``id`` column and a text ``name`` column. It will set the ``id`` column to be the primary key.

You can pass as many column-name column-type pairs as you like. Valid types are ``integer``, ``text``, ``float`` and ``blob``.

You can specify columns that should be NOT NULL using ``--not-null colname``. You can specify default values for columns using ``--default colname defaultvalue``.

::

    $ sqlite-utils create-table mydb.db mytable \
        id integer \
        name text \
        age integer \
        is_good integer \
        --not-null name \
        --not-null age \
        --default is_good 1 \
        --pk=id

    $ sqlite-utils tables mydb.db --schema -t
    table    schema
    -------  --------------------------------
    mytable  CREATE TABLE [mytable] (
                [id] INTEGER PRIMARY KEY,
                [name] TEXT NOT NULL,
                [age] INTEGER NOT NULL,
                [is_good] INTEGER DEFAULT '1'
            )

You can specify foreign key relationships between the tables you are creating using ``--fk colname othertable othercolumn``::

    $ sqlite-utils create-table books.db authors \
        id integer \
        name text \
        --pk=id

    $ sqlite-utils create-table books.db books \
        id integer \
        title text \
        author_id integer \
        --pk=id \
        --fk author_id authors id

    $ sqlite-utils tables books.db --schema -t
    table    schema
    -------  -------------------------------------------------
    authors  CREATE TABLE [authors] (
                [id] INTEGER PRIMARY KEY,
                [name] TEXT
             )
    books    CREATE TABLE [books] (
                [id] INTEGER PRIMARY KEY,
                [title] TEXT,
                [author_id] INTEGER REFERENCES [authors]([id])
             )

If a table with the same name already exists, you will get an error. You can choose to silently ignore this error with ``--ignore``, or you can replace the existing table with a new, empty table using ``--replace``.

.. _cli_drop_table:

Dropping tables
===============

You can drop a table using the ``drop-table`` command::

    $ sqlite-utils drop-table mytable

.. _cli_create_view:

Creating views
==============

You can create a view using the ``create-view`` command::

    $ sqlite-utils create-view mydb.db version "select sqlite_version()"

    $ sqlite-utils mydb.db "select * from version"
    [{"sqlite_version()": "3.31.1"}]

Use ``--replace`` to replace an existing view of the same name, and ``--ignore`` to do nothing if a view already exists.

.. _cli_drop_view:

Dropping views
==============

You can drop a view using the ``drop-view`` command::

    $ sqlite-utils drop-view myview

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

.. _cli_index_foreign_keys:

Adding indexes for all foreign keys
-----------------------------------

If you want to ensure that every foreign key column in your database has a corresponding index, you can do so like this::

    $ sqlite-utils index-foreign-keys books.db

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

You can add an index to an existing table using the ``create-index`` command::

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

A better solution here is to use database triggers. You can set up database triggers to automatically update the full-text index using the ``--create-triggers`` option when you first run ``enable-fts``::

    $ sqlite-utils enable-fts mydb.db documents title summary --create-triggers

To remove the FTS tables and triggers you created, use ``disable-fts``::

    $ sqlite-utils disable-fts mydb.db documents

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
