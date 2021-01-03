.. _cli:

================================
 sqlite-utils command-line tool
================================

The ``sqlite-utils`` command-line tool can be used to manipulate SQLite databases in a number of different ways.

.. contents:: :local:

.. _cli_query_json:

Running queries and returning JSON
==================================

You can execute a SQL query against a database and get the results back as JSON like this::

    $ sqlite-utils query dogs.db "select * from dogs"
    [{"id": 1, "age": 4, "name": "Cleo"},
     {"id": 2, "age": 2, "name": "Pancakes"}]

This is the default command for ``sqlite-utils``, so you can instead use this::

    $ sqlite-utils dogs.db "select * from dogs"

You can pass named parameters to the query using ``-p``::

    $ sqlite-utils query dogs.db "select :num * :num2" -p num 5 -p num2 6
    [{":num * :num2": 30}]

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
            "name": "transparent.gif",
            "content": {
                "$base64": true,
                "encoded": "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
            }
        }
    ]

If you execute an ``UPDATE``, ``INSERT`` or ``DELETE`` query the comand will return the number of affected rows::

    $ sqlite-utils dogs.db "update dogs set age = 5 where name = 'Cleo'"   
    [{"rows_affected": 1}]

You can run queries against a temporary in-memory database by passing ``:memory:`` as the filename::

    $ sqlite-utils :memory: "select sqlite_version()"
    [{"sqlite_version()": "3.29.0"}]

You can load SQLite extension modules using the `--load-extension` option::

    $ sqlite-utils :memory: "select spatialite_version()" --load-extension=/usr/local/lib/mod_spatialite.dylib
    [{"spatialite_version()": "4.3.0a"}]

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
    ]-

.. _cli_query_csv:

Running queries and returning CSV
=================================

You can use the ``--csv`` option to return results as CSV::

    $ sqlite-utils dogs.db "select * from dogs" --csv
    id,age,name
    1,4,Cleo
    2,2,Pancakes

This will default to including the column names as a header row. To exclude the headers, use ``--no-headers``::

    $ sqlite-utils dogs.db "select * from dogs" --csv --no-headers
    1,4,Cleo
    2,2,Pancakes

Use ``--tsv`` instead of ``--csv`` to get back tab-separated values::

    $ sqlite-utils dogs.db "select * from dogs" --tsv
    id	age	name
    1	4	Cleo
    2	2	Pancakes

.. _cli_query_table:

Running queries and outputting a table
======================================

You can use the ``--table`` option (or ``-t`` shortcut) to output query results as a table::

    $ sqlite-utils dogs.db "select * from dogs" --table
      id    age  name
    ----  -----  --------
       1      4  Cleo
       2      2  Pancakes

You can use the ``--fmt`` option to specify different table formats, for example ``rst`` for reStructuredText::

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

This command accepts the same output options as ``query`` - so you can pass ``--nl``, ``--csv``, ``--tsv``, ``--no-headers``, ``--table`` and ``--fmt``.

You can use the ``-c`` option to specify a subset of columns to return::

    $ sqlite-utils rows dogs.db dogs -c age -c name
    [{"age": 4, "name": "Cleo"},
     {"age": 2, "name": "Pancakes"}]

.. _cli_tables:

Listing tables
==============

You can list the names of tables in a database using the ``tables`` command::

    $ sqlite-utils tables mydb.db
    [{"table": "dogs"},
     {"table": "cats"},
     {"table": "chickens"}]

You can output this list in CSV using the ``--csv`` or ``--tsv`` options::

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

The ``--nl``, ``--csv``, ``--tsv`` and ``--table`` options are all available.

.. _cli_views:

Listing views
=============

The ``views`` command shows any views defined in the database::

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
* ``--tsv``
* ``--table``

.. _cli_triggers:

Listing triggers
================

The ``triggers`` command shows any triggers configured for the database::

    $ sqlite-utils triggers global-power-plants.db --table
    name             table      sql
    ---------------  ---------  -----------------------------------------------------------------
    plants_insert    plants     CREATE TRIGGER [plants_insert] AFTER INSERT ON [plants]
                                BEGIN
                                    INSERT OR REPLACE INTO [_counts]
                                    VALUES (
                                      'plants',
                                      COALESCE(
                                        (SELECT count FROM [_counts] WHERE [table] = 'plants'),
                                      0
                                      ) + 1
                                    );
                                END

It defaults to showing triggers for all tables. To see triggers for one or more specific tables pass their names as arguments::

    $ sqlite-utils triggers global-power-plants.db plants

The command takes the same format options as the ``tables`` and ``views`` commands.

.. _cli_analyze_tables:

Analyzing tables
================

When working with a new database it can be useful to get an idea of the shape of the data. The ``sqlite-utils analyze-tables`` command inspects specified tables (or all tables) and calculates some useful details about each of the columns in those tables.

To inspect the ``tags`` table in the ``github.db`` database, run the following::

    $ sqlite-utils analyze-tables github.db tags
    tags.repo: (1/3)

      Total rows: 261
      Null rows: 0
      Blank rows: 0

      Distinct values: 14

      Most common:
        88: 107914493
        75: 140912432
        27: 206156866

      Least common:
        1: 209590345
        2: 206649770
        2: 303218369

    tags.name: (2/3)

      Total rows: 261
      Null rows: 0
      Blank rows: 0

      Distinct values: 175

      Most common:
        10: 0.2
        9: 0.1
        7: 0.3

      Least common:
        1: 0.1.1
        1: 0.11.1
        1: 0.1a2

    tags.sha: (3/3)

      Total rows: 261
      Null rows: 0
      Blank rows: 0

      Distinct values: 261

For each column this tool displays the number of null rows, the number of blank rows (rows that contain an empty string), the number of distinct values and, for columns that are not entirely distinct, the most common and least common values.

If you do not specify any tables every table in the database will be analyzed::

    $ sqlite-utils analyze-tables github.db

If you wish to analyze one or more specific columns, use the ``-c`` option::

    $ sqlite-utils analyze-tables github.db tags -c sha

.. _cli_analyze_tables_save:

Saving the analyzed table details
---------------------------------

``analyze-tables`` can take quite a while to run for large database files. You can save the results of the analysis to a database table called ``_analyze_tables_`` using the ``--save`` option::

    $ sqlite-utils analyze-tables github.db --save

The ``_analyze_tables_`` table has the following schema::

    CREATE TABLE [_analyze_tables_] (
        [table] TEXT,
        [column] TEXT,
        [total_rows] INTEGER,
        [num_null] INTEGER,
        [num_blank] INTEGER,
        [num_distinct] INTEGER,
        [most_common] TEXT,
        [least_common] TEXT,
        PRIMARY KEY ([table], [column])
    );

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

You can insert binary data into a BLOB column by first encoding it using base64 and then structuring it like this::

    [
        {
            "name": "transparent.gif",
            "content": {
                "$base64": true,
                "encoded": "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
            }
        }
    ]

You can import all three records into an automatically created ``dogs`` table and set the ``id`` column as the primary key like so::

    $ sqlite-utils insert dogs.db dogs dogs.json --pk=id

You can skip inserting any records that have a primary key that already exists using ``--ignore``::

    $ sqlite-utils insert dogs.db dogs dogs.json --ignore

You can delete all the existing rows in the table before inserting the new records using ``--truncate``::

    $ sqlite-utils insert dogs.db dogs dogs.json --truncate

You can also import newline-delimited JSON using the ``--nl`` option. Since `Datasette <https://datasette.io/>`__ can export newline-delimited JSON, you can combine the two tools like so::

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

Data is expected to be encoded as Unicode UTF-8. If your data is an another character encoding you can specify it using the ``--encoding`` option::

    $ sqlite-utils insert dogs.db dogs docs.tsv --tsv --encoding=latin-1

A progress bar is displayed when inserting data from a file. You can hide the progress bar using the ``--silent`` option.

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

.. _cli_insert_files:

Inserting binary data from files
================================

SQLite ``BLOB`` columns can be used to store binary content. It can be useful to insert the contents of files into a SQLite table.

The ``insert-files`` command can be used to insert the content of files, along with their metadata.

Here's an example that inserts all of the GIF files in the current directory into a ``gifs.db`` database, placing the file contents in an ``images`` table::

    $ sqlite-utils insert-files gifs.db images *.gif

You can also pass one or more directories, in which case every file in those directories will be added recursively::

    $ sqlite-utils insert-files gifs.db images path/to/my-gifs

By default this command will create a table with the following schema::

    CREATE TABLE [images] (
        [path] TEXT PRIMARY KEY,
        [content] BLOB,
        [size] INTEGER
    );

You can customize the schema using one or more ``-c`` options. For a table schema that includes just the path, MD5 hash and last modification time of the file, you would use this::

    $ sqlite-utils insert-files gifs.db images *.gif -c path -c md5 -c mtime --pk=path

This will result in the following schema::

    CREATE TABLE [images] (
        [path] TEXT PRIMARY KEY,
        [md5] TEXT,
        [mtime] FLOAT
    );

You can change the name of one of these columns using a ``-c colname:coldef`` parameter. To rename the ``mtime`` column to ``last_modified`` you would use this::

    $ sqlite-utils insert-files gifs.db images *.gif \
        -c path -c md5 -c last_modified:mtime --pk=path

You can pass ``--replace`` or ``--upsert`` to indicate what should happen if you try to insert a file with an existing primary key. Pass ``--alter`` to cause any missing columns to be added to the table.

The full list of column definitions you can use is as follows:

``name``
    The name of the file, e.g. ``cleo.jpg``
``path``
    The path to the file relative to the root folder, e.g. ``pictures/cleo.jpg``
``fullpath``
    The fully resolved path to the image, e.g. ``/home/simonw/pictures/cleo.jpg``
``sha256``
    The SHA256 hash of the file contents
``md5``
    The MD5 hash of the file contents
``mode``
    The permission bits of the file, as an integer - you may want to convert this to octal
``content``
    The binary file contents, which will be stored as a BLOB
``mtime``
    The modification time of the file, as floating point seconds since the Unix epoch
``ctime``
    The creation time of the file, as floating point seconds since the Unix epoch
``mtime_int``
    The modification time as an integer rather than a float
``ctime_int``
    The creation time as an integer rather than a float
``mtime_iso``
    The modification time as an ISO timestamp, e.g. ``2020-07-27T04:24:06.654246``
``ctime_iso``
    The creation time is an ISO timestamp
``size``
    The integer size of the file in bytes

You can insert data piped from standard input like this::

    cat dog.jpg | sqlite-utils insert-files dogs.db pics - --name=dog.jpg

The ``-`` argument indicates data should be read from standard input. The string passed using the ``--name`` option will be used for the file name and path values.

When inserting data from standard input only the following column definitions are supported: ``name``, ``path``, ``content``, ``sha256``, ``md5`` and ``size``.

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

    $ sqlite-utils drop-table mydb.db mytable

.. _cli_transform_table:

Transforming tables
===================

The ``transform`` command allows you to apply complex transformations to a table that cannot be implemented using a regular SQLite ``ALTER TABLE`` command. See :ref:`python_api_transform` for details of how this works.

::

    $ sqlite-utils transform mydb.db mytable \
        --drop column1 \
        --rename column2 column_renamed

Every option for this table (with the exception of ``--pk-none``) can be specified multiple times. The options are as follows:

``--type column-name new-type``
    Change the type of the specified column. Valid types are ``integer``, ``text``, ``float``, ``blob``.

``--drop column-name``
    Drop the specified column.

``--rename column-name new-name``
    Rename this column to a new name.

``--column-order column``
    Use this multiple times to specify a new order for your columns. ``-o`` shortcut is also available.

``--not-null column-name``
    Set this column as ``NOT NULL``.

``--not-null-false column-name``
    For a column that is currently set as ``NOT NULL``, remove the ``NOT NULL``.

``--pk column-name``
    Change the primary key column for this table. Pass ``--pk`` multiple times if you want to create a compound primary key.

``--pk-none``
    Remove the primary key from this table, turning it into a ``rowid`` table.

``--default column-name value``
    Set the default value of this column.

``--default-none column``
    Remove the default value for this column.

``--drop-foreign-key column``
    Drop the specified foreign key.

If you want to see the SQL that will be executed to make the change without actually executing it, add the ``--sql`` flag. For example::

    $ sqlite-utils transform fixtures.db roadside_attractions \
        --rename pk id \
        --default name Untitled \
        --column-order id \
        --column-order longitude \
        --column-order latitude \
        --drop address \
        --sql
    CREATE TABLE [roadside_attractions_new_4033a60276b9] (
       [id] INTEGER PRIMARY KEY,
       [longitude] FLOAT,
       [latitude] FLOAT,
       [name] TEXT DEFAULT 'Untitled'
    );
    INSERT INTO [roadside_attractions_new_4033a60276b9] ([longitude], [latitude], [id], [name])
       SELECT [longitude], [latitude], [pk], [name] FROM [roadside_attractions];
    DROP TABLE [roadside_attractions];
    ALTER TABLE [roadside_attractions_new_4033a60276b9] RENAME TO [roadside_attractions];

.. _cli_extract:

Extracting columns into a separate table
========================================

The ``sqlite-utils extract`` command can be used to extract specified columns into a separate table.

Take a look at the Python API documentation for :ref:`python_api_extract` for a detailed description of how this works, including examples of table schemas before and after running an extraction operation.

The command takes a database, table and one or more columns that should be extracted. To extract the ``species`` column from the ``trees`` table you would run::

    $ sqlite-utils extract my.db trees species

This would produce the following schema:

.. code-block:: sql

    CREATE TABLE "trees" (
        [id] INTEGER PRIMARY KEY,
        [TreeAddress] TEXT,
        [species_id] INTEGER,
        FOREIGN KEY(species_id) REFERENCES species(id)
    )

    CREATE TABLE [species] (
        [id] INTEGER PRIMARY KEY,
        [species] TEXT
    )

The command takes the following options:

``--table TEXT``
    The name of the lookup to extract columns to. This defaults to using the name of the columns that are being extracted.

``--fk-column TEXT``
    The name of the foreign key column to add to the table. Defaults to ``columnname_id``.

``--rename <TEXT TEXT>``
    Use this option to rename the columns created in the new lookup table.

``--silent``
    Don't display the progress bar.

Here's a more complex example that makes use of these options. It converts `this CSV file <https://github.com/wri/global-power-plant-database/blob/232a666653e14d803ab02717efc01cdd437e7601/output_database/global_power_plant_database.csv>`__ full of global power plants into SQLite, then extracts the ``country`` and ``country_long`` columns into a separate ``countries`` table::

    wget 'https://github.com/wri/global-power-plant-database/blob/232a6666/output_database/global_power_plant_database.csv?raw=true'
    sqlite-utils insert global.db power_plants \
        'global_power_plant_database.csv?raw=true' --csv
    # Extract those columns:
    sqlite-utils extract global.db power_plants country country_long \
        --table countries \
        --fk-column country_id \
        --rename country_long name

After running the above, the command ``sqlite3 global.db .schema`` reveals the following schema:

.. code-block:: sql

    CREATE TABLE [countries] (
        [id] INTEGER PRIMARY KEY,
        [country] TEXT,
        [name] TEXT
    );
    CREATE UNIQUE INDEX [idx_countries_country_name]
        ON [countries] ([country], [name]);
    CREATE TABLE IF NOT EXISTS "power_plants" (
        [rowid] INTEGER PRIMARY KEY,
        [country_id] INTEGER,
        [name] TEXT,
        [gppd_idnr] TEXT,
        [capacity_mw] TEXT,
        [latitude] TEXT,
        [longitude] TEXT,
        [primary_fuel] TEXT,
        [other_fuel1] TEXT,
        [other_fuel2] TEXT,
        [other_fuel3] TEXT,
        [commissioning_year] TEXT,
        [owner] TEXT,
        [source] TEXT,
        [url] TEXT,
        [geolocation_source] TEXT,
        [wepp_id] TEXT,
        [year_of_capacity_data] TEXT,
        [generation_gwh_2013] TEXT,
        [generation_gwh_2014] TEXT,
        [generation_gwh_2015] TEXT,
        [generation_gwh_2016] TEXT,
        [generation_gwh_2017] TEXT,
        [generation_data_source] TEXT,
        [estimated_generation_gwh] TEXT,
        FOREIGN KEY(country_id) REFERENCES countries(id)
    );

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

Add ``--ignore`` to ignore an existing foreign key (as opposed to returning an error)::

    $ sqlite-utils add-foreign-key books.db books author_id --ignore

See :ref:`python_api_add_foreign_key` in the Python API documentation for further details, including how the automatic table guessing mechanism works.

.. _cli_add_foreign_keys:

Adding multiple foreign keys at once
------------------------------------

Adding a foreign key requires a ``VACUUM``. On large databases this can be an expensive operation, so if you are adding multiple foreign keys you can combine them into one operation (and hence one ``VACUUM``) using ``add-foreign-keys``::

    $ sqlite-utils add-foreign-keys books.db \
        books author_id authors id \
        authors country_id countries id

When you are using this command each foreign key needs to be defined in full, as four arguments - the table, column, other table and other column.

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

To set a custom FTS tokenizer, e.g. to enable Porter stemming, use ``--tokenize=``::

    $ sqlite-utils populate-fts mydb.db documents title summary --tokenize=porter

To remove the FTS tables and triggers you created, use ``disable-fts``::

    $ sqlite-utils disable-fts mydb.db documents

To rebuild one or more FTS tables (see :ref:`python_api_fts_rebuild`), use ``rebuild-fts``::

    $ sqlite-utils rebuild-fts mydb.db documents

You can rebuild every FTS table by running ``rebuild-fts`` without passing any table names::

    $ sqlite-utils rebuild-fts mydb.db

.. _cli_search:

Executing searches
==================

Once you have configured full-text search for a table, you can search it using ``sqlite-utils search``::

    $ sqlite-utils search mydb.db documents searchterm

This command accepts the same output options as ``sqlite-utils query``: ``--table``, ``--csv``, ``--tsv``, ``--nl`` etc.

By default it shows the most relevant matches first. You can specify a different sort order using the ``-o`` option, which can take a column or a column followed by ``desc``::

    # Sort by rowid
    $ sqlite-utils search mydb.db documents searchterm -o rowid
    # Sort by created in descending order
    $ sqlite-utils search mydb.db documents searchterm -o 'created desc'

You can specify a subset of columns to be returned using the ``-c`` option one or more times::

    $ sqlite-utils search mydb.db documents searchterm -c title -c created

By default all search results will be returned. You can use ``--limit 20`` to return just the first 20 results.

Use the ``--sql`` option to output the SQL that would be executed, rather than running the query::

    $ sqlite-utils search mydb.db documents searchterm --sql                  
    with original as (
        select
            rowid,
            *
        from [documents]
    )
    select
        [original].*
    from
        [original]
        join [documents_fts] on [original].rowid = [documents_fts].rowid
    where
        [documents_fts] match :query
    order by
        [documents_fts].rank

.. _cli_enable_counts:

Enabling cached counts
======================

``select count(*)`` queries can take a long time against large tables. ``sqlite-utils`` can speed these up by adding triggers to maintain a ``_counts`` table, see :ref:`python_api_cached_table_counts` for details.

The ``sqlite-utils enable-counts`` command can be used to configure these triggers, either for every table in the database or for specific tables.

::

    # Configure triggers for every table in the database
    $ sqlite-utils enable-counts mydb.db

    # Configure triggers just for specific tables
    $ sqlite-utils enable-counts mydb.db table1 table2

If the ``_counts`` table ever becomes out-of-sync with the actual table counts you can repair it using the ``reset-counts`` command::

    $ sqlite-utils reset-counts mydb.db

.. _cli_vacuum:

Vacuum
======

You can run VACUUM to optimize your database like so::

    $ sqlite-utils vacuum mydb.db

.. _cli_optimize:

Optimize
========

The optimize command can dramatically reduce the size of your database if you are using SQLite full-text search. It runs OPTIMIZE against all of your FTS4 and FTS5 tables, then runs VACUUM.

If you just want to run OPTIMIZE without the VACUUM, use the ``--no-vacuum`` flag.

::

    # Optimize all FTS tables and then VACUUM
    $ sqlite-utils optimize mydb.db

    # Optimize but skip the VACUUM
    $ sqlite-utils optimize --no-vacuum mydb.db

To optimize specific tables rather than every FTS table, pass those tables as extra arguments:

::

    $ sqlite-utils optimize mydb.db table_1 table_2

.. _cli_wal:

WAL mode
========

You can enable `Write-Ahead Logging <https://www.sqlite.org/wal.html>`__ for a database file using the ``enable-wal`` command::

    $ sqlite-utils enable-wal mydb.db

You can disable WAL mode using ``disable-wal``::

    $ sqlite-utils disable-wal mydb.db

Both of these commands accept one or more database files as arguments.

.. _cli_load_extension:

Loading SQLite extensions
=========================

Many of these commands have the ablity to load additional SQLite extensions using the ``--load-extension=/path/to/extension`` option - use ``--help`` to check for support, e.g. ``sqlite-utils rows --help``.

This option can be applied multiple times to load multiple extensions.

Since `SpatiaLite <https://www.gaia-gis.it/fossil/libspatialite/index>`__ is commonly used with SQLite, the value ``spatialite`` is special: it will search for SpatiaLite in the most common installation locations, saving you from needing to remember exactly where that module is located::

    $ sqlite-utils :memory: "select spatialite_version()" --load-extension=spatialite
    [{"spatialite_version()": "4.3.0a"}]
