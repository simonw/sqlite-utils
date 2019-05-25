.. _python_api:

============
 Python API
============

Connecting to or creating a database
====================================

Database objects are constructed by passing in either a path to a file on disk or an existing SQLite3 database connection:

.. code-block:: python

    from sqlite_utils import Database

    db = Database("my_database.db")

This will create ``my_database.db`` if it does not already exist. You can also pass in an existing SQLite connection:

.. code-block:: python

    import sqlite3

    db = Database(sqlite3.connect("my_database.db"))

If you want to create an in-memory database, you con do so like this:

.. code-block:: python

    db = Database(sqlite3.connect(":memory:"))

Tables are accessed using the indexing operator, like so:

.. code-block:: python

    table = db["my_table"]

If the table does not yet exist, it will be created the first time you attempt to insert or upsert data into it.

Listing tables
==============

You can list the names of tables in a database using the ``.table_names()`` method::

    >>> db.table_names()
    ['dogs']

To see just the FTS4 tables, use ``.table_names(fts4=True)``. For FTS5, use ``.table_names(fts5=True)``.

You can also iterate through the table objects themselves using the ``.tables`` property::

    >>> db.tables
    [<Table dogs>]

.. _python_api_rows:

Listing rows
============

To iterate through dictionaries for each of the rows in a table, use ``.rows``::

    >>> db = sqlite_utils.Database("dogs.db")
    >>> for row in db["dogs"].rows:
    ...     print(row)
    {'id': 1, 'age': 4, 'name': 'Cleo'}
    {'id': 2, 'age': 2, 'name': 'Pancakes'}

Creating tables
===============

The easiest way to create a new table is to insert a record into it:

.. code-block:: python

    from sqlite_utils import Database
    import sqlite3

    db = Database(sqlite3.connect("/tmp/dogs.db"))
    dogs = db["dogs"]
    dogs.insert({
        "name": "Cleo",
        "twitter": "cleopaws",
        "age": 3,
        "is_good_dog": True,
    })

This will automatically create a new table called "dogs" with the following schema::

    CREATE TABLE dogs (
        name TEXT,
        twitter TEXT,
        age INTEGER,
        is_good_dog INTEGER
    )

You can also specify a primary key by passing the ``pk=`` parameter to the ``.insert()`` call. This will only be obeyed if the record being inserted causes the table to be created:

.. code-block:: python

    dogs.insert({
        "id": 1,
        "name": "Cleo",
        "twitter": "cleopaws",
        "age": 3,
        "is_good_dog": True,
    }, pk="id")

The order of the columns in the table will be derived from the order of the keys in the dictionary, provided you are using Python 3.6 or later.

If you want to explicitly set the order of the columns you can do so using the ``column_order=`` parameter:

.. code-block:: python

    dogs.insert({
        "id": 1,
        "name": "Cleo",
        "twitter": "cleopaws",
        "age": 3,
        "is_good_dog": True,
    }, pk="id", column_order=("id", "twitter", "name"))

You don't need to pass all of the columns to the ``column_order`` parameter. If you only pass a subset of the columns the remaining columns will be ordered based on the key order of the dictionary.

After inserting a row like this, the ``dogs.last_rowid`` property will return the SQLite ``rowid`` assigned to the most recently inserted record.

The ``dogs.last_pk`` property will return the last inserted primary key value, if you specified one. This can be very useful when writing code that creates foreign key or many-to-many relationships.

Explicitly creating a table
---------------------------

You can directly create a new table without inserting any data into it using the ``.create()`` method::

    db["cats"].create({
        "id": int,
        "name": str,
        "weight": float,
    }, pk="id")

The first argument here is a dictionary specifying the columns you would like to create. Each column is paired with a Python type indicating the type of column. See :ref:`python_api_add_column` for full details on how these types work.

This method takes optional arguments ``pk=``, ``column_order=`` and ``foreign_keys=``.

.. _python_api_foreign_keys:

Specifying foreign keys
-----------------------

Any operation that can create a table (``.create()``, ``.insert()``, ``.insert_all()``, ``.upsert()`` and ``.upsert_all()``) accepts an optional ``foreign_keys=`` argument which can be used to set up foreign key constraints for the table that is being created.

If you are using your database with `Datasette <https://datasette.readthedocs.io/>`__, Datasette will detect these constraints and use them to generate hyperlinks to associated records.

The ``foreign_keys`` argument takes a sequence of three-tuples, each one specifying the column, other table and other column that should be used to create the relationship. For example:

.. code-block:: python

    db["authors"].insert_all([
        {"id": 1, "name": "Sally"},
        {"id": 2, "name": "Asheesh"}
    ], pk="id")
    db["books"].insert_all([
        {"title": "Hedgehogs of the world", "author_id": 1},
        {"title": "How to train your wolf", "author_id": 2},
    ], foreign_keys=[
        ("author_id", "authors", "id")
    ])

Bulk inserts
============

If you have more than one record to insert, the ``insert_all()`` method is a much more efficient way of inserting them. Just like ``insert()`` it will automatically detect the columns that should be created, but it will inspect the first batch of 100 items to help decide what those column types should be.

Use it like this:

.. code-block:: python

    dogs.insert_all([{
        "id": 1,
        "name": "Cleo",
        "twitter": "cleopaws",
        "age": 3,
        "is_good_dog": True,
    }, {
        "id": 2,
        "name": "Marnie",
        "twitter": "MarnieTheDog",
        "age": 16,
        "is_good_dog": True,
    }], pk="id", column_order=("id", "twitter", "name"))

The column types used in the ``CREATE TABLE`` statement are automatically derived from the types of data in that first batch of rows. Any additional or missing columns in subsequent batches will be ignored.

The function can accept an iterator or generator of rows and will commit them according to the batch size. The default batch size is 100, but you can specify a different size using the ``batch_size`` parameter:

.. code-block:: python

    db["big_table"].insert_all(({
        "id": 1,
        "name": "Name {}".format(i),
    } for i in range(10000)), batch_size=1000)

Upserting data
==============

Upserting allows you to insert records if they do not exist and update them if they DO exist, based on matching against their primary key.

For example, given the dogs database you could upsert the record for Cleo like so:

.. code-block:: python

    dogs.upsert([{
        "id": 1,
        "name": "Cleo",
        "twitter": "cleopaws",
        "age": 4,
        "is_good_dog": True,
    }, pk="id", column_order=("id", "twitter", "name"))

If a record exists with id=1, it will be updated to match those fields. If it does not exist it will be created.

Note that the ``pk`` and ``column_order`` parameters here are optional if you are certain that the table has already been created. You should pass them if the table may not exist at the time the first upsert is performed.

An ``upsert_all()`` method is also available, which behaves like ``insert_all()`` but performs upserts instead.

.. _python_api_add_column:

Adding columns
==============

You can add a new column to a table using the ``.add_column(col_name, col_type)`` method:

.. code-block:: python

    db["dogs"].add_column("instagram", str)
    db["dogs"].add_column("weight", float)
    db["dogs"].add_column("dob", datetime.date)
    db["dogs"].add_column("image", "BLOB")
    db["dogs"].add_column("website") # str by default

You can specify the ``col_type`` argument either using a SQLite type as a string, or by directly passing a Python type e.g. ``str`` or ``float``.

The ``col_type`` is optional - if you omit it the type of ``TEXT`` will be used.

SQLite types you can specify are ``"TEXT"``, ``"INTEGER"``, ``"FLOAT"`` or ``"BLOB"``.

If you pass a Python type, it will be mapped to SQLite types as shown here::

    float: "FLOAT"
    int: "INTEGER"
    bool: "INTEGER"
    str: "TEXT"
    bytes: "BLOB"
    datetime.datetime: "TEXT"
    datetime.date: "TEXT"
    datetime.time: "TEXT"

    # If numpy is installed
    np.int8: "INTEGER"
    np.int16: "INTEGER"
    np.int32: "INTEGER"
    np.int64: "INTEGER"
    np.uint8: "INTEGER"
    np.uint16: "INTEGER"
    np.uint32: "INTEGER"
    np.uint64: "INTEGER"
    np.float16: "FLOAT"
    np.float32: "FLOAT"
    np.float64: "FLOAT"

.. _python_api_add_column_alter:

Adding columns automatically on insert/update
=============================================

You can insert or update data that includes new columns and have the table automatically altered to fit the new schema using the ``alter=True`` argument. This can be passed to all four of ``.insert()``, ``.upsert()``, ``.insert_all()``and ``.insert_all()``:

.. code-block:: python

    db["new_table"].insert({"name": "Gareth"})
    # This will throw an exception:
    db["new_table"].insert({"name": "Gareth", "age": 32})
    # This will succeed and add a new "age" integer column:
    db["new_table"].insert({"name": "Gareth", "age": 32}, alter=True)
    # You can see confirm the new column like so:
    print(db["new_table"].columns_dict)
    # Outputs this:
    # {'name': <class 'str'>, 'age': <class 'int'>}

.. _python_api_add_foreign_key:

Adding foreign key constraints
==============================

The SQLite ``ALTER TABLE`` statement doesn't have the ability to add foreign key references to an existing column.

It's possible to add these references through very careful manipulation of SQLite's ``sqlite_master`` table, using ``PRAGMA writable_schema``.

``sqlite-utils`` can do this for you, though there is a significant risk of data corruption if something goes wrong so it is advisable to create a fresh copy of your database file before attempting this.

Here's an example of this mechanism in action:

.. code-block:: python

    db["authors"].insert_all([
        {"id": 1, "name": "Sally"},
        {"id": 2, "name": "Asheesh"}
    ], pk="id")
    db["books"].insert_all([
        {"title": "Hedgehogs of the world", "author_id": 1},
        {"title": "How to train your wolf", "author_id": 2},
    ])
    db["books"].add_foreign_key("author_id", "authors", "id")

.. _python_api_hash:

Setting an ID based on the hash of the row contents
===================================================

Sometimes you will find yourself working with a dataset that includes rows that do not have a provided obvious ID, but where you would like to assign one so that you can later upsert into that table without creating duplicate records.

In these cases, a useful technique is to create an ID that is derived from the sha1 hash of the row contents.

``sqlite-utils`` can do this for you using the ``hash_id=`` option. For example::

    db = sqlite_utils.Database("dogs.db")
    db["dogs"].upsert({"name": "Cleo", "twitter": "cleopaws"}, hash_id="id")
    print(list(db["dogs]))

Outputs::

    [{'id': 'f501265970505d9825d8d9f590bfab3519fb20b1', 'name': 'Cleo', 'twitter': 'cleopaws'}]

If you are going to use that ID straight away, you can access it using ``last_pk``::

    dog_id = db["dogs"].upsert({
        "name": "Cleo",
        "twitter": "cleopaws"
    }, hash_id="id").last_pk
    # dog_id is now "f501265970505d9825d8d9f590bfab3519fb20b1"

Creating views
==============

The ``.create_view()`` method on the database class can be used to create a view:

.. code-block:: python

    db.create_view("good_dogs", """
        select * from dogs where is_good_dog = 1
    """)

Storing JSON
============

SQLite has `excellent JSON support <https://www.sqlite.org/json1.html>`_, and ``sqlite-utils`` can help you take advantage of this: if you attempt to insert a value that can be represented as a JSON list or dictionary, ``sqlite-utils`` will create TEXT column and store your data as serialized JSON. This means you can quickly store even complex data structures in SQLite and query them using JSON features.

For example:

.. code-block:: python

    db["niche_museums"].insert({
        "name": "The Bigfoot Discovery Museum",
        "url": "http://bigfootdiscoveryproject.com/"
        "hours": {
            "Monday": [11, 18],
            "Wednesday": [11, 18],
            "Thursday": [11, 18],
            "Friday": [11, 18],
            "Saturday": [11, 18],
            "Sunday": [11, 18]
        },
        "address": {
            "streetAddress": "5497 Highway 9",
            "addressLocality": "Felton, CA",
            "postalCode": "95018"
        }
    })
    db.conn.execute("""
        select json_extract(address, '$.addressLocality')
        from niche_museums
    """).fetchall()
    # Returns [('Felton, CA',)]

Introspection
=============

If you have loaded an existing table, you can use introspection to find out more about it::

    >>> db["PlantType"]
    <sqlite_utils.db.Table at 0x10f5960b8>

The ``.count`` property shows the current number of rows (``select count(*) from table``)::

    >>> db["PlantType"].count
    3
    >>> db["Street_Tree_List"].count
    189144

The ``.columns`` property shows the columns in the table::

    >>> db["PlantType"].columns
    [Column(cid=0, name='id', type='INTEGER', notnull=0, default_value=None, is_pk=1),
     Column(cid=1, name='value', type='TEXT', notnull=0, default_value=None, is_pk=0)]

The ``.columns_dict`` property returns a dictionary version of this with just the names and types::

    >>> db["PlantType"].columns_dict
    {'id': <class 'int'>, 'value': <class 'str'>}

The ``.foreign_keys`` property shows if the table has any foreign key relationships::

    >>> db["Street_Tree_List"].foreign_keys
    [ForeignKey(table='Street_Tree_List', column='qLegalStatus', other_table='qLegalStatus', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='qCareAssistant', other_table='qCareAssistant', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='qSiteInfo', other_table='qSiteInfo', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='qSpecies', other_table='qSpecies', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='qCaretaker', other_table='qCaretaker', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='PlantType', other_table='PlantType', other_column='id')]

The ``.schema`` property outputs the table's schema as a SQL string::

    >>> print(db["Street_Tree_List"].schema)
    CREATE TABLE "Street_Tree_List" (
    "TreeID" INTEGER,
      "qLegalStatus" INTEGER,
      "qSpecies" INTEGER,
      "qAddress" TEXT,
      "SiteOrder" INTEGER,
      "qSiteInfo" INTEGER,
      "PlantType" INTEGER,
      "qCaretaker" INTEGER,
      "qCareAssistant" INTEGER,
      "PlantDate" TEXT,
      "DBH" INTEGER,
      "PlotSize" TEXT,
      "PermitNotes" TEXT,
      "XCoord" REAL,
      "YCoord" REAL,
      "Latitude" REAL,
      "Longitude" REAL,
      "Location" TEXT
    ,
    FOREIGN KEY ("PlantType") REFERENCES [PlantType](id),
        FOREIGN KEY ("qCaretaker") REFERENCES [qCaretaker](id),
        FOREIGN KEY ("qSpecies") REFERENCES [qSpecies](id),
        FOREIGN KEY ("qSiteInfo") REFERENCES [qSiteInfo](id),
        FOREIGN KEY ("qCareAssistant") REFERENCES [qCareAssistant](id),
        FOREIGN KEY ("qLegalStatus") REFERENCES [qLegalStatus](id))

The ``.indexes`` property shows you all indexes created for a table::

    >>> db["Street_Tree_List"].indexes
    [Index(seq=0, name='"Street_Tree_List_qLegalStatus"', unique=0, origin='c', partial=0, columns=['qLegalStatus']),
     Index(seq=1, name='"Street_Tree_List_qCareAssistant"', unique=0, origin='c', partial=0, columns=['qCareAssistant']),
     Index(seq=2, name='"Street_Tree_List_qSiteInfo"', unique=0, origin='c', partial=0, columns=['qSiteInfo']),
     Index(seq=3, name='"Street_Tree_List_qSpecies"', unique=0, origin='c', partial=0, columns=['qSpecies']),
     Index(seq=4, name='"Street_Tree_List_qCaretaker"', unique=0, origin='c', partial=0, columns=['qCaretaker']),
     Index(seq=5, name='"Street_Tree_List_PlantType"', unique=0, origin='c', partial=0, columns=['PlantType'])]

Enabling full-text search
=========================

You can enable full-text search on a table using ``.enable_fts(columns)``:

.. code-block:: python

    dogs.enable_fts(["name", "twitter"])

You can then run searches using the ``.search()`` method:

.. code-block:: python

    rows = dogs.search("cleo")

If you insert additional records into the table you will need to refresh the search index using ``populate_fts()``:

.. code-block:: python

    dogs.insert({
        "id": 2,
        "name": "Marnie",
        "twitter": "MarnieTheDog",
        "age": 16,
        "is_good_dog": True,
    }, pk="id")
    dogs.populate_fts(["name", "twitter"])

``.enable_fts()`` defaults to using `FTS5 <https://www.sqlite.org/fts5.html>`__. If you wish to use `FTS4 <https://www.sqlite.org/fts3.html>`__ instead, use the following:

.. code-block:: python

    dogs.enable_fts(["name", "twitter"], fts_version="FTS4")

Optimizing a full-text search table
===================================

Once you have populated a FTS table you can optimize it to dramatically reduce its size like so:

.. code-block:: python

    dogs.optimize()

This runs the following SQL::

    INSERT INTO dogs_fts (dogs_fts) VALUES ("optimize");

Creating indexes
================

You can create an index on a table using the ``.create_index(columns)`` method. The method takes a list of columns:

.. code-block:: python

    dogs.create_index(["is_good_dog"])

By default the index will be named ``idx_{table-name}_{columns}`` - if you want to customize the name of the created index you can pass the ``index_name`` parameter::]

.. code-block:: python

    dogs.create_index(
        ["is_good_dog", "age"],
        index_name="good_dogs_by_age"
    )

You can create a unique index by passing ``unique=True``::

.. code-block:: python

    dogs.create_index(["name"], unique=True)

Use ``if_not_exists=True`` to do nothing if an index with that name already exists.

Vacuum
======

You can optimize your database by running VACUUM against it like so:

.. code-block:: python

    Database("my_database.db").vacuum()
