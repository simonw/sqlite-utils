.. _python_api:

============
 Python API
============

Connecting to or creating a database
====================================

Database objects are constructed by passing in either a path to a file on disk or an existing SQLite3 database connection:

.. code-block:: python

    from sqlite_utils import Database

    db = Database("my_database.py")

This will create ``my_database.py`` if it does not already exist. You can also pass in an existing SQLite connection:

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

You can list the names of tables in a database using the ``.table_names`` property::

    >>> db.table_names
    ['dogs']

You can also iterate through the table objects themselves using ``.tables``::

    >>> db.tables
    [<Table dogs>]

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

The column types are automatically derived from the types of the incoming data.

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

Bulk inserts
============

If you have more than one record to insert, the ``insert_all()`` method is a much more efficient way of inserting them. Just like ``insert()`` it will automatically detect the columns that should be created, but it will inspect the first 100 items to help decide what those column types should be.

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

Vacuum
======

You can optimize your database by running VACUUM against it like so:

.. code-block:: python

    Database("my_database.py").vacuum()
