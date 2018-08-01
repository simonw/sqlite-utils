======================
 Databases and Tables
======================

Database objects are constructed by passing in a SQLite3 database connection:

.. code-block:: python

    from sqlite_utils import Database
    import sqlite3

    db = Database(sqlite3.connect("my_database.db"))

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

If you insert additioal records into the table you will need to refresh the search index using ``populate_fts()``:

.. code-block:: python

    dogs.insert({
        "id": 2,
        "name": "Marnie",
        "twitter": "MarnieTheDog",
        "age": 16,
        "is_good_dog": True,
    }, pk="id")
    dogs.populate_fts(["name", "twitter"])
