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

This will create ``my_database.db`` if it does not already exist.

If you want to recreate a database from scratch (first removing the existing file from disk if it already exists) you can use the ``recreate=True`` argument:

.. code-block:: python

    db = Database("my_database.db", recreate=True)

Instead of a file path you can pass in an existing SQLite connection:

.. code-block:: python

    import sqlite3

    db = Database(sqlite3.connect("my_database.db"))

If you want to create an in-memory database, you can do so like this:

.. code-block:: python

    db = Database(memory=True)

Connections use ``PRAGMA recursive_triggers=on`` by default. If you don't want to use `recursive triggers <https://www.sqlite.org/pragma.html#pragma_recursive_triggers>`__ you can turn them off using:

.. code-block:: python

    db = Database(memory=True, recursive_triggers=False)

.. _python_api_tracing:

Tracing queries
---------------

You can use the ``tracer`` mechanism to see SQL queries that are being executed by SQLite. A tracer is a function that you provide which will be called with ``sql`` and ``params`` arguments every time SQL is executed, for example:

.. code-block:: python

    def tracer(sql, params):
        print("SQL: {} - params: {}".format(sql, params))

You can pass this function to the ``Database()`` constructor like so:

.. code-block:: python

    db = Database(memory=True, tracer=tracer)

You can also turn on a tracer function temporarily for a block of code using the ``with db.tracer(...)`` context manager:

.. code-block:: python

    db = Database(memory=True)
    # ... later
    with db.tracer(tracer):
        db["dogs"].insert({"name": "Cleo"})

Queries will be passed to your ``tracer()`` function only for the duration of the ``with`` block.

.. _python_api_execute:

Executing queries
=================

The ``db.execute()`` and ``db.executescript()`` methods provide wrappers around ``.execute()`` and ``.executescript()`` on the underlying SQLite connection. These wrappers log to the tracer function if one has been registered.

.. code-block:: python

    db = Database(memory=True)
    db["dogs"].insert({"name": "Cleo"})
    db.execute("update dogs set name = 'Cleopaws'")

.. _python_api_table:

Accessing tables
================

Tables are accessed using the indexing operator, like so:

.. code-block:: python

    table = db["my_table"]

If the table does not yet exist, it will be created the first time you attempt to insert or upsert data into it.

You can also access tables using the ``.table()`` method like so:

.. code-block:: python

    table = db.table("my_table")

Using this factory function allows you to set :ref:`python_api_table_configuration`.

.. _python_api_tables:

Listing tables
==============

You can list the names of tables in a database using the ``.table_names()`` method::

    >>> db.table_names()
    ['dogs']

To see just the FTS4 tables, use ``.table_names(fts4=True)``. For FTS5, use ``.table_names(fts5=True)``.

You can also iterate through the table objects themselves using the ``.tables`` property::

    >>> db.tables
    [<Table dogs>]

.. _python_api_views:

Listing views
=============

``.view_names()`` shows you a list of views in the database::

    >>> db.view_names()
    ['good_dogs']

You can iterate through view objects using the ``.views`` property::

    >>> db.views
    [<View good_dogs>]

View objects are similar to Table objects, except that any attempts to insert or update data will throw an error. The full list of methods and properties available on a view object is as follows:

* ``columns``
* ``columns_dict``
* ``count``
* ``schema``
* ``rows``
* ``rows_where(where, where_args, order_by)``
* ``drop()``

.. _python_api_rows:

Listing rows
============

To iterate through dictionaries for each of the rows in a table, use ``.rows``::

    >>> db = sqlite_utils.Database("dogs.db")
    >>> for row in db["dogs"].rows:
    ...     print(row)
    {'id': 1, 'age': 4, 'name': 'Cleo'}
    {'id': 2, 'age': 2, 'name': 'Pancakes'}

You can filter rows by a WHERE clause using ``.rows_where(where, where_args)``::

    >>> db = sqlite_utils.Database("dogs.db")
    >>> for row in db["dogs"].rows_where("age > ?", [3]):
    ...     print(row)
    {'id': 1, 'age': 4, 'name': 'Cleo'}

To specify an order, use the ``order_by=`` argument::

    >>> for row in db["dogs"].rows_where("age > 1", order_by="age"):
    ...     print(row)
    {'id': 2, 'age': 2, 'name': 'Pancakes'}
    {'id': 1, 'age': 4, 'name': 'Cleo'}

You can use ``order_by="age desc"`` for descending order.

You can order all records in the table by excluding the ``where`` argument::

    >>> for row in db["dogs"].rows_where(order_by="age desc"):
    ...     print(row)
    {'id': 1, 'age': 4, 'name': 'Cleo'}
    {'id': 2, 'age': 2, 'name': 'Pancakes'}

.. _python_api_get:

Retrieving a specific record
============================

You can retrieve a record by its primary key using ``table.get()``::

    >>> db = sqlite_utils.Database("dogs.db")
    >>> print(db["dogs"].get(1))
    {'id': 1, 'age': 4, 'name': 'Cleo'}

If the table has a compound primary key you can pass in the primary key values as a tuple::

    >>> db["compound_dogs"].get(("mixed", 3))

If the record does not exist a ``NotFoundError`` will be raised:

.. code-block:: python

    from sqlite_utils.db import NotFoundError

    try:
        row = db["dogs"].get(5)
    except NotFoundError:
        print("Dog not found")

.. _python_api_creating_tables:

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

After inserting a row like this, the ``dogs.last_rowid`` property will return the SQLite ``rowid`` assigned to the most recently inserted record.

The ``dogs.last_pk`` property will return the last inserted primary key value, if you specified one. This can be very useful when writing code that creates foreign keys or many-to-many relationships.

.. _python_api_custom_columns:

Custom column order and column types
------------------------------------

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

Column types are detected based on the example data provided. Sometimes you may find you need to over-ride these detected types - to create an integer column for data that was provided as a string for example, or to ensure that a table where the first example was ``None`` is created as an ``INTEGER`` rather than a ``TEXT`` column. You can do this using the ``columns=`` parameter:

.. code-block:: python

    dogs.insert({
        "id": 1,
        "name": "Cleo",
        "age": "5",
    }, pk="id", columns={"age": int, "weight": float})

This will create a table with the following schema:

.. code-block:: sql

    CREATE TABLE [dogs] (
        [id] INTEGER PRIMARY KEY,
        [name] TEXT,
        [age] INTEGER,
        [weight] FLOAT
    )

.. _python_api_explicit_create:

Explicitly creating a table
---------------------------

You can directly create a new table without inserting any data into it using the ``.create()`` method::

    db["cats"].create({
        "id": int,
        "name": str,
        "weight": float,
    }, pk="id")

The first argument here is a dictionary specifying the columns you would like to create. Each column is paired with a Python type indicating the type of column. See :ref:`python_api_add_column` for full details on how these types work.

This method takes optional arguments ``pk=``, ``column_order=``, ``foreign_keys=``, ``not_null=set()`` and ``defaults=dict()`` - explained below.

.. _python_api_compound_primary_keys:

Compound primary keys
---------------------

If you want to create a table with a compound primary key that spans multiple columns, you can do so by passing a tuple of column names to any of the methods that accept a ``pk=`` parameter. For example:

.. code-block:: python

    db["cats"].create({
        "id": int,
        "breed": str,
        "name": str,
        "weight": float,
    }, pk=("breed", "id"))

This also works for the ``.insert()``, ``.insert_all()``, ``.upsert()`` and ``.upsert_all()`` methods.

.. _python_api_foreign_keys:

Specifying foreign keys
-----------------------

Any operation that can create a table (``.create()``, ``.insert()``, ``.insert_all()``, ``.upsert()`` and ``.upsert_all()``) accepts an optional ``foreign_keys=`` argument which can be used to set up foreign key constraints for the table that is being created.

If you are using your database with `Datasette <https://datasette.readthedocs.io/>`__, Datasette will detect these constraints and use them to generate hyperlinks to associated records.

The ``foreign_keys`` argument takes a list that indicates which foreign keys should be created. The list can take several forms. The simplest is a list of columns:

.. code-block:: python

    foreign_keys=["author_id"]

The library will guess which tables you wish to reference based on the column names using the rules described in :ref:`python_api_add_foreign_key`.

You can also be more explicit, by passing in a list of tuples:

.. code-block:: python

    foreign_keys=[
        ("author_id", "authors", "id")
    ]

This means that the ``author_id`` column should be a foreign key that references the ``id`` column in the ``authors`` table.

You can leave off the third item in the tuple to have the referenced column automatically set to the primary key of that table. A full example:

.. code-block:: python

    db["authors"].insert_all([
        {"id": 1, "name": "Sally"},
        {"id": 2, "name": "Asheesh"}
    ], pk="id")
    db["books"].insert_all([
        {"title": "Hedgehogs of the world", "author_id": 1},
        {"title": "How to train your wolf", "author_id": 2},
    ], foreign_keys=[
        ("author_id", "authors")
    ])

.. _python_api_table_configuration:

Table configuration options
===========================

The ``.insert()``, ``.upsert()``, ``.insert_all()`` and ``.upsert_all()`` methods each take a number of keyword arguments, some of which influence what happens should they cause a table to be created and some of which affect the behavior of those methods.

You can set default values for these methods by accessing the table through the ``db.table(...)`` method (instead of using ``db["table_name"]``), like so:

.. code-block:: python

    table = db.table(
        "authors",
        pk="id",
        not_null={"name", "score"},
        column_order=("id", "name", "score", "url")
    )
    # Now you can call .insert() like so:
    table.insert({"id": 1, "name": "Tracy", "score": 5})

The configuration options that can be specified in this way are ``pk``, ``foreign_keys``, ``column_order``, ``not_null``, ``defaults``, ``batch_size``, ``hash_id``, ``alter``, ``ignore``, ``replace``, ``extracts``, ``conversions``, ``columns``. These are all documented below.

.. _python_api_defaults_not_null:

Setting defaults and not null constraints
=========================================

Each of the methods that can cause a table to be created take optional arguments ``not_null=set()`` and ``defaults=dict()``. The methods that take these optional arguments are:

* ``db.create_table(...)``
* ``table.create(...)``
* ``table.insert(...)``
* ``table.insert_all(...)``
* ``table.upsert(...)``
* ``table.upsert_all(...)``

You can use ``not_null=`` to pass a set of column names that should have a ``NOT NULL`` constraint set on them when they are created.

You can use ``defaults=`` to pass a dictionary mapping columns to the default value that should be specified in the ``CREATE TABLE`` statement.

Here's an example that uses these features:

.. code-block:: python

    db["authors"].insert_all(
        [{"id": 1, "name": "Sally", "score": 2}],
        pk="id",
        not_null={"name", "score"},
        defaults={"score": 1},
    )
    db["authors"].insert({"name": "Dharma"})

    list(db["authors"].rows)
    # Outputs:
    # [{'id': 1, 'name': 'Sally', 'score': 2},
    #  {'id': 3, 'name': 'Dharma', 'score': 1}]
    print(db["authors"].schema)                                                                                                                    # Outputs:
    # CREATE TABLE [authors] (
    #     [id] INTEGER PRIMARY KEY,
    #     [name] TEXT NOT NULL,
    #     [score] INTEGER NOT NULL DEFAULT 1
    # )

.. _python_api_bulk_inserts:

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

The column types used in the ``CREATE TABLE`` statement are automatically derived from the types of data in that first batch of rows. Any additional columns in subsequent batches will cause a ``sqlite3.OperationalError`` exception to be raised unless the ``alter=True`` argument is supplied, in which case the new columns will be created.

The function can accept an iterator or generator of rows and will commit them according to the batch size. The default batch size is 100, but you can specify a different size using the ``batch_size`` parameter:

.. code-block:: python

    db["big_table"].insert_all(({
        "id": 1,
        "name": "Name {}".format(i),
    } for i in range(10000)), batch_size=1000)

You can skip inserting any records that have a primary key that already exists using ``ignore=True``. This works with both ``.insert({...}, ignore=True)`` and ``.insert_all([...], ignore=True)``.

You can delete all the existing rows in the table before inserting the new
records using ``truncate=True``. This is useful if you want to replace the data in the table.

.. _python_api_insert_replace:

Insert-replacing data
=====================

If you want to insert a record or replace an existing record with the same primary key, using the ``replace=True`` argument to ``.insert()`` or ``.insert_all()``::

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
    }], pk="id", replace=True)

.. note::
    Prior to sqlite-utils 2.x the ``.upsert()`` and ``.upsert_all()`` methods did this. See :ref:`python_api_upsert` for the new behaviour of those methods in 2.x.

.. _python_api_update:

Updating a specific record
==========================

You can update a record by its primary key using ``table.update()``::

    >>> db = sqlite_utils.Database("dogs.db")
    >>> print(db["dogs"].get(1))
    {'id': 1, 'age': 4, 'name': 'Cleo'}
    >>> db["dogs"].update(1, {"age": 5})
    >>> print(db["dogs"].get(1))
    {'id': 1, 'age': 5, 'name': 'Cleo'}

The first argument to ``update()`` is the primary key. This can be a single value, or a tuple if that table has a compound primary key::

    >>> db["compound_dogs"].update((5, 3), {"name": "Updated"})

The second argument is a dictonary of columns that should be updated, along with their new values.

You can cause any missing columns to be added automatically using ``alter=True``::

    >>> db["dogs"].update(1, {"breed": "Mutt"}, alter=True)

.. _python_api_delete:

Deleting a specific record
==========================

You can delete a record using ``table.delete()``::

    >>> db = sqlite_utils.Database("dogs.db")
    >>> db["dogs"].delete(1)

The ``delete()`` method takes the primary key of the record. This can be a tuple of values if the row has a compound primary key::

    >>> db["compound_dogs"].delete((5, 3))

.. _python_api_delete_where:

Deleting multiple records
=========================

You can delete all records in a table that match a specific WHERE statement using ``table.delete_where()``::

    >>> db = sqlite_utils.Database("dogs.db")
    >>> # Delete every dog with age less than 3
    >>> db["dogs"].delete_where("age < ?", [3]):

Calling ``table.delete_where()`` with no other arguments will delete every row in the table.

.. _python_api_upsert:

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

Any existing columns that are not referenced in the dictionary passed to ``.upsert()`` will be unchanged. If you want to replace a record entirely, use ``.insert(doc, replace=True)`` instead.

Note that the ``pk`` and ``column_order`` parameters here are optional if you are certain that the table has already been created. You should pass them if the table may not exist at the time the first upsert is performed.

An ``upsert_all()`` method is also available, which behaves like ``insert_all()`` but performs upserts instead.

.. note::
    ``.upsert()`` and ``.upsert_all()`` in sqlite-utils 1.x worked like ``.insert(..., replace=True)`` and ``.insert_all(..., replace=True)`` do in 2.x. See `issue #66 <https://github.com/simonw/sqlite-utils/issues/66>`__ for details of this change.

.. _python_api_lookup_tables:

Working with lookup tables
==========================

A useful pattern when populating large tables in to break common values out into lookup tables. Consider a table of ``Trees``, where each tree has a species. Ideally these species would be split out into a separate ``Species`` table, with each one assigned an integer primary key that can be referenced from the ``Trees`` table ``species_id`` column.

.. _python_api_explicit_lookup_tables:

Creating lookup tables explicitly
---------------------------------

Calling ``db["Species"].lookup({"name": "Palm"})`` creates a table called ``Species`` (if one does not already exist) with two columns: ``id`` and ``name``. It sets up a unique constraint on the ``name`` column to guarantee it will not contain duplicate rows. It then inserts a new row with the ``name`` set to ``Palm`` and returns the new integer primary key value.

If the ``Species`` table already exists, it will insert the new row and return the primary key. If a row with that ``name`` already exists, it will return the corresponding primary key value directly.

If you call ``.lookup()`` against an existing table without the unique constraint it will attempt to add the constraint, raising an ``IntegrityError`` if the constraint cannot be created.

If you pass in a dictionary with multiple values, both values will be used to insert or retrieve the corresponding ID and any unique constraint that is created will cover all of those columns, for example:

.. code-block:: python

    db["Trees"].insert({
        "latitude": 49.1265976,
        "longitude": 2.5496218,
        "species": db["Species"].lookup({
            "common_name": "Common Juniper",
            "latin_name": "Juniperus communis"
        })
    })

.. _python_api_extracts:

Populating lookup tables automatically during insert/upsert
-----------------------------------------------------------

A more efficient way to work with lookup tables is to define them using the ``extracts=`` parameter, which is accepted by ``.insert()``, ``.upsert()``, ``.insert_all()``, ``.upsert_all()`` and by the ``.table(...)`` factory function.

``extracts=`` specifies columns which should be "extracted" out into a separate lookup table during the data insertion.

It can be either a list of column names, in which case the extracted table names will match the column names exactly, or it can be a dictionary mapping column names to the desired name of the extracted table.

To extract the ``species`` column out to a separate ``Species`` table, you can do this:

.. code-block:: python

    # Using the table factory
    trees = db.table("Trees", extracts={"species": "Species"})
    trees.insert({
        "latitude": 49.1265976,
        "longitude": 2.5496218,
        "species": "Common Juniper"
    })

    # If you want the table to be called 'species', you can do this:
    trees = db.table("Trees", extracts=["species"])

    # Using .insert() directly
    db["Trees"].insert({
        "latitude": 49.1265976,
        "longitude": 2.5496218,
        "species": "Common Juniper"
    }, extracts={"species": "Species"})

.. _python_api_m2m:

Working with many-to-many relationships
=======================================

``sqlite-utils`` includes a shortcut for creating records using many-to-many relationships in the form of the ``table.m2m(...)`` method.

Here's how to create two new records and connect them via a many-to-many table in a single line of code:

.. code-block:: python

    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id").m2m(
        "humans", {"id": 1, "name": "Natalie"}, pk="id"
    )

Running this example actually creates three tables: ``dogs``, ``humans`` and a many-to-many ``dogs_humans`` table. It will insert a record into each of those tables.

The ``.m2m()`` method executes against the last record that was affected by ``.insert()`` or ``.update()`` - the record identified by the ``table.last_pk`` property. To execute ``.m2m()`` against a specific record you can first select it by passing its primary key to ``.update()``:

.. code-block:: python

    db["dogs"].update(1).m2m(
        "humans", {"id": 2, "name": "Simon"}, pk="id"
    )

The first argument to ``.m2m()`` can be either the name of a table as a string or it can be the table object itself.

The second argument can be a single dictionary record or a list of dictionaries. These dictionaries will be passed to ``.upsert()`` against the specified table.

Here's alternative code that creates the dog record and adds two people to it:

.. code-block:: python

    db = Database(memory=True)
    dogs = db.table("dogs", pk="id")
    humans = db.table("humans", pk="id")
    dogs.insert({"id": 1, "name": "Cleo"}).m2m(
        humans, [
            {"id": 1, "name": "Natalie"},
            {"id": 2, "name": "Simon"}
        ]
    )

The method will attempt to find an existing many-to-many table by looking for a table that has foreign key relationships against both of the tables in the relationship.

If it cannot find such a table, it will create a new one using the names of the two tables - ``dogs_humans`` in this example. You can customize the name of this table using the ``m2m_table=`` argument to ``.m2m()``.

It it finds multiple candidate tables with foreign keys to both of the specified tables it will raise a ``sqlite_utils.db.NoObviousTable`` exception. You can avoid this error by specifying the correct table using ``m2m_table=``.

.. _python_api_m2m_lookup:

Using m2m and lookup tables together
------------------------------------

You can work with (or create) lookup tables as part of a call to ``.m2m()`` using the ``lookup=`` parameter. This accepts the same argument as ``table.lookup()`` does - a dictionary of values that should be used to lookup or create a row in the lookup table.

This example creates a dogs table, populates it, creates a characteristics table, populates that and sets up a many-to-many relationship between the two. It chains ``.m2m()`` twice to create two associated characteristics:

.. code-block:: python

    db = Database(memory=True)
    dogs = db.table("dogs", pk="id")
    dogs.insert({"id": 1, "name": "Cleo"}).m2m(
        "characteristics", lookup={
            "name": "Playful"
        }
    ).m2m(
        "characteristics", lookup={
            "name": "Opinionated"
        }
    )

You can inspect the database to see the results like this::

    >>> db.table_names()
    ['dogs', 'characteristics', 'characteristics_dogs']
    >>> list(db["dogs"].rows)
    [{'id': 1, 'name': 'Cleo'}]
    >>> list(db["characteristics"].rows)
    [{'id': 1, 'name': 'Playful'}, {'id': 2, 'name': 'Opinionated'}]
    >>> list(db["characteristics_dogs"].rows)
    [{'characteristics_id': 1, 'dogs_id': 1}, {'characteristics_id': 2, 'dogs_id': 1}]
    >>> print(db["characteristics_dogs"].schema)
    CREATE TABLE [characteristics_dogs] (
        [characteristics_id] INTEGER REFERENCES [characteristics]([id]),
        [dogs_id] INTEGER REFERENCES [dogs]([id]),
        PRIMARY KEY ([characteristics_id], [dogs_id])
    )

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

You can also add a column that is a foreign key reference to another table using the ``fk`` parameter:

.. code-block:: python

    db["dogs"].add_column("species_id", fk="species")

This will automatically detect the name of the primary key on the species table and use that (and its type) for the new column.

You can explicitly specify the column you wish to reference using ``fk_col``:

.. code-block:: python

    db["dogs"].add_column("species_id", fk="species", fk_col="ref")

You can set a ``NOT NULL DEFAULT 'x'`` constraint on the new column using ``not_null_default``:

.. code-block:: python

    db["dogs"].add_column("friends_count", int, not_null_default=0)

.. _python_api_add_column_alter:

Adding columns automatically on insert/update
=============================================

You can insert or update data that includes new columns and have the table automatically altered to fit the new schema using the ``alter=True`` argument. This can be passed to all four of ``.insert()``, ``.upsert()``, ``.insert_all()`` and ``.upsert_all()``, or it can be passed to ``db.table(table_name, alter=True)`` to enable it by default for all method calls against that table instance.

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

    # This works too:
    new_table = db.table("new_table", alter=True)
    new_table.insert({"name": "Gareth", "age": 32, "shoe_size": 11})

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

The ``table.add_foreign_key(column, other_table, other_column)`` method takes the name of the column, the table that is being referenced and the key column within that other table. If you ommit the ``other_column`` argument the primary key from that table will be used automatically. If you omit the ``other_table`` argument the table will be guessed based on some simple rules:

- If the column is of format ``author_id``, look for tables called ``author`` or ``authors``
- If the column does not end in ``_id``, try looking for a table with the exact name of the column or that name with an added ``s``

This method first checks that the specified foreign key references tables and columns that exist and does not clash with an existing foreign key. It will raise a ``sqlite_utils.db.AlterError`` exception if these checks fail.

.. _python_api_add_foreign_keys:

Adding multiple foreign key constraints at once
-----------------------------------------------

The final step in adding a new foreign key to a SQLite database is to run ``VACUUM``, to ensure the new foreign key is available in future introspection queries.

``VACUUM`` against a large (multi-GB) database can take several minutes or longer. If you are adding multiple foreign keys using ``table.add_foreign_key(...)`` these can quickly add up.

Instead, you can use ``db.add_foreign_keys(...)`` to add multiple foreign keys within a single transaction. This method takes a list of four-tuples, each one specifying a ``table``, ``column``, ``other_table`` and ``other_column``.

Here's an example adding two foreign keys at once:

.. code-block:: python

    db.add_foreign_keys([
        ("dogs", "breed_id", "breeds", "id"),
        ("dogs", "home_town_id", "towns", "id")
    ])

This method runs the same checks as ``.add_foreign_keys()`` and will raise ``sqlite_utils.db.AlterError`` if those checks fail.

.. _python_api_index_foreign_keys:

Adding indexes for all foreign keys
-----------------------------------

If you want to ensure that every foreign key column in your database has a corresponding index, you can do so like this:

.. code-block:: python

    db.index_foreign_keys()

.. _python_api_drop:

Dropping a table or view
========================

You can drop a table or view using the ``.drop()`` method:

.. code-block:: python

    db["my_table"].drop()

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

.. _python_api_create_view:

Creating views
==============

The ``.create_view()`` method on the database class can be used to create a view:

.. code-block:: python

    db.create_view("good_dogs", """
        select * from dogs where is_good_dog = 1
    """)

This will raise a ``sqlite_utils.utils.OperationalError`` if a view with that name already exists.

You can pass ``ignore=True`` to silently ignore an existing view and do nothing, or ``replace=True`` to replace an existing view with a new definition if your select statement differs from the current view:

.. code-block:: python

    db.create_view("good_dogs", """
        select * from dogs where is_good_dog = 1
    """, replace=True)

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
    db.execute("""
        select json_extract(address, '$.addressLocality')
        from niche_museums
    """).fetchall()
    # Returns [('Felton, CA',)]

.. _python_api_conversions:

Converting column values using SQL functions
============================================

Sometimes it can be useful to run values through a SQL function prior to inserting them. A simple example might be converting a value to upper case while it is being inserted.

The ``conversions={...}`` parameter can be used to specify custom SQL to be used as part of a ``INSERT`` or ``UPDATE`` SQL statement.

You can specify an upper case conversion for a specific column like so:

.. code-block:: python

    db["example"].insert({
        "name": "The Bigfoot Discovery Museum"
    }, conversions={"name": "upper(?)"})

    # list(db["example"].rows) now returns:
    # [{'name': 'THE BIGFOOT DISCOVERY MUSEUM'}]

The dictionary key is the column name to be converted. The value is the SQL fragment to use, with a ``?`` placeholder for the original value.

A more useful example: if you are working with `SpatiaLite <https://www.gaia-gis.it/fossil/libspatialite/index>`__ you may find yourself wanting to create geometry values from a WKT value. Code to do that could look like this:

.. code-block:: python

    import sqlite3
    import sqlite_utils
    from shapely.geometry import shape
    import requests

    # Open a database and load the SpatiaLite extension:
    import sqlite3

    conn = sqlite3.connect("places.db")
    conn.enable_load_extension(True)
    conn.load_extension("/usr/local/lib/mod_spatialite.dylib")

    # Use sqlite-utils to create a places table:
    db = sqlite_utils.Database(conn)
    places = db["places"].create({"id": int, "name": str,})

    # Add a SpatiaLite 'geometry' column:
    db.execute("select InitSpatialMetadata(1)")
    db.execute(
        "SELECT AddGeometryColumn('places', 'geometry', 4326, 'MULTIPOLYGON', 2);"
    )

    # Fetch some GeoJSON from Who's On First:
    geojson = requests.get(
        "https://data.whosonfirst.org/404/227/475/404227475.geojson"
    ).json()

    # Convert to "Well Known Text" format using shapely
    wkt = shape(geojson["geometry"]).wkt

    # Insert the record, converting the WKT to a SpatiaLite geometry:
    db["places"].insert(
        {"name": "Wales", "geometry": wkt},
        conversions={"geometry": "GeomFromText(?, 4326)"},
    )

.. _python_api_introspection:

Introspection
=============

If you have loaded an existing table or view, you can use introspection to find out more about it::

    >>> db["PlantType"]
    <Table PlantType (id, value)>

The ``.exists()`` method can be used to find out if a table exists or not::

    >>> db["PlantType"].exists()
    True
    >>> db["PlantType2"].exists()
    False

The ``.count`` property shows the current number of rows (``select count(*) from table``)::

    >>> db["PlantType"].count
    3
    >>> db["Street_Tree_List"].count
    189144

The ``.columns`` property shows the columns in the table or view::

    >>> db["PlantType"].columns
    [Column(cid=0, name='id', type='INTEGER', notnull=0, default_value=None, is_pk=1),
     Column(cid=1, name='value', type='TEXT', notnull=0, default_value=None, is_pk=0)]

The ``.columns_dict`` property returns a dictionary version of this with just the names and types::

    >>> db["PlantType"].columns_dict
    {'id': <class 'int'>, 'value': <class 'str'>}

The ``.pks`` property returns a list of strings naming the primary key columns for the table::

    >>> db["PlantType"].pks
    ['id']

The ``.foreign_keys`` property shows if the table has any foreign key relationships. It is not available on views.

::

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

The ``.indexes`` property shows you all indexes created for a table. It is not available on views.

::

    >>> db["Street_Tree_List"].indexes
    [Index(seq=0, name='"Street_Tree_List_qLegalStatus"', unique=0, origin='c', partial=0, columns=['qLegalStatus']),
     Index(seq=1, name='"Street_Tree_List_qCareAssistant"', unique=0, origin='c', partial=0, columns=['qCareAssistant']),
     Index(seq=2, name='"Street_Tree_List_qSiteInfo"', unique=0, origin='c', partial=0, columns=['qSiteInfo']),
     Index(seq=3, name='"Street_Tree_List_qSpecies"', unique=0, origin='c', partial=0, columns=['qSpecies']),
     Index(seq=4, name='"Street_Tree_List_qCaretaker"', unique=0, origin='c', partial=0, columns=['qCaretaker']),
     Index(seq=5, name='"Street_Tree_List_PlantType"', unique=0, origin='c', partial=0, columns=['PlantType'])]

The ``.triggers`` property lists database triggers. It can be used on both database and table objects.

::

    >>> db["authors"].triggers
    [Trigger(name='authors_ai', table='authors', sql='CREATE TRIGGER [authors_ai] AFTER INSERT...'),
     Trigger(name='authors_ad', table='authors', sql="CREATE TRIGGER [authors_ad] AFTER DELETE..."),
     Trigger(name='authors_au', table='authors', sql="CREATE TRIGGER [authors_au] AFTER UPDATE")]
    >>> db.triggers
    ... similar output to db["authors"].triggers

The ``detect_fts()`` method returns the associated SQLite FTS table name, if one exists for this table. If the table has not been configured for full-text search it returns ``None``.

::

    >> db["authors"].detect_fts()
    "authors_fts"

.. _python_api_fts:

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

A better solution is to use database triggers. You can set up database triggers to automatically update the full-text index using ``create_triggers=True``:

.. code-block:: python

    dogs.enable_fts(["name", "twitter"], create_triggers=True)

``.enable_fts()`` defaults to using `FTS5 <https://www.sqlite.org/fts5.html>`__. If you wish to use `FTS4 <https://www.sqlite.org/fts3.html>`__ instead, use the following:

.. code-block:: python

    dogs.enable_fts(["name", "twitter"], fts_version="FTS4")

You can customize the tokenizer configured for the table using the ``tokenize=`` parameter. For example, to enable Porter stemming, where English words like "running" will match stemmed alternatives such as "run", use ``tokenize="porter"``:

.. code-block:: python

    db["articles"].enable_fts(["headline", "body"], tokenize="porter")

The SQLite documentation has more on `FTS5 tokenizers <https://www.sqlite.org/fts5.html#tokenizers>`__ and `FTS4 tokenizers <https://www.sqlite.org/fts3.html#tokenizer>`__. ``porter`` is a valid option for both.

To remove the FTS tables and triggers you created, use the ``disable_fts()`` table method:

.. code-block:: python

    dogs.disable_fts()

Optimizing a full-text search table
===================================

Once you have populated a FTS table you can optimize it to dramatically reduce its size like so:

.. code-block:: python

    dogs.optimize()

This runs the following SQL::

    INSERT INTO dogs_fts (dogs_fts) VALUES ("optimize");
    DELETE FROM [dogs_fts_docsize] WHERE id NOT IN (
        SELECT rowid FROM [dogs_fts]);

That ``DELETE`` statement cleans up rows that may have been created by `an obscure bug <https://github.com/simonw/sqlite-utils/issues/153>`__ in previous versions of ``sqlite-utils``.

Creating indexes
================

You can create an index on a table using the ``.create_index(columns)`` method. The method takes a list of columns:

.. code-block:: python

    dogs.create_index(["is_good_dog"])

By default the index will be named ``idx_{table-name}_{columns}`` - if you want to customize the name of the created index you can pass the ``index_name`` parameter:

.. code-block:: python

    dogs.create_index(
        ["is_good_dog", "age"],
        index_name="good_dogs_by_age"
    )

You can create a unique index by passing ``unique=True``:

.. code-block:: python

    dogs.create_index(["name"], unique=True)

Use ``if_not_exists=True`` to do nothing if an index with that name already exists.

.. _python_api_vacuum:

Vacuum
======

You can optimize your database by running VACUUM against it like so:

.. code-block:: python

    Database("my_database.db").vacuum()

.. _python_api_wal:

WAL mode
========

You can enable `Write-Ahead Logging <https://www.sqlite.org/wal.html>`__ for a database with ``.enable_wal()``:

.. code-block:: python

    Database("my_database.db").enable_wal()

You can disable WAL mode using ``.disable_wal()``:

.. code-block:: python

    Database("my_database.db").disable_wal()

You can check the current journal mode for a database using the ``journal_mode`` property:

.. code-block:: python

    journal_mode = Database("my_database.db").journal_mode

This will usually be ``wal`` or ``delete`` (meaning WAL is disabled), but can have other values - see the `PRAGMA journal_mode <https://www.sqlite.org/pragma.html#pragma_journal_mode>`__ documentation.

.. _python_api_suggest_column_types:

Suggesting column types
=======================

When you create a new table for a list of inserted or upserted Python dictionaries, those methods detect the correct types for the database columns based on the data you pass in.

In some situations you may need to intervene in this process, to customize the columns that are being created in some way - see :ref:`python_api_explicit_create`.

That table ``.create()`` method takes a dictionary mapping column names to the Python type they should store:

.. code-block:: python

    db["cats"].create({
        "id": int,
        "name": str,
        "weight": float,
    })

You can use the ``suggest_column_types()`` helper function to derive a dictionary of column names and types from a list of records, suitable to be passed to ``table.create()``.

For example:

.. code-block:: python

    from sqlite_utils import Database, suggest_column_types

    cats = [{
        "id": 1,
        "name": "Snowflake"
    }, {
        "id": 2,
        "name": "Crabtree",
        "age": 4
    }]
    types = suggest_column_types(cats)
    # types now looks like this:
    # {"id": <class 'int'>,
    #  "name": <class 'str'>,
    #  "age": <class 'int'>}

    # Manually add an extra field:
    types["thumbnail"] = bytes
    # types now looks like this:
    # {"id": <class 'int'>,
    #  "name": <class 'str'>,
    #  "age": <class 'int'>,
    #  "thumbnail": <class 'bytes'>}

    # Create the table
    db = Database("cats.db")
    db["cats"].create(types, pk="id")
    # Insert the records
    db["cats"].insert_all(cats)

    # list(db["cats"].rows) now returns:
    # [{"id": 1, "name": "Snowflake", "age": None, "thumbnail": None}
    #  {"id": 2, "name": "Crabtree", "age": 4, "thumbnail": None}]

    # The table schema looks like this:
    # print(db["cats"].schema)
    # CREATE TABLE [cats] (
    #    [id] INTEGER PRIMARY KEY,
    #    [name] TEXT,
    #    [age] INTEGER,
    #    [thumbnail] BLOB
    # )

.. _find_spatialite:

Finding SpatiaLite
==================

The ``find_spatialite()`` function searches for the `SpatiaLite <https://www.gaia-gis.it/fossil/libspatialite/index>`__ SQLite extension in some common places. It returns a string path to the location, or ``None`` if SpatiaLite was not found.

You can use it in code like this:

.. code-block:: python

    from sqlite_utils import Database
    from sqlite_utils.utils import find_spatialite

    db = Database("mydb.db")
    spatialite = find_spatialite()
    if spatialite:
        db.conn.enable_load_extension(True)
        db.conn.load_extension(spatialite)
