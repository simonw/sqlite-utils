.. _python_api:

=============================
 sqlite_utils Python library
=============================

.. contents:: :local:
   :class: this-will-duplicate-information-and-it-is-still-useful-here

.. _python_api_getting_started:

Getting started
===============

Here's how to create a new SQLite database file containing a new ``chickens`` table, populated with four records:

.. code-block:: python

    from sqlite_utils import Database

    db = Database("chickens.db")
    db["chickens"].insert_all([{
        "name": "Azi",
        "color": "blue",
    }, {
        "name": "Lila",
        "color": "blue",
    }, {
        "name": "Suna",
        "color": "gold",
    }, {
        "name": "Cardi",
        "color": "black",
    }])

You can loop through those rows like this:

.. code-block:: python

    for row in db["chickens"].rows:
        print(row)

Which outputs the following::

    {'name': 'Azi', 'color': 'blue'}
    {'name': 'Lila', 'color': 'blue'}
    {'name': 'Suna', 'color': 'gold'}
    {'name': 'Cardi', 'color': 'black'}

To run a SQL query, use :ref:`db.query() <python_api_query>`:

.. code-block:: python

    for row in db.query("""
        select color, count(*)
        from chickens group by color
        order by count(*) desc
    """):
        print(row)

Which outputs::

    {'color': 'blue', 'count(*)': 2}
    {'color': 'gold', 'count(*)': 1}
    {'color': 'black', 'count(*)': 1}

.. _python_api_connect:

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

You can also create a named in-memory database. Unlike regular memory databases these can be accessed by multiple threads, provided at least one reference to the database still exists. `del db` will clear the database from memory.

.. code-block:: python

    db = Database(memory_name="my_shared_database")

Connections use ``PRAGMA recursive_triggers=on`` by default. If you don't want to use `recursive triggers <https://www.sqlite.org/pragma.html#pragma_recursive_triggers>`__ you can turn them off using:

.. code-block:: python

    db = Database(memory=True, recursive_triggers=False)

.. _python_api_attach:

Attaching additional databases
------------------------------

SQLite supports cross-database SQL queries, which can join data from tables in more than one database file.

You can attach an additional database using the ``.attach()`` method, providing an alias to use for that database and the path to the SQLite file on disk.

.. code-block:: python

    db = Database("first.db")
    db.attach("second", "second.db")
    # Now you can run queries like this one:
    print(db.query("""
    select * from table_in_first
        union all
    select * from second.table_in_second
    """))

You can reference tables in the attached database using the alias value you passed to ``db.attach(alias, filepath)`` as a prefix, for example the ``second.table_in_second`` reference in the SQL query above.

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
    with db.tracer(print):
        db["dogs"].insert({"name": "Cleo"})

This example will print queries only for the duration of the ``with`` block.

.. _python_api_executing_queries:

Executing queries
=================

The ``Database`` class offers several methods for directly executing SQL queries.

.. _python_api_query:

db.query(sql, params)
---------------------

The ``db.query(sql)`` function executes a SQL query and returns an iterator over Python dictionaries representing the resulting rows:

.. code-block:: python

    db = Database(memory=True)
    db["dogs"].insert_all([{"name": "Cleo"}, {"name": "Pancakes"}])
    for row in db.query("select * from dogs"):
        print(row)
    # Outputs:
    # {'name': 'Cleo'}
    # {'name': 'Pancakes'}

.. _python_api_execute:

db.execute(sql, params)
-----------------------

The ``db.execute()`` and ``db.executescript()`` methods provide wrappers around ``.execute()`` and ``.executescript()`` on the underlying SQLite connection. These wrappers log to the :ref:`tracer function <python_api_tracing>` if one has been registered.

``db.execute(sql)`` returns a `sqlite3.Cursor <https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor>`__ that was used to execute the SQL.

.. code-block:: python

    db = Database(memory=True)
    db["dogs"].insert({"name": "Cleo"})
    cursor = db.execute("update dogs set name = 'Cleopaws'")
    print(cursor.rowcount)
    # Outputs the number of rows affected by the update
    # In this case 2

Other cursor methods such as ``.fetchone()`` and ``.fetchall()`` are also available, see the `standard library documentation <https://docs.python.org/3/library/sqlite3.html#sqlite3.Cursor>`__.

.. _python_api_parameters:

Passing parameters
------------------

Both ``db.query()`` and ``db.execute()`` accept an optional second argument for parameters to be passed to the SQL query.

This can take the form of either a tuple/list or a dictionary, depending on the type of parameters used in the query. Values passed in this way will be correctly quoted and escaped, helping avoid SQL injection vulnerabilities.

``?`` parameters in the SQL query can be filled in using a list:

.. code-block:: python

    db.execute("update dogs set name = ?", ["Cleopaws"])
    # This will rename ALL dogs to be called "Cleopaws"

Named parameters using ``:name`` can be filled using a dictionary:

.. code-block:: python

    dog = next(db.query(
        "select rowid, name from dogs where name = :name",
        {"name": "Cleopaws"}
    ))
    # dog is now {'rowid': 1, 'name': 'Cleopaws'}

In this example ``next()`` is used to retrieve the first result in the iterator returned by the ``db.query()`` method.

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
* ``rows_where(where, where_args, order_by, select)``
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

The first argument is a fragment of SQL. The second, optional argument is values to be passed to that fragment - you can use ``?`` placeholders and pass an array, or you can use ``:named`` parameters and pass a dictionary, like this::

    >>> for row in db["dogs"].rows_where("age > :age", {"age": 3}):
    ...     print(row)
    {'id': 1, 'age': 4, 'name': 'Cleo'}

To return custom columns (instead of the default that uses ``select *``) pass ``select="column1, column2"``::

    >>> db = sqlite_utils.Database("dogs.db")
    >>> for row in db["dogs"].rows_where(select='name, age'):
    ...     print(row)
    {'name': 'Cleo', 'age': 4}

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

This method also accepts ``offset=`` and ``limit=`` arguments, for specifying an OFFSET and a LIMIT for the SQL query::

    >>> for row in db["dogs"].rows_where(order_by="age desc", limit=1):
    ...     print(row)
    {'id': 1, 'age': 4, 'name': 'Cleo'}

.. _python_api_rows_count_where:

Counting rows
-------------

To count the number of rows that would be returned by a where filter, use ``.count_where(where, where_args)``:

    >>> db["dogs"].count_where("age > ?", [1])
    2

.. _python_api_pks_and_rows_where:

Listing rows with their primary keys
====================================

Sometimes it can be useful to retrieve the primary key along with each row, in order to pass that key (or primary key tuple) to the ``.get()`` or ``.update()`` methods.

The ``.pks_and_rows_where()`` method takes the same signature as ``.rows_where()`` (with the exception of the ``select=`` parameter) but returns a generator that yields pairs of ``(primary key, row dictionary)``.

The primary key value will usually be a single value but can also be a tuple if the table has a compound primary key.

If the table is a ``rowid`` table (with no explicit primary key column) then that ID will be returned.

::

    >>> db = sqlite_utils.Database(memory=True)
    >>> db["dogs"].insert({"name": "Cleo"})
    >>> for pk, row in db["dogs"].pks_and_rows_where():
    ...     print(pk, row)
    1 {'rowid': 1, 'name': 'Cleo'}

    >>> db["dogs_with_pk"].insert({"id": 5, "name": "Cleo"}, pk="id")
    >>> for pk, row in db["dogs_with_pk"].pks_and_rows_where():
    ...     print(pk, row)
    5 {'id': 5, 'name': 'Cleo'}

    >>> db["dogs_with_compound_pk"].insert(
    ...     {"species": "dog", "id": 3, "name": "Cleo"},
    ...     pk=("species", "id")
    ... )
    >>> for pk, row in db["dogs_with_compound_pk"].pks_and_rows_where():
    ...     print(pk, row)
    ('dog', 3) {'species': 'dog', 'id': 3, 'name': 'Cleo'}

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

.. _python_api_schema:

Showing the schema
==================

The ``db.schema`` property returns the full SQL schema for the database as a string::

    >>> db = sqlite_utils.Database("dogs.db")
    >>> print(db.schema)
    CREATE TABLE "dogs" (
        [id] INTEGER PRIMARY KEY,
        [name] TEXT
    );

.. _python_api_creating_tables:

Creating tables
===============

The easiest way to create a new table is to insert a record into it:

.. code-block:: python

    from sqlite_utils import Database
    import sqlite3

    db = Database("dogs.db")
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

    db["dogs"].insert({
        "id": 1,
        "name": "Cleo",
        "twitter": "cleopaws",
        "age": 3,
        "is_good_dog": True,
    }, pk="id", column_order=("id", "twitter", "name"))

You don't need to pass all of the columns to the ``column_order`` parameter. If you only pass a subset of the columns the remaining columns will be ordered based on the key order of the dictionary.

Column types are detected based on the example data provided. Sometimes you may find you need to over-ride these detected types - to create an integer column for data that was provided as a string for example, or to ensure that a table where the first example was ``None`` is created as an ``INTEGER`` rather than a ``TEXT`` column. You can do this using the ``columns=`` parameter:

.. code-block:: python

    db["dogs"].insert({
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

You can directly create a new table without inserting any data into it using the ``.create()`` method:

.. code-block:: python

    db["cats"].create({
        "id": int,
        "name": str,
        "weight": float,
    }, pk="id")

The first argument here is a dictionary specifying the columns you would like to create. Each column is paired with a Python type indicating the type of column. See :ref:`python_api_add_column` for full details on how these types work.

This method takes optional arguments ``pk=``, ``column_order=``, ``foreign_keys=``, ``not_null=set()`` and ``defaults=dict()`` - explained below.

A ``sqlite_utils.utils.sqlite3.OperationalError`` will be raised if a table of that name already exists.

You can pass ``ignore=True`` to ignore that error. You can also use ``if_not_exists=True`` to use the SQL ``CREATE TABLE IF NOT EXISTS`` pattern to achieve the same effect:

.. code-block:: python

    db["cats"].create({
        "id": int,
        "name": str,
    }, pk="id", if_not_exists=True)

To drop and replace any existing table of that name, pass ``replace=True``. This is a **dangerous operation** that will result in loss of existing data in the table.

You can also pass ``transform=True`` to have any existing tables :ref:`transformed <python_api_transform>` to match your new table specification. This is a **dangerous operation** as it will drop columns that are no longer listed in your call to ``.create()``, so be careful when running this.

.. code-block:: python

    db["cats"].create({
        "id": int,
        "name": str,
        "weight": float,
    }, pk="id", transform=True)

The ``transform=True`` option will update the table schema if any of the following have changed:

- The specified columns or their types
- The specified primary key
- The order of the columns, defined using ``column_order=``
- The ``not_null=`` or ``defaults=`` arguments

Changes to ``foreign_keys=`` are not currently detected and applied by ``transform=True``.

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

If you are using your database with `Datasette <https://datasette.io/>`__, Datasette will detect these constraints and use them to generate hyperlinks to associated records.

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
---------------------------

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

The configuration options that can be specified in this way are ``pk``, ``foreign_keys``, ``column_order``, ``not_null``, ``defaults``, ``batch_size``, ``hash_id``, ``hash_id_columns``, ``alter``, ``ignore``, ``replace``, ``extracts``, ``conversions``, ``columns``. These are all documented below.

.. _python_api_defaults_not_null:

Setting defaults and not null constraints
-----------------------------------------

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
    print(db["authors"].schema)
    # Outputs:
    # CREATE TABLE [authors] (
    #     [id] INTEGER PRIMARY KEY,
    #     [name] TEXT NOT NULL,
    #     [score] INTEGER NOT NULL DEFAULT 1
    # )


.. _python_api_rename_table:

Renaming a table
================

The ``db.rename_table(old_name, new_name)`` method can be used to rename a table:

.. code-block:: python

    db.rename_table("my_table", "new_name_for_my_table")

This executes the following SQL:

.. code-block:: sql

    ALTER TABLE [my_table] RENAME TO [new_name_for_my_table]

.. _python_api_duplicate:

Duplicating tables
==================

The ``table.duplicate()`` method creates a copy of the table, copying both the table schema and all of the rows in that table:

.. code-block:: python

    db["authors"].duplicate("authors_copy")

The new ``authors_copy`` table will now contain a duplicate copy of the data from ``authors``.

This method raises ``sqlite_utils.db.NoTable`` if the table does not exist.

.. _python_api_bulk_inserts:

Bulk inserts
============

If you have more than one record to insert, the ``insert_all()`` method is a much more efficient way of inserting them. Just like ``insert()`` it will automatically detect the columns that should be created, but it will inspect the first batch of 100 items to help decide what those column types should be.

Use it like this:

.. code-block:: python

    db["dogs"].insert_all([{
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

You can delete all the existing rows in the table before inserting the new records using ``truncate=True``. This is useful if you want to replace the data in the table.

Pass ``analyze=True`` to run ``ANALYZE`` against the table after inserting the new records.

.. _python_api_insert_replace:

Insert-replacing data
=====================

If you try to insert data using a primary key that already exists, the ``.insert()`` or ``.insert_all()`` method will raise a ``sqlite3.IntegrityError`` exception.

This example that catches that exception:

.. code-block:: python

    from sqlite_utils.utils import sqlite3

    try:
        db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    except sqlite3.IntegrityError:
        print("Record already exists with that primary key")

Importing from ``sqlite_utils.utils.sqlite3`` ensures your code continues to work even if you are using the ``pysqlite3`` library instead of the Python standard library ``sqlite3`` module.

Use the ``ignore=True`` parameter to ignore this error:

.. code-block:: python

    # This fails silently if a record with id=1 already exists
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id", ignore=True)

To replace any existing records that have a matching primary key, use the ``replace=True`` parameter to ``.insert()`` or ``.insert_all()``:

.. code-block:: python

    db["dogs"].insert_all([{
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
    Prior to sqlite-utils 2.0 the ``.upsert()`` and ``.upsert_all()`` methods worked the same way as ``.insert(replace=True)`` does today. See :ref:`python_api_upsert` for the new behaviour of those methods introduced in 2.0.

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

The second argument is a dictionary of columns that should be updated, along with their new values.

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
    >>> db["dogs"].delete_where("age < ?", [3])

Calling ``table.delete_where()`` with no other arguments will delete every row in the table.

Pass ``analyze=True`` to run ``ANALYZE`` against the table after deleting the rows.

.. _python_api_upsert:

Upserting data
==============

Upserting allows you to insert records if they do not exist and update them if they DO exist, based on matching against their primary key.

For example, given the dogs database you could upsert the record for Cleo like so:

.. code-block:: python

    db["dogs"].upsert({
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

.. _python_api_convert:

Converting data in columns
==========================

The ``table.convert(...)`` method can be used to apply a conversion function to the values in a column, either to update that column or to populate new columns. It is the Python library equivalent of the :ref:`sqlite-utils convert <cli_convert>` command.

This feature works by registering a custom SQLite function that applies a Python transformation, then running a SQL query equivalent to ``UPDATE table SET column = convert_value(column);``

To transform a specific column to uppercase, you would use the following:

.. code-block:: python

    db["dogs"].convert("name", lambda value: value.upper())

You can pass a list of columns, in which case the transformation will be applied to each one:

.. code-block:: python

    db["dogs"].convert(["name", "twitter"], lambda value: value.upper())

To save the output to of the transformation to a different column, use the ``output=`` parameter:

.. code-block:: python

    db["dogs"].convert("name", lambda value: value.upper(), output="name_upper")

This will add the new column, if it does not already exist. You can pass ``output_type=int`` or some other type to control the type of the new column - otherwise it will default to text.

If you want to drop the original column after saving the results in a separate output column, pass ``drop=True``.

By default any rows with a falsey value for the column - such as ``0`` or ``None`` - will be skipped. Pass ``skip_false=False`` to disable this behaviour.

You can create multiple new columns from a single input column by passing ``multi=True`` and a conversion function that returns a Python dictionary. This example creates new ``upper`` and ``lower`` columns populated from the single ``title`` column:

.. code-block:: python

    table.convert(
        "title", lambda v: {"upper": v.upper(), "lower": v.lower()}, multi=True
    )

The ``.convert()`` method accepts optional ``where=`` and ``where_args=`` parameters which can be used to apply the conversion to a subset of rows specified by a where clause. Here's how to apply the conversion only to rows with an ``id`` that is higher than 20:

.. code-block:: python

    table.convert("title", lambda v: v.upper(), where="id > :id", where_args={"id": 20})

These behave the same as the corresponding parameters to the :ref:`.rows_where() <python_api_rows>` method, so you can use ``?`` placeholders and a list of values instead of ``:named`` placeholders with a dictionary.

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

The ``.lookup()`` method has an optional second argument which can be used to populate other columns in the table but only if the row does not exist yet. These columns will not be included in the unique index.

To create a species record with a note on when it was first seen, you can use this:

.. code-block:: python

    db["Species"].lookup({"name": "Palm"}, {"first_seen": "2021-03-04"})

The first time this is called the record will be created for ``name="Palm"``. Any subsequent calls with that name will ignore the second argument, even if it includes different values.

``.lookup()`` also accepts keyword arguments, which are passed through to the :ref:`insert() method <python_api_creating_tables>` and can be used to influence the shape of the created table. Supported parameters are:

- ``pk`` - which defaults to ``id``
- ``foreign_keys``
- ``column_order``
- ``not_null``
- ``defaults``
- ``extracts``
- ``conversions``
- ``columns``

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

The ``.m2m()`` method also takes an optional ``pk=`` argument to specify the primary key that should be used if the table is created, and an optional ``alter=True`` argument to specify that any missing columns of an existing table should be added if they are needed.

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

.. _python_api_analyze_column:

Analyzing a column
==================

The ``table.analyze_column(column)`` method is used by the :ref:`analyze-tables <cli_analyze_tables>` CLI command.

It takes the following arguments and options:

``column`` - required
    The name of the column to analyze

``common_limit``
    The number of most common values to return. Defaults to 10.

``value_truncate``
    If set to an integer, values longer than this will be truncated to this length. Defaults to None.

``most_common``
    If set to False, the ``most_common`` field of the returned ``ColumnDetails`` will be set to None. Defaults to True.

``least_common``
    If set to False, the ``least_common`` field of the returned ``ColumnDetails`` will be set to None. Defaults to True.

And returns a ``ColumnDetails`` named tuple with the following fields:

``table``
    The name of the table

``column``
    The name of the column

``total_rows``
    The total number of rows in the table

``num_null``
    The number of rows for which this column is null

``num_blank``
    The number of rows for which this column is blank (the empty string)

``num_distinct``
    The number of distinct values in this column

``most_common``
    The ``N`` most common values as a list of ``(value, count)`` tuples`, or ``None`` if the table consists entirely of distinct values

``least_common``
    The ``N`` least common values as a list of ``(value, count)`` tuples`, or ``None`` if the table is entirely distinct or if the number of distinct values is less than N (since they will already have been returned in ``most_common``)

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

The ``table.add_foreign_key(column, other_table, other_column)`` method takes the name of the column, the table that is being referenced and the key column within that other table. If you omit the ``other_column`` argument the primary key from that table will be used automatically. If you omit the ``other_table`` argument the table will be guessed based on some simple rules:

- If the column is of format ``author_id``, look for tables called ``author`` or ``authors``
- If the column does not end in ``_id``, try looking for a table with the exact name of the column or that name with an added ``s``

This method first checks that the specified foreign key references tables and columns that exist and does not clash with an existing foreign key. It will raise a ``sqlite_utils.db.AlterError`` exception if these checks fail.

To ignore the case where the key already exists, use ``ignore=True``:

.. code-block:: python

    db["books"].add_foreign_key("author_id", "authors", "id", ignore=True)

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

Pass ``ignore=True`` if you want to ignore the error caused by the table or view not existing.

.. code-block:: python

    db["my_table"].drop(ignore=True)

.. _python_api_transform:

Transforming a table
====================

The SQLite ``ALTER TABLE`` statement is limited. It can add and drop columns and rename tables, but it cannot change column types, change ``NOT NULL`` status or change the primary key for a table.

The ``table.transform()`` method can do all of these things, by implementing a multi-step pattern `described in the SQLite documentation <https://www.sqlite.org/lang_altertable.html#otheralter>`__:

1. Start a transaction
2. ``CREATE TABLE tablename_new_x123`` with the required changes
3. Copy the old data into the new table using ``INSERT INTO tablename_new_x123 SELECT * FROM tablename;``
4. ``DROP TABLE tablename;``
5. ``ALTER TABLE tablename_new_x123 RENAME TO tablename;``
6. Commit the transaction

The ``.transform()`` method takes a number of parameters, all of which are optional.

As a bonus, calling ``.transform()`` will reformat the schema for the table that is stored in SQLite to make it more readable. This works even if you call it without any arguments.

To keep the original table around instead of dropping it, pass the ``keep_table=`` option and specify the name of the table you would like it to be renamed to:

.. code-block:: python

    table.transform(types={"age": int}, keep_table="original_table")

Altering column types
---------------------

To alter the type of a column, use the ``types=`` argument:

.. code-block:: python

    # Convert the 'age' column to an integer, and 'weight' to a float
    table.transform(types={"age": int, "weight": float})

See :ref:`python_api_add_column` for a list of available types.

Renaming columns
----------------

The ``rename=`` parameter can rename columns:

.. code-block:: python

    # Rename 'age' to 'initial_age':
    table.transform(rename={"age": "initial_age"})

Dropping columns
----------------

To drop columns, pass them in the ``drop=`` set:

.. code-block:: python

    # Drop the 'age' column:
    table.transform(drop={"age"})

Changing primary keys
---------------------

To change the primary key for a table, use ``pk=``. This can be passed a single column for a regular primary key, or a tuple of columns to create a compound primary key. Passing ``pk=None`` will remove the primary key and convert the table into a ``rowid`` table.

.. code-block:: python

    # Make `user_id` the new primary key
    table.transform(pk="user_id")

Changing not null status
------------------------

You can change the ``NOT NULL`` status of columns by using ``not_null=``. You can pass this a set of columns to make those columns ``NOT NULL``:

.. code-block:: python

    # Make the 'age' and 'weight' columns NOT NULL
    table.transform(not_null={"age", "weight"})

If you want to take existing ``NOT NULL`` columns and change them to allow null values, you can do so by passing a dictionary of true/false values instead:

.. code-block:: python

    # 'age' is NOT NULL but we want to allow NULL:
    table.transform(not_null={"age": False})

    # Make age allow NULL and switch weight to being NOT NULL:
    table.transform(not_null={"age": False, "weight": True})

Altering column defaults
------------------------

The ``defaults=`` parameter can be used to set or change the defaults for different columns:

.. code-block:: python

    # Set default age to 1:
    table.transform(defaults={"age": 1})

    # Now remove the default from that column:
    table.transform(defaults={"age": None})

Changing column order
---------------------

The ``column_order=`` parameter can be used to change the order of the columns. If you pass the names of a subset of the columns those will go first and columns you omitted will appear in their existing order after them.

.. code-block:: python

    # Change column order
    table.transform(column_order=("name", "age", "id")

Dropping foreign key constraints
--------------------------------

You can use ``.transform()`` to remove foreign key constraints from a table.

This example drops two foreign keys - the one from ``places.country`` to ``country.id`` and the one from ``places.continent`` to ``continent.id``:

.. code-block:: python

    db["places"].transform(
        drop_foreign_keys=("country", "continent")
    )

.. _python_api_transform_sql:

Custom transformations with .transform_sql()
--------------------------------------------

The ``.transform()`` method can handle most cases, but it does not automatically upgrade indexes, views or triggers associated with the table that is being transformed.

If you want to do something more advanced, you can call the ``table.transform_sql(...)`` method with the same arguments that you would have passed to ``table.transform(...)``.

This method will return a list of SQL statements that should be executed to implement the change. You can then make modifications to that SQL - or add additional SQL statements - before executing it yourself.

.. _python_api_extract:

Extracting columns into a separate table
========================================

The ``table.extract()`` method can be used to extract specified columns into a separate table.

Imagine a ``Trees`` table that looks like this:

===  ============  =======
 id  TreeAddress   Species
===  ============  =======
  1  52 Vine St    Palm
  2  12 Draft St   Oak
  3  51 Dark Ave   Palm
  4  1252 Left St  Palm
===  ============  =======

The ``Species`` column contains duplicate values. This database could be improved by extracting that column out into a separate ``Species`` table and pointing to it using a foreign key column.

The schema of the above table is:

.. code-block:: sql

    CREATE TABLE [Trees] (
        [id] INTEGER PRIMARY KEY,
        [TreeAddress] TEXT,
        [Species] TEXT
    )

Here's how to extract the ``Species`` column using ``.extract()``:

.. code-block:: python

    db["Trees"].extract("Species")

After running this code the table schema now looks like this:

.. code-block:: sql

    CREATE TABLE "Trees" (
        [id] INTEGER PRIMARY KEY,
        [TreeAddress] TEXT,
        [Species_id] INTEGER,
        FOREIGN KEY(Species_id) REFERENCES Species(id)
    )

A new ``Species`` table will have been created with the following schema:

.. code-block:: sql

    CREATE TABLE [Species] (
        [id] INTEGER PRIMARY KEY,
        [Species] TEXT
    )

The ``.extract()`` method defaults to creating a table with the same name as the column that was extracted, and adding a foreign key column called ``tablename_id``.

You can specify a custom table name using ``table=``, and a custom foreign key name using ``fk_column=``. This example creates a table called ``tree_species`` and a foreign key column called ``tree_species_id``:

.. code-block:: python

    db["Trees"].extract("Species", table="tree_species", fk_column="tree_species_id")

The resulting schema looks like this:

.. code-block:: sql

    CREATE TABLE "Trees" (
        [id] INTEGER PRIMARY KEY,
        [TreeAddress] TEXT,
        [tree_species_id] INTEGER,
        FOREIGN KEY(tree_species_id) REFERENCES tree_species(id)
    )

    CREATE TABLE [tree_species] (
        [id] INTEGER PRIMARY KEY,
        [Species] TEXT
    )

You can also extract multiple columns into the same external table. Say for example you have a table like this:

===  ============  ==========  =========
 id  TreeAddress   CommonName  LatinName
===  ============  ==========  =========
  1  52 Vine St    Palm        Arecaceae
  2  12 Draft St   Oak         Quercus
  3  51 Dark Ave   Palm        Arecaceae
  4  1252 Left St  Palm        Arecaceae
===  ============  ==========  =========

You can pass ``["CommonName", "LatinName"]`` to ``.extract()`` to extract both of those columns:

.. code-block:: python

    db["Trees"].extract(["CommonName", "LatinName"])

This produces the following schema:

.. code-block:: sql

    CREATE TABLE "Trees" (
        [id] INTEGER PRIMARY KEY,
        [TreeAddress] TEXT,
        [CommonName_LatinName_id] INTEGER,
        FOREIGN KEY(CommonName_LatinName_id) REFERENCES CommonName_LatinName(id)
    )
    CREATE TABLE [CommonName_LatinName] (
        [id] INTEGER PRIMARY KEY,
        [CommonName] TEXT,
        [LatinName] TEXT
    )

The table name ``CommonName_LatinName`` is derived from the extract columns. You can use ``table=`` and ``fk_column=`` to specify custom names like this:

.. code-block:: python

    db["Trees"].extract(["CommonName", "LatinName"], table="Species", fk_column="species_id")

This produces the following schema:

.. code-block:: sql

    CREATE TABLE "Trees" (
        [id] INTEGER PRIMARY KEY,
        [TreeAddress] TEXT,
        [species_id] INTEGER,
        FOREIGN KEY(species_id) REFERENCES Species(id)
    )
    CREATE TABLE [Species] (
        [id] INTEGER PRIMARY KEY,
        [CommonName] TEXT,
        [LatinName] TEXT
    )

You can use the ``rename=`` argument to rename columns in the lookup table. To create a ``Species`` table with columns called ``name`` and ``latin`` you can do this:

.. code-block:: python

    db["Trees"].extract(
        ["CommonName", "LatinName"],
        table="Species",
        fk_column="species_id",
        rename={"CommonName": "name", "LatinName": "latin"}
    )

This produces a lookup table like so:

.. code-block:: sql

    CREATE TABLE [Species] (
        [id] INTEGER PRIMARY KEY,
        [name] TEXT,
        [latin] TEXT
    )

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

The hash will be created using all of the column values. To create a hash using a subset of the columns, pass the ``hash_id_columns=`` parameter::

    db["dogs"].upsert(
        {"name": "Cleo", "twitter": "cleopaws", "age": 7},
        hash_id_columns=("name", "twitter")
    )

The ``hash_id=`` parameter is optional if you specify ``hash_id_columns=`` - it will default to putting the hash in a column called ``id``.

You can manually calculate these hashes using the :ref:`hash_record(record, keys=...) <reference_utils_hash_record>` utility function.

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
    import httpx

    db = sqlite_utils.Database("places.db")
    # Initialize SpatiaLite
    db.init_spatialite()
    # Use sqlite-utils to create a places table
    places = db["places"].create({"id": int, "name": str})

    # Add a SpatiaLite 'geometry' column
    places.add_geometry_column("geometry", "MULTIPOLYGON")

    # Fetch some GeoJSON from Who's On First:
    geojson = httpx.get(
        "https://raw.githubusercontent.com/whosonfirst-data/"
        "whosonfirst-data-admin-gb/master/data/404/227/475/404227475.geojson"
    ).json()

    # Convert to "Well Known Text" format using shapely
    wkt = shape(geojson["geometry"]).wkt

    # Insert the record, converting the WKT to a SpatiaLite geometry:
    db["places"].insert(
        {"name": "Wales", "geometry": wkt},
        conversions={"geometry": "GeomFromText(?, 4326)"},
    )

This example uses gographical data from [Who's On First](https://whosonfirst.org/) and depends on the [Shapely](https://shapely.readthedocs.io/en/stable/manual.html) and [HTTPX](https://www.python-httpx.org/) Python libraries.

.. _python_api_sqlite_version:

Checking the SQLite version
===========================

The ``db.sqlite_version`` property returns a tuple of integers representing the version of SQLite used for that database object::

    >>> db.sqlite_version
    (3, 36, 0)

.. _python_api_itedump:

Dumping the database to SQL
===========================

The ``db.iterdump()`` method returns a sequence of SQL strings representing a complete dump of the database. Use it like this:

.. code-block:: python

    full_sql = "".join(db.iterdump())

This uses the `sqlite3.Connection.iterdump() <https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.iterdump>`__ method.

If you are using ``pysqlite3`` or ``sqlean.py`` the underlying method may be missing. If you install the `sqlite-dump <https://pypi.org/project/sqlite-dump/>`__ package then the ``db.iterdump()`` method will use that implementation instead:

.. code-block:: bash

    pip install sqlite-dump

.. _python_api_introspection:

Introspecting tables and views
==============================

If you have loaded an existing table or view, you can use introspection to find out more about it::

    >>> db["PlantType"]
    <Table PlantType (id, value)>

.. _python_api_introspection_exists:

.exists()
---------

The ``.exists()`` method can be used to find out if a table exists or not::

    >>> db["PlantType"].exists()
    True
    >>> db["PlantType2"].exists()
    False

.. _python_api_introspection_count:

.count
------

The ``.count`` property shows the current number of rows (``select count(*) from table``)::

    >>> db["PlantType"].count
    3
    >>> db["Street_Tree_List"].count
    189144

This property will take advantage of :ref:`python_api_cached_table_counts` if the ``use_counts_table`` property is set on the database. You can avoid that optimization entirely by calling ``table.count_where()`` instead of accessing the property.

.. _python_api_introspection_columns:

.columns
--------

The ``.columns`` property shows the columns in the table or view. It returns a list of ``Column(cid, name, type, notnull, default_value, is_pk)`` named tuples.

::

    >>> db["PlantType"].columns
    [Column(cid=0, name='id', type='INTEGER', notnull=0, default_value=None, is_pk=1),
     Column(cid=1, name='value', type='TEXT', notnull=0, default_value=None, is_pk=0)]

.. _python_api_introspection_columns_dict:

.columns_dict
-------------

The ``.columns_dict`` property returns a dictionary version of the columns with just the names and Python types::

    >>> db["PlantType"].columns_dict
    {'id': <class 'int'>, 'value': <class 'str'>}

.. _python_api_introspection_default_values:

.default_values
---------------

The ``.default_values`` property returns a dictionary of default values for each column that has a default::

    >>> db["table_with_defaults"].default_values
    {'score': 5}

.. _python_api_introspection_pks:

.pks
----

The ``.pks`` property returns a list of strings naming the primary key columns for the table::

    >>> db["PlantType"].pks
    ['id']

If a table has no primary keys but is a `rowid table <https://www.sqlite.org/rowidtable.html>`__, this property will return ``['rowid']``.

.. _python_api_introspection_use_rowid:

.use_rowid
----------

Almost all SQLite tables have a ``rowid`` column, but a table with no explicitly defined primary keys must use that ``rowid`` as the primary key for identifying individual rows. The ``.use_rowid`` property checks to see if a table needs to use the ``rowid`` in this way - it returns ``True`` if the table has no explicitly defined primary keys and ``False`` otherwise.

    >>> db["PlantType"].use_rowid
    False


.. _python_api_introspection_foreign_keys:

.foreign_keys
-------------

The ``.foreign_keys`` property returns any foreign key relationships for the table, as a list of ``ForeignKey(table, column, other_table, other_column)`` named tuples. It is not available on views.

::

    >>> db["Street_Tree_List"].foreign_keys
    [ForeignKey(table='Street_Tree_List', column='qLegalStatus', other_table='qLegalStatus', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='qCareAssistant', other_table='qCareAssistant', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='qSiteInfo', other_table='qSiteInfo', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='qSpecies', other_table='qSpecies', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='qCaretaker', other_table='qCaretaker', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='PlantType', other_table='PlantType', other_column='id')]

.. _python_api_introspection_schema:

.schema
-------

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

.. _python_api_introspection_strict:

.strict
-------

The ``.strict`` property identifies if the table is a `SQLite STRICT table <https://www.sqlite.org/stricttables.html>`__.

::

    >>> db["ny_times_us_counties"].strict
    False

.. _python_api_introspection_indexes:

.indexes
--------

The ``.indexes`` property returns all indexes created for a table, as a list of ``Index(seq, name, unique, origin, partial, columns)`` named tuples. It is not available on views.

::

    >>> db["Street_Tree_List"].indexes
    [Index(seq=0, name='"Street_Tree_List_qLegalStatus"', unique=0, origin='c', partial=0, columns=['qLegalStatus']),
     Index(seq=1, name='"Street_Tree_List_qCareAssistant"', unique=0, origin='c', partial=0, columns=['qCareAssistant']),
     Index(seq=2, name='"Street_Tree_List_qSiteInfo"', unique=0, origin='c', partial=0, columns=['qSiteInfo']),
     Index(seq=3, name='"Street_Tree_List_qSpecies"', unique=0, origin='c', partial=0, columns=['qSpecies']),
     Index(seq=4, name='"Street_Tree_List_qCaretaker"', unique=0, origin='c', partial=0, columns=['qCaretaker']),
     Index(seq=5, name='"Street_Tree_List_PlantType"', unique=0, origin='c', partial=0, columns=['PlantType'])]

.. _python_api_introspection_xindexes:

.xindexes
---------

The ``.xindexes`` property returns more detailed information about the indexes on the table, using the SQLite `PRAGMA index_xinfo() <https://sqlite.org/pragma.html#pragma_index_xinfo>`__ mechanism. It returns a list of ``XIndex(name, columns)`` named tuples, where ``columns`` is a list of ``XIndexColumn(seqno, cid, name, desc, coll, key)`` named tuples.

::

    >>> db["ny_times_us_counties"].xindexes
    [
        XIndex(
            name='idx_ny_times_us_counties_date',
            columns=[
                XIndexColumn(seqno=0, cid=0, name='date', desc=1, coll='BINARY', key=1),
                XIndexColumn(seqno=1, cid=-1, name=None, desc=0, coll='BINARY', key=0)
            ]
        ),
        XIndex(
            name='idx_ny_times_us_counties_fips',
            columns=[
                XIndexColumn(seqno=0, cid=3, name='fips', desc=0, coll='BINARY', key=1),
                XIndexColumn(seqno=1, cid=-1, name=None, desc=0, coll='BINARY', key=0)
            ]
        )
    ]

.. _python_api_introspection_triggers:

.triggers
---------

The ``.triggers`` property lists database triggers. It can be used on both database and table objects. It returns a list of ``Trigger(name, table, sql)`` named tuples.

::

    >>> db["authors"].triggers
    [Trigger(name='authors_ai', table='authors', sql='CREATE TRIGGER [authors_ai] AFTER INSERT...'),
     Trigger(name='authors_ad', table='authors', sql="CREATE TRIGGER [authors_ad] AFTER DELETE..."),
     Trigger(name='authors_au', table='authors', sql="CREATE TRIGGER [authors_au] AFTER UPDATE")]
    >>> db.triggers
    ... similar output to db["authors"].triggers

.. _python_api_introspection_triggers_dict:

.triggers_dict
--------------

The ``.triggers_dict`` property returns the triggers for that table as a dictionary mapping their names to their SQL definitions.

::

    >>> db["authors"].triggers_dict
    {'authors_ai': 'CREATE TRIGGER [authors_ai] AFTER INSERT...',
     'authors_ad': 'CREATE TRIGGER [authors_ad] AFTER DELETE...',
     'authors_au': 'CREATE TRIGGER [authors_au] AFTER UPDATE'}

The same property exists on the database, and will return all triggers across all tables:

::

    >>> db.triggers_dict
    {'authors_ai': 'CREATE TRIGGER [authors_ai] AFTER INSERT...',
     'authors_ad': 'CREATE TRIGGER [authors_ad] AFTER DELETE...',
     'authors_au': 'CREATE TRIGGER [authors_au] AFTER UPDATE'}

.. _python_api_introspection_detect_fts:

.detect_fts()
-------------

The ``detect_fts()`` method returns the associated SQLite FTS table name, if one exists for this table. If the table has not been configured for full-text search it returns ``None``.

::

    >>> db["authors"].detect_fts()
    "authors_fts"

.. _python_api_introspection_virtual_table_using:

.virtual_table_using
--------------------

The ``.virtual_table_using`` property reveals if a table is a virtual table. It returns ``None`` for regular tables and the upper case version of the type of virtual table otherwise. For example::

    >>> db["authors"].enable_fts(["name"])
    >>> db["authors_fts"].virtual_table_using
    "FTS5"

.. _python_api_introspection_has_counts_triggers:

.has_counts_triggers
--------------------

The ``.has_counts_triggers`` property shows if a table has been configured with triggers for updating a ``_counts`` table, as described in :ref:`python_api_cached_table_counts`.

::

    >>> db["authors"].has_counts_triggers
    False
    >>> db["authors"].enable_counts()
    >>> db["authors"].has_counts_triggers
    True

.. _python_api_fts:

Full-text search
================

SQLite includes bundled extensions that implement `powerful full-text search <https://www.sqlite.org/fts5.html>`__.

.. _python_api_fts_enable:

Enabling full-text search for a table
-------------------------------------

You can enable full-text search on a table using ``.enable_fts(columns)``:

.. code-block:: python

    db["dogs"].enable_fts(["name", "twitter"])

You can then run searches using the ``.search()`` method:

.. code-block:: python

    rows = list(db["dogs"].search("cleo"))

This method returns a generator that can be looped over to get dictionaries for each row, similar to :ref:`python_api_rows`.

If you insert additional records into the table you will need to refresh the search index using ``populate_fts()``:

.. code-block:: python

    db["dogs"].insert({
        "id": 2,
        "name": "Marnie",
        "twitter": "MarnieTheDog",
        "age": 16,
        "is_good_dog": True,
    }, pk="id")
    db["dogs"].populate_fts(["name", "twitter"])

A better solution is to use database triggers. You can set up database triggers to automatically update the full-text index using ``create_triggers=True``:

.. code-block:: python

    db["dogs"].enable_fts(["name", "twitter"], create_triggers=True)

``.enable_fts()`` defaults to using `FTS5 <https://www.sqlite.org/fts5.html>`__. If you wish to use `FTS4 <https://www.sqlite.org/fts3.html>`__ instead, use the following:

.. code-block:: python

    db["dogs"].enable_fts(["name", "twitter"], fts_version="FTS4")

You can customize the tokenizer configured for the table using the ``tokenize=`` parameter. For example, to enable Porter stemming, where English words like "running" will match stemmed alternatives such as "run", use ``tokenize="porter"``:

.. code-block:: python

    db["articles"].enable_fts(["headline", "body"], tokenize="porter")

The SQLite documentation has more on `FTS5 tokenizers <https://www.sqlite.org/fts5.html#tokenizers>`__ and `FTS4 tokenizers <https://www.sqlite.org/fts3.html#tokenizer>`__. ``porter`` is a valid option for both.

If you attempt to configure a FTS table where one already exists, a ``sqlite3.OperationalError`` exception will be raised.

You can replace the existing table with a new configuration using ``replace=True``:

.. code-block:: python

    db["articles"].enable_fts(["headline"], tokenize="porter", replace=True)

This will have no effect if the FTS table already exists, otherwise it will drop and recreate the table with the new settings. This takes into consideration the columns, the tokenizer, the FTS version used and whether or not the table has triggers.

To remove the FTS tables and triggers you created, use the ``disable_fts()`` table method:

.. code-block:: python

    db["dogs"].disable_fts()

.. _python_api_quote_fts:

Quoting characters for use in search
------------------------------------

SQLite supports `advanced search query syntax <https://www.sqlite.org/fts3.html#full_text_index_queries>`__. In some situations you may wish to disable this, since characters such as ``.`` may have special meaning that causes errors when searching for strings provided by your users.

The ``db.quote_fts(query)`` method returns the query with SQLite full-text search quoting applied such that the query should be safe to use in a search::

    db.quote_fts("Search term.")
    # Returns: '"Search" "term."'

.. _python_api_fts_search:

Searching with table.search()
-----------------------------

The ``table.search(q)`` method returns a generator over Python dictionaries representing rows that match the search phrase ``q``, ordered by relevance with the most relevant results first.

.. code-block:: python

    for article in db["articles"].search("jquery"):
        print(article)

The ``.search()`` method also accepts the following optional parameters:

``order_by`` string
    The column to sort by. Defaults to relevance score. Can optionally include a ``desc``, e.g. ``rowid desc``.

``columns`` array of strings
    Columns to return. Defaults to all columns.

``limit`` integer
    Number of results to return. Defaults to all results.

``offset`` integer
    Offset to use along side the limit parameter.

``where`` string
    Extra SQL fragment for the WHERE clause

``where_args`` dictionary
    Arguments to use for ``:param`` placeholders in the extra WHERE clause

``quote`` bool
    Apply :ref:`FTS quoting rules <python_api_quote_fts>` to the search query, disabling advanced query syntax in a way that avoids surprising errors.

To return just the title and published columns for three matches for ``"dog"`` where the ``id`` is greater than 10 ordered by ``published`` with the most recent first, use the following:

.. code-block:: python

    for article in db["articles"].search(
        "dog",
        order_by="published desc",
        limit=3,
        where="id > :min_id",
        where_args={"min_id": 10},
        columns=["title", "published"]
    ):
        print(article)

.. _python_api_fts_search_sql:

Building SQL queries with table.search_sql()
--------------------------------------------

You can generate the SQL query that would be used for a search using the ``table.search_sql()`` method. It takes the same arguments as ``table.search()``, with the exception of the search query and the ``where_args`` parameter, since those should be provided when the returned SQL is executed.

.. code-block:: python

    print(db["articles"].search_sql(columns=["title", "author"]))

Outputs:

.. code-block:: sql

    with original as (
        select
            rowid,
            [title],
            [author]
        from [articles]
    )
    select
        [original].[title],
        [original].[author]
    from
        [original]
        join [articles_fts] on [original].rowid = [articles_fts].rowid
    where
        [articles_fts] match :query
    order by
        [articles_fts].rank

This method detects if a SQLite table uses FTS4 or FTS5, and outputs the correct SQL for ordering by relevance depending on the search type.

The FTS4 output looks something like this:

.. code-block:: sql

    with original as (
        select
            rowid,
            [title],
            [author]
        from [articles]
    )
    select
        [original].[title],
        [original].[author]
    from
        [original]
        join [articles_fts] on [original].rowid = [articles_fts].rowid
    where
        [articles_fts] match :query
    order by
        rank_bm25(matchinfo([articles_fts], 'pcnalx'))

This uses the ``rank_bm25()`` custom SQL function from `sqlite-fts4 <https://github.com/simonw/sqlite-fts4>`__. You can register that custom function against a ``Database`` connection using this method:

.. code-block:: python

    db.register_fts4_bm25()

.. _python_api_fts_rebuild:

Rebuilding a full-text search table
===================================

You can rebuild a table using the ``table.rebuild_fts()`` method. This is useful for if the table configuration changes or the indexed data has become corrupted in some way.

.. code-block:: python

    db["dogs"].rebuild_fts()

This method can be called on a table that has been configured for full-text search - ``dogs`` in this instance -  or directly on a ``_fts`` table:

.. code-block:: python

    db["dogs_fts"].rebuild_fts()

This runs the following SQL::

    INSERT INTO dogs_fts (dogs_fts) VALUES ("rebuild");

.. _python_api_fts_optimize:

Optimizing a full-text search table
===================================

Once you have populated a FTS table you can optimize it to dramatically reduce its size like so:

.. code-block:: python

    db["dogs"].optimize()

This runs the following SQL::

    INSERT INTO dogs_fts (dogs_fts) VALUES ("optimize");

.. _python_api_cached_table_counts:

Cached table counts using triggers
==================================

The ``select count(*)`` query in SQLite requires a full scan of the primary key index, and can take an increasingly long time as the table grows larger.

The ``table.enable_counts()`` method can be used to configure triggers to continuously update a record in a ``_counts`` table. This value can then be used to quickly retrieve the count of rows in the associated table.

.. code-block:: python

    db["dogs"].enable_counts()

This will create the ``_counts`` table if it does not already exist, with the following schema:

.. code-block:: sql

    CREATE TABLE [_counts] (
       [table] TEXT PRIMARY KEY,
       [count] INTEGER DEFAULT 0
    )

You can enable cached counts for every table in a database (except for virtual tables and the ``_counts`` table itself) using the database ``enable_counts()`` method:

.. code-block:: python

    db.enable_counts()

Once enabled, table counts will be stored in the ``_counts`` table. The count records will be automatically kept up-to-date by the triggers when rows are added or deleted to the table.

To access these counts you can query the ``_counts`` table directly or you can use the ``db.cached_counts()`` method. This method returns a dictionary mapping tables to their counts::

    >>> db.cached_counts()
    {'global-power-plants': 33643,
     'global-power-plants_fts_data': 136,
     'global-power-plants_fts_idx': 199,
     'global-power-plants_fts_docsize': 33643,
     'global-power-plants_fts_config': 1}

You can pass a list of table names to this method to retrieve just those counts::

    >>> db.cached_counts(["global-power-plants"])
    {'global-power-plants': 33643}

The ``table.count`` property executes a ``select count(*)`` query by default, unless the ``db.use_counts_table`` property is set to ``True``.

You can set ``use_counts_table`` to ``True`` when you instantiate the database object:

.. code-block:: python

    db = Database("global-power-plants.db", use_counts_table=True)

If the property is ``True`` any calls to the ``table.count`` property will first attempt to find the cached count in the ``_counts`` table, and fall back on a ``count(*)`` query if the value is not available or the table is missing.

Calling the ``.enable_counts()`` method on a database or table object will set ``use_counts_table`` to ``True`` for the lifetime of that database object.

If the ``_counts`` table ever becomes out-of-sync with the actual table counts you can repair it using the ``.reset_counts()`` method:

.. code-block:: python

    db.reset_counts()

.. _python_api_create_index:

Creating indexes
================

You can create an index on a table using the ``.create_index(columns)`` method. The method takes a list of columns:

.. code-block:: python

    db["dogs"].create_index(["is_good_dog"])

By default the index will be named ``idx_{table-name}_{columns}``. If you pass ``find_unique_name=True`` and the automatically derived name already exists, an available name will be found by incrementing a suffix number, for example ``idx_items_title_2``.

You can customize the name of the created index by passing the ``index_name`` parameter:

.. code-block:: python

    db["dogs"].create_index(
        ["is_good_dog", "age"],
        index_name="good_dogs_by_age"
    )

To create an index in descending order for a column, wrap the column name in ``db.DescIndex()`` like this:

.. code-block:: python

    from sqlite_utils.db import DescIndex

    db["dogs"].create_index(
        ["is_good_dog", DescIndex("age")],
        index_name="good_dogs_by_age"
    )

You can create a unique index by passing ``unique=True``:

.. code-block:: python

    db["dogs"].create_index(["name"], unique=True)

Use ``if_not_exists=True`` to do nothing if an index with that name already exists.

Pass ``analyze=True`` to run ``ANALYZE`` against the new index after creating it.

.. _python_api_analyze:

Optimizing index usage with ANALYZE
===================================

The `SQLite ANALYZE command <https://www.sqlite.org/lang_analyze.html>`__ builds a table of statistics which the query planner can use to make better decisions about which indexes to use for a given query.

You should run ``ANALYZE`` if your database is large and you do not think your indexes are being efficiently used.

To run ``ANALYZE`` against every index in a database, use this:

.. code-block:: python

    db.analyze()

To run it just against a specific named index, pass the name of the index to that method:

.. code-block:: python

    db.analyze("idx_countries_country_name")

To run against all indexes attached to a specific table, you can either pass the table name to ``db.analyze(...)`` or you can call the method directly on the table, like this:

.. code-block:: python

    db["dogs"].analyze()

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

.. _python_api_register_function:

Registering custom SQL functions
================================

SQLite supports registering custom SQL functions written in Python. The ``db.register_function()`` method lets you register these functions, and keeps track of functions that have already been registered.

If you use it as a method it will automatically detect the name and number of arguments needed by the function:

.. code-block:: python

    from sqlite_utils import Database

    db = Database(memory=True)

    def reverse_string(s):
        return "".join(reversed(list(s)))

    db.register_function(reverse_string)
    print(db.execute('select reverse_string("hello")').fetchone()[0])
    # This prints "olleh"

You can also use the method as a function decorator like so:

.. code-block:: python

    @db.register_function
    def reverse_string(s):
        return "".join(reversed(list(s)))

    print(db.execute('select reverse_string("hello")').fetchone()[0])

By default, the name of the Python function will be used as the name of the SQL function. You can customize this with the ``name=`` keyword argument:

.. code-block:: python

    @db.register_function(name="rev")
    def reverse_string(s):
        return "".join(reversed(list(s)))

    print(db.execute('select rev("hello")').fetchone()[0])

Python 3.8 added the ability to register `deterministic SQLite functions <https://sqlite.org/deterministic.html>`__, allowing you to indicate that a function will return the exact same result for any given inputs and hence allowing SQLite to apply some performance optimizations. You can mark a function as deterministic using ``deterministic=True``, like this:

.. code-block:: python

    @db.register_function(deterministic=True)
    def reverse_string(s):
        return "".join(reversed(list(s)))

If you run this on a version of Python prior to 3.8 your code will still work, but the ``deterministic=True`` parameter will be ignored.

By default registering a function with the same name and number of arguments will have no effect - the ``Database`` instance keeps track of functions that have already been registered and skips registering them if ``@db.register_function`` is called a second time.

If you want to deliberately replace the registered function with a new implementation, use the ``replace=True`` argument:

.. code-block:: python

    @db.register_function(deterministic=True, replace=True)
    def reverse_string(s):
        return s[::-1]

Exceptions that occur inside a user-defined function default to returning the following error::

    Unexpected error: user-defined function raised exception

You can cause ``sqlite3`` to return more useful errors, including the traceback from the custom function, by executing the following before your custom functions are executed:

.. code-block:: python

    from sqlite_utils.utils import sqlite3

    sqlite3.enable_callback_tracebacks(True)

.. _python_api_quote:

Quoting strings for use in SQL
==============================

In almost all cases you should pass values to your SQL queries using the optional ``parameters`` argument to ``db.query()``, as described in :ref:`python_api_parameters`.

If that option isn't relevant to your use-case you can to quote a string for use with SQLite using the ``db.quote()`` method, like so:

::

    >>> db = Database(memory=True)
    >>> db.quote("hello")
    "'hello'"
    >>> db.quote("hello'this'has'quotes")
    "'hello''this''has''quotes'"

.. _python_api_rows_from_file:

Reading rows from a file
========================

The ``sqlite_utils.utils.rows_from_file()`` helper function can read rows (a sequence of dictionaries) from CSV, TSV, JSON or newline-delimited JSON files.

.. autofunction:: sqlite_utils.utils.rows_from_file
   :noindex:

.. _python_api_maximize_csv_field_size_limit:

Setting the maximum CSV field size limit
========================================

Sometimes when working with CSV files that include extremely long fields you may see an error that looks like this::

    _csv.Error: field larger than field limit (131072)

The Python standard library ``csv`` module enforces a field size limit. You can increase that limit using the ``csv.field_size_limit(new_limit)`` method (`documented here <https://docs.python.org/3/library/csv.html#csv.field_size_limit>`__) but if you don't want to pick a new level you may instead want to increase it to the maximum possible.

The maximum possible value for this is not documented, and varies between systems.

Calling ``sqlite_utils.utils.maximize_csv_field_size_limit()`` will set the value to the highest possible for the current system:

.. code-block:: python

    from sqlite_utils.utils import maximize_csv_field_size_limit

    maximize_csv_field_size_limit()


If you need to reset to the original value after calling this function you can do so like this:

.. code-block:: python

    from sqlite_utils.utils import ORIGINAL_CSV_FIELD_SIZE_LIMIT
    import csv

    csv.field_size_limit(ORIGINAL_CSV_FIELD_SIZE_LIMIT)

.. _python_api_typetracker:

Detecting column types using TypeTracker
========================================

Sometimes you may find yourself working with data that lacks type information - data from a CSV file for example.

The ``TypeTracker`` class can be used to try to automatically identify the most likely types for data that is initially represented as strings.

Consider this example:

.. code-block:: python

    import csv, io

    csv_file = io.StringIO("id,name\n1,Cleo\n2,Cardi")
    rows = list(csv.DictReader(csv_file))

    # rows is now this:
    # [{'id': '1', 'name': 'Cleo'}, {'id': '2', 'name': 'Cardi'}]

If we insert this data directly into a table we will get a schema that is entirely ``TEXT`` columns:

.. code-block:: python

    from sqlite_utils import Database

    db = Database(memory=True)
    db["creatures"].insert_all(rows)
    print(db.schema)
    # Outputs:
    # CREATE TABLE [creatures] (
    #    [id] TEXT,
    #    [name] TEXT
    # );

We can detect the best column types using a ``TypeTracker`` instance:

.. code-block:: python

    from sqlite_utils.utils import TypeTracker

    tracker = TypeTracker()
    db["creatures2"].insert_all(tracker.wrap(rows))
    print(tracker.types)
    # Outputs {'id': 'integer', 'name': 'text'}

We can then apply those types to our new table using the :ref:`table.transform() <python_api_transform>` method:

.. code-block:: python

    db["creatures2"].transform(types=tracker.types)
    print(db["creatures2"].schema)
    # Outputs:
    # CREATE TABLE [creatures2] (
    #    [id] INTEGER,
    #    [name] TEXT
    # );

.. _python_api_gis:

SpatiaLite helpers
==================

`SpatiaLite <https://www.gaia-gis.it/fossil/libspatialite/index>`__ is a geographic extension to SQLite (similar to PostgreSQL + PostGIS). Using requires finding, loading and initializing the extension, adding geometry columns to existing tables and optionally creating spatial indexes. The utilities here help streamline that setup.

.. _python_api_gis_init_spatialite:

Initialize SpatiaLite
---------------------

.. automethod:: sqlite_utils.db.Database.init_spatialite
   :noindex:

.. _python_api_gis_find_spatialite:

Finding SpatiaLite
------------------

.. autofunction:: sqlite_utils.utils.find_spatialite

.. _python_api_gis_add_geometry_column:

Adding geometry columns
-----------------------

.. automethod:: sqlite_utils.db.Table.add_geometry_column
   :noindex:

.. _python_api_gis_create_spatial_index:

Creating a spatial index
------------------------

.. automethod:: sqlite_utils.db.Table.create_spatial_index
   :noindex:
