=======
 Table
=======

Tables are accessed using the indexing operator, like so:

.. code-block:: python

    from sqlite_utils.db import Database
    import sqlite3

    database = Database(sqlite3.connect("my_database.db"))
    table = database["my_table"]

If the table does not yet exist, it will be created the first time you attempt to insert or upsert data into it.

Introspection
=============

If you have loaded an existing table, you can use introspection to find out more about it::

    >>> db["PlantType"]
    <sqlite_utils.db.Table at 0x10f5960b8>
    >>> db["PlantType"].count
    3
    >>> db["PlantType"].columns
    [Column(cid=0, name='id', type='INTEGER', notnull=0, default_value=None, is_pk=1),
     Column(cid=1, name='value', type='TEXT', notnull=0, default_value=None, is_pk=0)]
    >>> db["Street_Tree_List"].count
    189144
    >>> db["Street_Tree_List"].foreign_keys
    [ForeignKey(table='Street_Tree_List', column='qLegalStatus', other_table='qLegalStatus', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='qCareAssistant', other_table='qCareAssistant', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='qSiteInfo', other_table='qSiteInfo', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='qSpecies', other_table='qSpecies', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='qCaretaker', other_table='qCaretaker', other_column='id'),
     ForeignKey(table='Street_Tree_List', column='PlantType', other_table='PlantType', other_column='id')]
