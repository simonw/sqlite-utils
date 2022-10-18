.. _reference:

===============
 API reference
===============

.. contents:: :local:
   :class: this-will-duplicate-information-and-it-is-still-useful-here

.. _reference_db_database:

sqlite_utils.db.Database
========================

.. autoclass:: sqlite_utils.db.Database
    :members:
    :undoc-members:
    :special-members: __getitem__
    :exclude-members: use_counts_table, execute_returning_dicts, resolve_foreign_keys

.. _reference_db_queryable:

sqlite_utils.db.Queryable
=========================

:ref:`Table <reference_db_table>` and :ref:`View <reference_db_view>` are  both subclasses of ``Queryable``, providing access to the following methods:

.. autoclass:: sqlite_utils.db.Queryable
    :members:
    :undoc-members:
    :exclude-members: execute_count

.. _reference_db_table:

sqlite_utils.db.Table
=====================

.. autoclass:: sqlite_utils.db.Table
    :members:
    :undoc-members:
    :show-inheritance:
    :exclude-members: guess_foreign_column, value_or_default, build_insert_queries_and_params, insert_chunk, add_missing_columns

.. _reference_db_view:

sqlite_utils.db.View
====================

.. autoclass:: sqlite_utils.db.View
    :members:
    :undoc-members:
    :show-inheritance:

.. _reference_db_other:

Other
=====

.. _reference_db_other_column:

sqlite_utils.db.Column
----------------------

.. autoclass:: sqlite_utils.db.Column

.. _reference_db_other_column_details:

sqlite_utils.db.ColumnDetails
-----------------------------

.. autoclass:: sqlite_utils.db.ColumnDetails

sqlite_utils.utils
==================

.. _reference_utils_hash_record:

sqlite_utils.utils.hash_record
------------------------------

.. autofunction:: sqlite_utils.utils.hash_record

.. _reference_utils_rows_from_file:

sqlite_utils.utils.rows_from_file
---------------------------------

.. autofunction:: sqlite_utils.utils.rows_from_file

.. _reference_utils_typetracker:

sqlite_utils.utils.TypeTracker
------------------------------

.. autoclass:: sqlite_utils.utils.TypeTracker
   :members: wrap, types

.. _reference_utils_chunks:

sqlite_utils.utils.chunks
-------------------------

.. autofunction:: sqlite_utils.utils.chunks

.. _reference_utils_flatten:

sqlite_utils.utils.flatten
--------------------------

.. autofunction:: sqlite_utils.utils.flatten
