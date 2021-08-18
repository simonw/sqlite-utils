.. _reference:

===============
 API reference
===============

.. contents:: :local:

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
