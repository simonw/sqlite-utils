.. _plugin_directory:

==================
 Plugin directory
==================

The following plugins are available for ``sqlite-utils``. Install them using
the :ref:`sqlite-utils install <plugins>` command.

SQL functions
=============

* `sqlite-utils-dateutil <https://github.com/simonw/sqlite-utils-dateutil>`__
  adds date utility SQL functions, such as
  ``select dateutil_parse('3rd november')``.
* `sqlite-utils-jq <https://github.com/simonw/sqlite-utils-jq>`__ adds a
  ``jq(document, expression)`` SQL function for running
  `jq <https://jqlang.github.io/jq/>`__ programs against JSON documents
  directly in SQLite.
* `sqlite-utils-ml <https://github.com/rclement/sqlite-utils-ml>`__ by Romain
  Clement adds functions for training machine learning models and running
  predictions directly in SQLite.
* Alex Garcia maintains plugins for his
  `family of SQLite extensions <https://github.com/asg017/sqlite-ecosystem>`__:

  * ``sqlite-utils-sqlite-regex``
  * ``sqlite-utils-sqlite-path``
  * ``sqlite-utils-sqlite-url``
  * ``sqlite-utils-sqlite-ulid``
  * ``sqlite-utils-sqlite-lines``
  * ``sqlite-utils-sqlite-jsonschema``
  * ``sqlite-utils-sqlite-tg``, which provides geospatial functions powered by
    `TG <https://github.com/tidwall/tg>`__

Interactive shells
==================

* `sqlite-utils-litecli <https://github.com/simonw/sqlite-utils-litecli>`__
  adds an interactive SQLite shell with syntax highlighting and
  auto-completion, started using ``sqlite-utils litecli data.db``.
* `sqlite-utils-shell <https://github.com/simonw/sqlite-utils-shell>`__ adds a
  basic interactive SQLite shell. Start an in-memory shell using
  ``sqlite-utils shell``, or open a database using
  ``sqlite-utils shell data.db``.

Database utilities
==================

* `sqlite-utils-fast-fks <https://github.com/simonw/sqlite-utils-fast-fks>`__
  brings back the fast ``db.add_foreign_keys()`` method that directly
  manipulates the ``sqlite_master`` table. It also adds a
  ``sqlite-utils fast-fks`` command.
* `sqlite-utils-move-tables <https://github.com/simonw/sqlite-utils-move-tables>`__
  adds ``sqlite-utils move-tables`` for moving tables between databases.

Migration support is built into ``sqlite-utils`` itself. See
:ref:`migrations` for the Python API and CLI usage.
