.. _changelog:

===========
 Changelog
===========

.. _v3_34:

3.34 (2023-07-22)
-----------------

This release introduces a new :ref:`plugin system <plugins>`. (:issue:`567`)

- Documentation describing :ref:`how to build a plugin <plugins_building>`.
- Plugin hook: :ref:`plugins_hooks_register_commands`, for plugins to add extra commands to ``sqlite-utils``. (:issue:`569`)
- Plugin hook: :ref:`plugins_hooks_prepare_connection`. Plugins can use this to help prepare the SQLite connection to do things like registering custom SQL functions. Thanks, `Alex Garcia <https://github.com/asg017>`__. (:issue:`574`)
- ``sqlite_utils.Database(..., execute_plugins=False)`` option for disabling plugin execution. (:issue:`575`)
- ``sqlite-utils install -e path-to-directory`` option for installing editable code. This option is useful during the development of a plugin. (:issue:`570`)
- ``table.create(...)`` method now accepts ``replace=True`` to drop and replace an existing table with the same name, or ``ignore=True`` to silently do nothing if a table already exists with the same name. (:issue:`568`)
- ``sqlite-utils insert ... --stop-after 10`` option for stopping the insert after a specified number of records. Works for the ``upsert`` command as well. (:issue:`561`)
- The ``--csv`` and ``--tsv`` modes for ``insert`` now accept a ``--empty-null`` option, which cases empty strings in the CSV file to be stored as ``null`` in the database. (:issue:`563`)
- New ``db.rename_table(table_name, new_name)`` method for renaming tables. (:issue:`565`)
- ``sqlite-utils rename-table my.db table_name new_name`` command for renaming tables. (:issue:`565`)
- The ``table.transform(...)`` method now takes an optional ``keep_table=new_table_name`` parameter, which will cause the original table to be renamed to ``new_table_name`` rather than being dropped at the end of the transformation. (:issue:`571`)
- Documentation now notes that calling ``table.transform()`` without any arguments will reformat the SQL schema stored by SQLite to be more aesthetically pleasing. (:issue:`564`)

.. _v3_33:

3.33 (2023-06-25)
-----------------

- ``sqlite-utils`` will now use `sqlean.py <https://github.com/nalgeon/sqlean.py>`__ in place of ``sqlite3`` if it is installed in the same virtual environment. This is useful for Python environments with either an outdated version of SQLite or with restrictions on SQLite such as disabled extension loading or restrictions resulting in the ``sqlite3.OperationalError: table sqlite_master may not be modified`` error. (:issue:`559`)
- New ``with db.ensure_autocommit_off()`` context manager, which ensures that the database is in autocommit mode for the duration of a block of code. This is used by ``db.enable_wal()`` and ``db.disable_wal()`` to ensure they work correctly with ``pysqlite3`` and ``sqlean.py``.
- New ``db.iterdump()`` method, providing an iterator over SQL strings representing a dump of the database. This uses ``sqlite-dump`` if it is available, otherwise falling back on the ``conn.iterdump()`` method from ``sqlite3``. Both ``pysqlite3`` and ``sqlean.py`` omit support for ``iterdump()`` - this method helps paper over that difference.

.. _v3_32_1:

3.32.1 (2023-05-21)
-------------------

- Examples in the :ref:`CLI documentation <cli>` can now all be copied and pasted without needing to remove a leading ``$``. (:issue:`551`)
- Documentation now covers :ref:`installation_completion` for ``bash`` and ``zsh``. (:issue:`552`)

.. _v3_32:

3.32 (2023-05-21)
-----------------

- New experimental ``sqlite-utils tui`` interface for interactively building command-line invocations, powered by `Trogon <https://github.com/Textualize/trogon>`__. This requires an optional dependency, installed using ``sqlite-utils install trogon``. There is a screenshot :ref:`in the documentation <cli_tui>`. (:issue:`545`)
- ``sqlite-utils analyze-tables`` command (:ref:`documentation <cli_analyze_tables>`) now has a ``--common-limit 20`` option for changing the number of common/least-common values shown for each column. (:issue:`544`)
- ``sqlite-utils analyze-tables --no-most`` and ``--no-least`` options for disabling calculation of most-common and least-common values.
- If a column contains only ``null`` values, ``analyze-tables`` will no longer attempt to calculate the most common and least common values for that column. (:issue:`547`)
- Calling ``sqlite-utils analyze-tables`` with non-existent columns in the ``-c/--column`` option now results in an error message. (:issue:`548`)
- The ``table.analyze_column()`` method (:ref:`documented here <python_api_analyze_column>`) now accepts ``most_common=False`` and ``least_common=False`` options for disabling calculation of those values.

.. _v3_31:

3.31 (2023-05-08)
-----------------

- Dropped support for Python 3.6. Tests now ensure compatibility with Python 3.11. (:issue:`517`)
- Automatically locates the SpatiaLite extension on Apple Silicon. Thanks, Chris Amico. (`#536 <https://github.com/simonw/sqlite-utils/pull/536>`__)
- New ``--raw-lines`` option for the ``sqlite-utils query`` and ``sqlite-utils memory`` commands, which outputs just the raw value of the first column of every row. (:issue:`539`)
- Fixed a bug where ``table.upsert_all()`` failed if the ``not_null=`` option was passed. (:issue:`538`)
- Fixed a ``ResourceWarning`` when using ``sqlite-utils insert``. (:issue:`534`)
- Now shows a more detailed error message when ``sqlite-utils insert`` is called with invalid JSON. (:issue:`532`)
- ``table.convert(..., skip_false=False)`` and ``sqlite-utils convert --no-skip-false`` options, for avoiding a misfeature where the :ref:`convert()  <python_api_convert>` mechanism skips rows in the database with a falsey value for the specified column. Fixing this by default would be a backwards-incompatible change and is under consideration for a 4.0 release in the future. (:issue:`527`)
- Tables can now be created with self-referential foreign keys. Thanks, Scott Perry. (`#537 <https://github.com/simonw/sqlite-utils/pull/537>`__)
- ``sqlite-utils transform`` no longer breaks if a table defines default values for columns. Thanks, Kenny Song. (:issue:`509`)
- Fixed a bug where repeated calls to ``table.transform()`` did not work correctly. Thanks, Martin Carpenter. (:issue:`525`)
- Improved error message if ``rows_from_file()`` is passed a non-binary-mode file-like object. (:issue:`520`)

.. _v3_30:

3.30 (2022-10-25)
-----------------

- Now tested against Python 3.11. (:issue:`502`)
- New ``table.search_sql(include_rank=True)`` option, which adds a ``rank`` column to the generated SQL. Thanks, Jacob Chapman. (`#480 <https://github.com/simonw/sqlite-utils/pull/480>`__)
- Progress bars now display for newline-delimited JSON files using the ``--nl`` option. Thanks, Mischa Untaga. (:issue:`485`)
- New ``db.close()`` method. (:issue:`504`)
- Conversion functions passed to :ref:`table.convert(...) <python_api_convert>` can now return lists or dictionaries, which will be inserted into the database as JSON strings. (:issue:`495`)
- ``sqlite-utils install`` and ``sqlite-utils uninstall`` commands for installing packages into the same virtual environment as ``sqlite-utils``, :ref:`described here <cli_install>`. (:issue:`483`)
- New :ref:`sqlite_utils.utils.flatten() <reference_utils_flatten>` utility function. (:issue:`500`)
- Documentation on :ref:`using Just <contributing_just>` to run tests, linters and build documentation. 
- Documentation now covers the :ref:`release_process` for this package.

.. _v3_29:

3.29 (2022-08-27)
-----------------

- The ``sqlite-utils query``, ``memory`` and ``bulk`` commands now all accept a new ``--functions`` option. This can be passed a string of Python code, and any callable objects defined in that code will be made available to SQL queries as custom SQL functions. See :ref:`cli_query_functions` for details. (:issue:`471`)
- ``db[table].create(...)`` method now accepts a new ``transform=True`` parameter. If the table already exists it will be :ref:`transformed <python_api_transform>` to match the schema configuration options passed to the function. This may result in columns being added or dropped, column types being changed, column order being updated or not null and default values for columns being set. (:issue:`467`)
- Related to the above, the ``sqlite-utils create-table`` command now accepts a ``--transform`` option.
- New introspection property: ``table.default_values`` returns a dictionary mapping each column name with a default value to the configured default value. (:issue:`475`)
- The ``--load-extension`` option can now be provided a path to a compiled SQLite extension module accompanied by the name of an entrypoint, separated by a colon - for example ``--load-extension ./lines0:sqlite3_lines0_noread_init``. This feature is modelled on code first `contributed to Datasette <https://github.com/simonw/datasette/pull/1789>`__ by Alex Garcia. (:issue:`470`)
- Functions registered using the :ref:`db.register_function() <python_api_register_function>` method can now have a custom name specified using the new ``db.register_function(fn, name=...)`` parameter. (:issue:`458`)
- :ref:`sqlite-utils rows <cli_rows>` has a new ``--order`` option for specifying the sort order for the returned rows. (:issue:`469`)
- All of the CLI options that accept Python code blocks can now all be used to define functions that can access modules imported in that same block of code without needing to use the ``global`` keyword. (:issue:`472`)
- Fixed bug where ``table.extract()`` would not behave correctly for columns containing null values. Thanks, Forest Gregg. (:issue:`423`)
- New tutorial: `Cleaning data with sqlite-utils and Datasette <https://datasette.io/tutorials/clean-data>`__ shows how to use ``sqlite-utils`` to import and clean an example CSV file.
- Datasette and ``sqlite-utils`` now have a Discord community. `Join the Discord here <https://discord.gg/Ass7bCAMDw>`__.

.. _v3_28:

3.28 (2022-07-15)
-----------------

- New :ref:`table.duplicate(new_name) <python_api_duplicate>` method for creating a copy of a table with a matching schema and row contents. Thanks, `David <https://github.com/davidleejy>`__. (:issue:`449`)
- New ``sqlite-utils duplicate data.db table_name new_name`` CLI command for :ref:`cli_duplicate_table`. (:issue:`454`)
- ``sqlite_utils.utils.rows_from_file()`` is now a :ref:`documented API <reference_utils_rows_from_file>`. It can be used to read a sequence of dictionaries from a file-like object containing CSV, TSV, JSON or newline-delimited JSON. It can be passed an explicit format or can attempt to detect the format automatically. (:issue:`443`)
- ``sqlite_utils.utils.TypeTracker`` is now a documented API for detecting the likely column types for a sequence of string rows, see :ref:`python_api_typetracker`. (:issue:`445`)
- ``sqlite_utils.utils.chunks()`` is now a documented API for :ref:`splitting an iterator into chunks  <reference_utils_chunks>`. (:issue:`451`)
- ``sqlite-utils enable-fts`` now has a ``--replace`` option for replacing the existing FTS configuration for a table. (:issue:`450`)
- The ``create-index``, ``add-column`` and ``duplicate`` commands all now take a ``--ignore`` option for ignoring errors should the database not be in the right state for them to operate. (:issue:`450`)

.. _v3_27:

3.27 (2022-06-14)
-----------------

See also `the annotated release notes <https://simonwillison.net/2022/Jun/19/weeknotes/#sqlite-utils-3-27>`__ for this release.

- Documentation now uses the `Furo <https://github.com/pradyunsg/furo>`__ Sphinx theme. (:issue:`435`)
- Code examples in documentation now have a "copy to clipboard" button. (:issue:`436`)
- ``sqlite_utils.utils.utils.rows_from_file()`` is now a documented API, see :ref:`python_api_rows_from_file`. (:issue:`443`)
- ``rows_from_file()`` has two new parameters to help handle CSV files with rows that contain more values than are listed in that CSV file's headings: ``ignore_extras=True`` and ``extras_key="name-of-key"``. (:issue:`440`)
- ``sqlite_utils.utils.maximize_csv_field_size_limit()`` helper function for increasing the field size limit for reading CSV files to its maximum, see :ref:`python_api_maximize_csv_field_size_limit`. (:issue:`442`)
- ``table.search(where=, where_args=)`` parameters for adding additional ``WHERE`` clauses to a search query. The ``where=`` parameter is available on ``table.search_sql(...)`` as well. See :ref:`python_api_fts_search`. (:issue:`441`)
- Fixed bug where ``table.detect_fts()`` and other search-related functions could fail if two FTS-enabled tables had names that were prefixes of each other. (:issue:`434`)

.. _v3_26_1:

3.26.1 (2022-05-02)
-------------------

- Now depends on `click-default-group-wheel <https://github.com/simonw/click-default-group-wheel>`__, a pure Python wheel package. This means you can install and use this package with `Pyodide <https://pyodide.org/>`__, which can run Python entirely in your browser using WebAssembly. (`#429 <https://github.com/simonw/sqlite-utils/pull/429>`__)

  Try that out using the `Pyodide REPL <https://pyodide.org/en/stable/console.html>`__:

  .. code-block:: python

      >>> import micropip
      >>> await micropip.install("sqlite-utils")
      >>> import sqlite_utils
      >>> db = sqlite_utils.Database(memory=True)
      >>> list(db.query("select 3 * 5"))
      [{'3 * 5': 15}]

.. _v3_26:

3.26 (2022-04-13)
-----------------

- New ``errors=r.IGNORE/r.SET_NULL`` parameter for the ``r.parsedatetime()`` and ``r.parsedate()`` :ref:`convert recipes <cli_convert_recipes>`. (:issue:`416`)
- Fixed a bug where ``--multi`` could not be used in combination with ``--dry-run`` for the :ref:`convert <cli_convert>` command. (:issue:`415`)
- New documentation: :ref:`cli_convert_complex`. (:issue:`420`)
- More robust detection for whether or not ``deterministic=True`` is supported. (:issue:`425`)

.. _v3_25_1:

3.25.1 (2022-03-11)
-------------------

- Improved display of type information and parameters in the :ref:`API reference documentation <reference>`. (:issue:`413`)

.. _v3_25:

3.25 (2022-03-01)
-----------------

- New ``hash_id_columns=`` parameter for creating a primary key that's a hash of the content of specific columns - see :ref:`python_api_hash` for details. (:issue:`343`)
- New :ref:`db.sqlite_version <python_api_sqlite_version>` property, returning a tuple of integers representing the version of SQLite, for example ``(3, 38, 0)``.
- Fixed a bug where :ref:`register_function(deterministic=True) <python_api_register_function>` caused errors on versions of SQLite prior to 3.8.3. (:issue:`408`)
- New documented :ref:`hash_record(record, keys=...) <reference_utils_hash_record>` function.

.. _v3_24:

3.24 (2022-02-15)
-----------------

- SpatiaLite helpers for the ``sqlite-utils`` command-line tool - thanks, Chris Amico. (:issue:`398`)

  - :ref:`sqlite-utils create-database <cli_create_database>` ``--init-spatialite`` option for initializing SpatiaLite on a newly created database.
  - :ref:`sqlite-utils add-geometry-column <cli_spatialite>` command for adding geometry columns.
  - :ref:`sqlite-utils create-spatial-index <cli_spatialite_indexes>` command for adding spatial indexes.

- ``db[table].create(..., if_not_exists=True)`` option for :ref:`creating a table <python_api_explicit_create>` only if it does not already exist. (:issue:`397`)
- ``Database(memory_name="my_shared_database")`` parameter for creating a :ref:`named in-memory database <python_api_connect>` that can be shared between multiple connections. (:issue:`405`)
- Documentation now describes :ref:`how to add a primary key to a rowid table <cli_transform_table_add_primary_key_to_rowid>` using ``sqlite-utils transform``. (:issue:`403`)

.. _v3_23:

3.23 (2022-02-03)
-----------------

This release introduces four new utility methods for working with `SpatiaLite <https://www.gaia-gis.it/fossil/libspatialite/index>`__. Thanks, Chris Amico. (`#385 <https://github.com/simonw/sqlite-utils/pull/385>`__)

- ``sqlite_utils.utils.find_spatialite()`` :ref:`finds the location of the SpatiaLite module <python_api_gis_find_spatialite>` on disk.
- ``db.init_spatialite()`` :ref:`initializes SpatiaLite <python_api_gis_init_spatialite>` for the given database.
- ``table.add_geometry_column(...)`` :ref:`adds a geometry column <python_api_gis_add_geometry_column>` to an existing table.
- ``table.create_spatial_index(...)`` :ref:`creates a spatial index <python_api_gis_create_spatial_index>` for a column.
- ``sqlite-utils batch`` now accepts a ``--batch-size`` option. (:issue:`392`)

.. _v3_22_1:

3.22.1 (2022-01-25)
-------------------

- All commands now include example usage in their ``--help`` - see :ref:`cli_reference`. (:issue:`384`)
- Python library documentation has a new :ref:`python_api_getting_started` section. (:issue:`387`)
- Documentation now uses `Plausible analytics <https://plausible.io/>`__. (:issue:`389`)

.. _v3_22:

3.22 (2022-01-11)
-----------------

- New :ref:`cli_reference` documentation page, listing the output of ``--help`` for every one of the CLI commands. (:issue:`383`)
- ``sqlite-utils rows`` now has ``--limit`` and ``--offset`` options for paginating through data. (:issue:`381`)
- ``sqlite-utils rows`` now has ``--where`` and ``-p`` options for filtering the table using a ``WHERE`` query, see :ref:`cli_rows`. (:issue:`382`)

.. _v3_21:

3.21 (2022-01-10)
-----------------

CLI and Python library improvements to help run `ANALYZE <https://www.sqlite.org/lang_analyze.html>`__ after creating indexes or inserting rows, to gain better performance from the SQLite query planner when it runs against indexes.

Three new CLI commands: ``create-database``, ``analyze`` and ``bulk``.

More details and examples can be found in `the annotated release notes <https://simonwillison.net/2022/Jan/11/sqlite-utils/>`__.

- New ``sqlite-utils create-database`` command for creating new empty database files. (:issue:`348`)
- New Python methods for running ``ANALYZE`` against a database, table or index: ``db.analyze()`` and ``table.analyze()``, see :ref:`python_api_analyze`. (:issue:`366`)
- New :ref:`sqlite-utils analyze command <cli_analyze>` for running ``ANALYZE`` using the CLI. (:issue:`379`)
- The ``create-index``, ``insert`` and ``upsert`` commands now have a new ``--analyze`` option for running ``ANALYZE`` after the command has completed. (:issue:`379`)
- New :ref:`sqlite-utils bulk command <cli_bulk>` which can import records in the same way as ``sqlite-utils insert`` (from JSON, CSV or TSV) and use them to bulk execute a parametrized SQL query. (:issue:`375`)
- The CLI tool can now also be run using ``python -m sqlite_utils``. (:issue:`368`)
- Using ``--fmt`` now implies ``--table``, so you don't need to pass both options. (:issue:`374`)
- The ``--convert`` function applied to rows can now modify the row in place. (:issue:`371`)
- The :ref:`insert-files command <cli_insert_files>` supports two new columns: ``stem`` and ``suffix``. (:issue:`372`)
- The ``--nl`` import option now ignores blank lines in the input. (:issue:`376`)
- Fixed bug where streaming input to the ``insert`` command with ``--batch-size 1`` would appear to only commit after several rows had been ingested, due to unnecessary input buffering. (:issue:`364`)

.. _v3_20:

3.20 (2022-01-05)
-----------------

- ``sqlite-utils insert ... --lines`` to insert the lines from a file into a table with a single ``line`` column, see :ref:`cli_insert_unstructured`.
- ``sqlite-utils insert ... --text`` to insert the contents of the file into a table with a single ``text`` column and a single row.
- ``sqlite-utils insert ... --convert`` allows a Python function to be provided that will be used to convert each row that is being inserted into the database. See :ref:`cli_insert_convert`, including details on special behavior when combined with ``--lines`` and ``--text``. (:issue:`356`)
- ``sqlite-utils convert`` now accepts a code value of ``-`` to read code from standard input. (:issue:`353`)
- ``sqlite-utils convert`` also now accepts code that defines a named ``convert(value)`` function, see :ref:`cli_convert`.
- ``db.supports_strict`` property showing if the database connection supports `SQLite strict tables <https://www.sqlite.org/stricttables.html>`__.
- ``table.strict`` property (see :ref:`python_api_introspection_strict`) indicating if the table uses strict mode. (:issue:`344`)
- Fixed bug where ``sqlite-utils upsert ... --detect-types`` ignored the ``--detect-types`` option. (:issue:`362`)

.. _v3_19:

3.19 (2021-11-20)
-----------------

- The :ref:`table.lookup() method <python_api_lookup_tables>` now accepts keyword arguments that match those on the underlying ``table.insert()`` method: ``foreign_keys=``, ``column_order=``, ``not_null=``, ``defaults=``, ``extracts=``, ``conversions=`` and ``columns=``. You can also now pass ``pk=`` to specify a different column name to use for the primary key. (:issue:`342`)

.. _v3_18:

3.18 (2021-11-14)
-----------------

- The ``table.lookup()`` method now has an optional second argument which can be used to populate columns only the first time the record is created, see :ref:`python_api_lookup_tables`. (:issue:`339`)
- ``sqlite-utils memory`` now has a ``--flatten`` option for :ref:`flattening nested JSON objects <cli_inserting_data_flatten>` into separate columns, consistent with ``sqlite-utils insert``. (:issue:`332`)
- ``table.create_index(..., find_unique_name=True)`` parameter, which finds an available name for the created index even if the default name has already been taken. This means that ``index-foreign-keys`` will work even if one of the indexes it tries to create clashes with an existing index name. (:issue:`335`)
- Added ``py.typed`` to the module, so `mypy <http://mypy-lang.org/>`__ should now correctly pick up the type annotations. Thanks, Andreas Longo. (:issue:`331`)
- Now depends on ``python-dateutil`` instead of depending on ``dateutils``. Thanks, Denys Pavlov. (:issue:`324`)
- ``table.create()`` (see :ref:`python_api_explicit_create`) now handles ``dict``, ``list`` and ``tuple`` types, mapping them to ``TEXT`` columns in SQLite so that they can be stored encoded as JSON. (:issue:`338`)
- Inserted data with square braces in the column names (for example a CSV file containing a ``item[price]``) column now have the braces converted to underscores: ``item_price_``. Previously such columns would be rejected with an error. (:issue:`329`)
- Now also tested against Python 3.10. (`#330 <https://github.com/simonw/sqlite-utils/pull/330>`__)

.. _v3_17.1:

3.17.1 (2021-09-22)
-------------------

- :ref:`sqlite-utils memory <cli_memory>` now works if files passed to it share the same file name. (:issue:`325`)
- :ref:`sqlite-utils query <cli_query>` now returns ``[]`` in JSON mode if no rows are returned. (:issue:`328`)

.. _v3_17:

3.17 (2021-08-24)
-----------------

- The :ref:`sqlite-utils memory <cli_memory>` command has a new ``--analyze`` option, which runs the equivalent of the :ref:`analyze-tables <cli_analyze_tables>` command directly against the in-memory database created from the incoming CSV or JSON data. (:issue:`320`)
- :ref:`sqlite-utils insert-files <cli_insert_files>` now has the ability to insert file contents in to ``TEXT`` columns in addition to the default ``BLOB``. Pass the ``--text`` option or use ``content_text`` as a column specifier. (:issue:`319`)

.. _v3_16:

3.16 (2021-08-18)
-----------------

- Type signatures added to  more methods, including ``table.resolve_foreign_keys()``, ``db.create_table_sql()``, ``db.create_table()`` and ``table.create()``. (:issue:`314`)
- New ``db.quote_fts(value)`` method, see :ref:`python_api_quote_fts` - thanks, Mark Neumann. (:issue:`246`)
- ``table.search()`` now accepts an optional ``quote=True`` parameter. (:issue:`296`)
- CLI command ``sqlite-utils search`` now accepts a ``--quote`` option. (:issue:`296`)
- Fixed bug where ``--no-headers`` and ``--tsv`` options to :ref:`sqlite-utils insert <cli_insert_csv_tsv>` could not be used together. (:issue:`295`)
- Various small improvements to :ref:`reference` documentation.

.. _v3_15.1:

3.15.1 (2021-08-10)
-------------------

- Python library now includes type annotations on almost all of the methods, plus detailed docstrings describing each one. (:issue:`311`)
- New :ref:`reference` documentation page, powered by those docstrings.
- Fixed bug where ``.add_foreign_keys()`` failed to raise an error if called against a ``View``. (:issue:`313`)
- Fixed bug where ``.delete_where()`` returned a ``[]`` instead of returning ``self`` if called against a non-existent table. (:issue:`315`)

.. _v3_15:

3.15 (2021-08-09)
-----------------

- ``sqlite-utils insert --flatten`` option for :ref:`flattening nested JSON objects <cli_inserting_data_flatten>` to create tables with column names like ``topkey_nestedkey``. (:issue:`310`)
- Fixed several spelling mistakes in the documentation, spotted `using codespell <https://til.simonwillison.net/python/codespell>`__.
- Errors that occur while using the ``sqlite-utils`` CLI tool now show the responsible SQL and query parameters, if possible. (:issue:`309`)

.. _v3_14:

3.14 (2021-08-02)
-----------------

This release introduces the new :ref:`sqlite-utils convert command <cli_convert>` (:issue:`251`) and corresponding :ref:`table.convert(...) <python_api_convert>` Python method (:issue:`302`). These tools can be used to apply a Python conversion function to one or more columns of a table, either updating the column in place or using transformed data from that column to populate one or more other columns.

This command-line example uses the Python standard library `textwrap module <https://docs.python.org/3/library/textwrap.html>`__ to wrap the content of the ``content`` column in the ``articles`` table to 100 characters::

    $ sqlite-utils convert content.db articles content \
        '"\n".join(textwrap.wrap(value, 100))' \
        --import=textwrap

The same operation in Python code looks like this:

.. code-block:: python

    import sqlite_utils, textwrap

    db = sqlite_utils.Database("content.db")
    db["articles"].convert("content", lambda v: "\n".join(textwrap.wrap(v, 100)))

See the full documentation for the :ref:`sqlite-utils convert command <cli_convert>` and the :ref:`table.convert(...) <python_api_convert>` Python method for more details.

Also in this release:

- The new ``table.count_where(...)`` method, for counting rows in a table that match a specific SQL ``WHERE`` clause. (:issue:`305`)
- New ``--silent`` option for the :ref:`sqlite-utils insert-files command <cli_insert_files>` to hide the terminal progress bar, consistent with the ``--silent`` option for ``sqlite-utils convert``. (:issue:`301`)

.. _v3_13:

3.13 (2021-07-24)
-----------------

- ``sqlite-utils schema my.db table1 table2`` command now accepts optional table names. (:issue:`299`)
- ``sqlite-utils memory --help`` now describes the ``--schema`` option.

.. _v3_12:

3.12 (2021-06-25)
-----------------

- New :ref:`db.query(sql, params) <python_api_query>` method, which executes a SQL query and returns the results as an iterator over Python dictionaries. (:issue:`290`)
- This project now uses ``flake8`` and has started to use ``mypy``. (:issue:`291`)
- New documentation on :ref:`contributing <contributing>` to this project. (:issue:`292`)

.. _v3_11:

3.11 (2021-06-20)
-----------------

- New ``sqlite-utils memory data.csv --schema`` option, for outputting the schema of the in-memory database generated from one or more files. See :ref:`cli_memory_schema_dump_save`. (:issue:`288`)
- Added :ref:`installation instructions <installation>`. (:issue:`286`)

.. _v3_10:

3.10 (2021-06-19)
-----------------

This release introduces the ``sqlite-utils memory`` command, which can be used to load CSV or JSON data into a temporary in-memory database and run SQL queries (including joins across multiple files) directly against that data.

Also new: ``sqlite-utils insert --detect-types``, ``sqlite-utils dump``, ``table.use_rowid`` plus some smaller fixes.

sqlite-utils memory
~~~~~~~~~~~~~~~~~~~

This example of ``sqlite-utils memory`` retrieves information about the all of the repositories in the `Dogsheep <https://github.com/dogsheep>`__ organization on GitHub using `this JSON API <https://api.github.com/users/dogsheep/repos>`__, sorts them by their number of stars and outputs a table of the top five (using ``-t``)::

    $ curl -s 'https://api.github.com/users/dogsheep/repos' \
      | sqlite-utils memory - '
          select full_name, forks_count, stargazers_count
          from stdin order by stargazers_count desc limit 5
        ' -t
    full_name                            forks_count    stargazers_count
    ---------------------------------  -------------  ------------------
    dogsheep/twitter-to-sqlite                    12                 225
    dogsheep/github-to-sqlite                     14                 139
    dogsheep/dogsheep-photos                       5                 116
    dogsheep/dogsheep.github.io                    7                  90
    dogsheep/healthkit-to-sqlite                   4                  85

The tool works against files on disk as well. This example joins data from two CSV files::

    $ cat creatures.csv
    species_id,name
    1,Cleo
    2,Bants
    2,Dori
    2,Azi
    $ cat species.csv
    id,species_name
    1,Dog
    2,Chicken
    $ sqlite-utils memory species.csv creatures.csv '
      select * from creatures join species on creatures.species_id = species.id
    '
    [{"species_id": 1, "name": "Cleo", "id": 1, "species_name": "Dog"},
     {"species_id": 2, "name": "Bants", "id": 2, "species_name": "Chicken"},
     {"species_id": 2, "name": "Dori", "id": 2, "species_name": "Chicken"},
     {"species_id": 2, "name": "Azi", "id": 2, "species_name": "Chicken"}]

Here the ``species.csv`` file becomes the ``species`` table, the ``creatures.csv`` file becomes the ``creatures`` table and the output is JSON, the default output format.

You can also use the ``--attach`` option to attach existing SQLite database files to the in-memory database, in order to join data from CSV or JSON directly against your existing tables.

Full documentation of this new feature is available in :ref:`cli_memory`. (:issue:`272`)

sqlite-utils insert \-\-detect-types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :ref:`sqlite-utils insert <cli_inserting_data>` command can be used to insert data from JSON, CSV or TSV files into a SQLite database file. The new ``--detect-types`` option (shortcut ``-d``), when used in conjunction with a CSV or TSV import, will automatically detect if columns in the file are integers or floating point numbers as opposed to treating everything as a text column and create the new table with the corresponding schema. See :ref:`cli_insert_csv_tsv` for details. (:issue:`282`)

Other changes
~~~~~~~~~~~~~

- **Bug fix**: ``table.transform()``, when run against a table without explicit primary keys, would incorrectly create a new version of the table with an explicit primary key column called ``rowid``. (:issue:`284`)
- New ``table.use_rowid`` introspection property, see :ref:`python_api_introspection_use_rowid`. (:issue:`285`)
- The new ``sqlite-utils dump file.db`` command outputs a SQL dump that can be used to recreate a database. (:issue:`274`)
- ``-h`` now works as a shortcut for ``--help``, thanks Loren McIntyre. (:issue:`276`)
- Now using `pytest-cov <https://pytest-cov.readthedocs.io/>`__ and `Codecov <https://about.codecov.io/>`__ to track test coverage - currently at 96%. (:issue:`275`)
- SQL errors that occur when using ``sqlite-utils query`` are now displayed as CLI errors.

.. _v3_9_1:

3.9.1 (2021-06-12)
------------------

- Fixed bug when using ``table.upsert_all()`` to create a table with only a single column that is treated as the primary key. (:issue:`271`)

.. _v3_9:

3.9 (2021-06-11)
----------------

- New ``sqlite-utils schema`` command showing the full SQL schema for a database, see :ref:`Showing the schema (CLI)<cli_schema>`. (:issue:`268`)
- ``db.schema`` introspection property exposing the same feature to the Python library, see :ref:`Showing the schema (Python library) <python_api_schema>`.

.. _v3_8:

3.8 (2021-06-02)
----------------

- New ``sqlite-utils indexes`` command to list indexes in a database, see :ref:`cli_indexes`. (:issue:`263`)
- ``table.xindexes`` introspection property returning more details about that table's indexes, see :ref:`python_api_introspection_xindexes`. (:issue:`261`)

.. _v3_7:

3.7 (2021-05-28)
----------------

- New ``table.pks_and_rows_where()`` method returning ``(primary_key, row_dictionary)`` tuples - see :ref:`python_api_pks_and_rows_where`. (:issue:`240`)
- Fixed bug with ``table.add_foreign_key()`` against columns containing spaces. (:issue:`238`)
- ``table_or_view.drop(ignore=True)`` option for avoiding errors if the table or view does not exist. (:issue:`237`)
- ``sqlite-utils drop-view --ignore`` and ``sqlite-utils drop-table --ignore`` options. (:issue:`237`)
- Fixed a bug with inserts of nested JSON containing non-ascii strings - thanks, Dylan Wu. (:issue:`257`)
- Suggest ``--alter`` if an error occurs caused by a missing column. (:issue:`259`)
- Support creating indexes with columns in descending order, see :ref:`API documentation <python_api_create_index>` and :ref:`CLI documentation <cli_create_index>`. (:issue:`260`)
- Correctly handle CSV files that start with a UTF-8 BOM. (:issue:`250`)

.. _v3_6:

3.6 (2021-02-18)
----------------

This release adds the ability to execute queries joining data from more than one database file - similar to the cross database querying feature introduced in `Datasette 0.55 <https://docs.datasette.io/en/stable/changelog.html#v0-55>`__.

- The ``db.attach(alias, filepath)`` Python method can be used to attach extra databases to the same connection, see :ref:`db.attach() in the Python API documentation <python_api_attach>`. (:issue:`113`)
- The ``--attach`` option attaches extra aliased databases to run SQL queries against directly on the command-line, see :ref:`attaching additional databases in the CLI documentation <cli_query_attach>`. (:issue:`236`)

.. _v3_5:

3.5 (2021-02-14)
----------------

- ``sqlite-utils insert --sniff`` option for detecting the delimiter and quote character used by a CSV file, see :ref:`cli_insert_csv_tsv_delimiter`. (:issue:`230`)
- The ``table.rows_where()``, ``table.search()`` and ``table.search_sql()`` methods all now take optional ``offset=`` and ``limit=`` arguments. (:issue:`231`)
- New ``--no-headers`` option for ``sqlite-utils insert --csv`` to handle CSV files that are missing the header row, see :ref:`cli_insert_csv_tsv_no_header`. (:issue:`228`)
- Fixed bug where inserting data with extra columns in subsequent chunks would throw an error. Thanks `@nieuwenhoven <https://github.com/nieuwenhoven>`__ for the fix. (:issue:`234`)
- Fixed bug importing CSV files with columns containing more than 128KB of data. (:issue:`229`)
- Test suite now runs in CI against Ubuntu, macOS and Windows. Thanks `@nieuwenhoven <https://github.com/nieuwenhoven>`__ for the Windows test fixes. (:issue:`232`)

.. _v3_4_1:

3.4.1 (2021-02-05)
------------------

- Fixed a code import bug that slipped in to 3.4. (:issue:`226`)

.. _v3_4:

3.4 (2021-02-05)
----------------

- ``sqlite-utils insert --csv`` now accepts optional ``--delimiter`` and ``--quotechar`` options. See :ref:`cli_insert_csv_tsv_delimiter`. (:issue:`223`)

.. _v3_3:

3.3 (2021-01-17)
----------------

- The ``table.m2m()`` method now accepts an optional ``alter=True`` argument to specify that any missing columns should be added to the referenced table. See :ref:`python_api_m2m`. (:issue:`222`)

.. _v3_2_1:

3.2.1 (2021-01-12)
------------------

- Fixed a bug where ``.add_missing_columns()`` failed to take case insensitive column names into account. (:issue:`221`)

.. _v3_2:

3.2 (2021-01-03)
----------------

This release introduces a new mechanism for speeding up ``count(*)`` queries using cached table counts, stored in a ``_counts`` table and updated by triggers. This mechanism is described in :ref:`python_api_cached_table_counts`, and can be enabled using Python API methods or the new ``enable-counts`` CLI command. (:issue:`212`)

- ``table.enable_counts()`` method for enabling these triggers on a specific table.
- ``db.enable_counts()`` method for enabling triggers on every table in the database. (:issue:`213`)
- New ``sqlite-utils enable-counts my.db`` command for enabling counts on all or specific tables, see :ref:`cli_enable_counts`. (:issue:`214`)
- New ``sqlite-utils triggers`` command for listing the triggers defined for a database or specific tables, see :ref:`cli_triggers`. (:issue:`218`)
- New ``db.use_counts_table`` property which, if ``True``, causes ``table.count`` to read from the ``_counts`` table. (:issue:`215`)
- ``table.has_counts_triggers`` property revealing if a table has been configured with the new ``_counts`` database triggers.
- ``db.reset_counts()`` method and ``sqlite-utils reset-counts`` command for resetting the values in the ``_counts`` table. (:issue:`219`)
- The previously undocumented ``db.escape()`` method has been renamed to ``db.quote()`` and is now covered by the documentation: :ref:`python_api_quote`. (:issue:`217`)
- New ``table.triggers_dict`` and ``db.triggers_dict`` introspection properties. (:issue:`211`, :issue:`216`)
- ``sqlite-utils insert`` now shows a more useful error message for invalid JSON. (:issue:`206`)

.. _v3_1_1:

3.1.1 (2021-01-01)
------------------

- Fixed failing test caused by ``optimize`` sometimes creating larger database files. (:issue:`209`)
- Documentation now lives on https://sqlite-utils.datasette.io/
- README now includes ``brew install sqlite-utils`` installation method.

.. _v3_1:

3.1 (2020-12-12)
----------------

- New command: ``sqlite-utils analyze-tables my.db`` outputs useful information about the table columns in the database, such as the number of distinct values and how many rows are null. See :ref:`cli_analyze_tables` for documentation. (:issue:`207`)
- New ``table.analyze_column(column)`` Python method used by the ``analyze-tables`` command - see :ref:`python_api_analyze_column`.
- The ``table.update()`` method now correctly handles values that should be stored as JSON. Thanks, Andreas Madsack. (`#204 <https://github.com/simonw/sqlite-utils/pull/204>`__)

.. _v3_0:

3.0 (2020-11-08)
----------------

This release introduces a new ``sqlite-utils search`` command for searching tables, see :ref:`cli_search`. (:issue:`192`)

The ``table.search()`` method has been redesigned, see :ref:`python_api_fts_search`. (:issue:`197`)

The release includes minor backwards-incompatible changes, hence the version bump to 3.0. Those changes, which should not affect most users, are:

- The ``-c`` shortcut option for outputting CSV is no longer available. The full ``--csv`` option is required instead.
- The ``-f`` shortcut for ``--fmt`` has also been removed - use ``--fmt``.
- The ``table.search()`` method now defaults to sorting by relevance, not sorting by ``rowid``. (:issue:`198`)
- The ``table.search()`` method now returns a generator over a list of Python dictionaries. It previously returned a list of tuples.

Also in this release:

- The ``query``, ``tables``, ``rows`` and ``search`` CLI commands now accept a new ``--tsv`` option which outputs the results in TSV. (:issue:`193`)
- A new ``table.virtual_table_using`` property reveals if a table is a virtual table, and returns the upper case type of virtual table (e.g. ``FTS4`` or ``FTS5``) if it is. It returns ``None`` if the table is not a virtual table. (:issue:`196`)
- The new ``table.search_sql()`` method returns the SQL for searching a table, see :ref:`python_api_fts_search_sql`.
- ``sqlite-utils rows`` now accepts multiple optional ``-c`` parameters specifying the columns to return. (:issue:`200`)

Changes since the 3.0a0 alpha release:

- The ``sqlite-utils search`` command now defaults to returning every result, unless you add a ``--limit 20`` option.
- The ``sqlite-utils search -c`` and ``table.search(columns=[])`` options are now fully respected. (:issue:`201`)

.. _v2_23:

2.23 (2020-10-28)
-----------------

- ``table.m2m(other_table, records)`` method now takes any iterable, not just a list or tuple. Thanks, Adam Wolf. (`#189 <https://github.com/simonw/sqlite-utils/pull/189>`__)
- ``sqlite-utils insert`` now displays a progress bar for CSV or TSV imports. (:issue:`173`)
- New ``@db.register_function(deterministic=True)`` option for registering deterministic SQLite functions in Python 3.8 or higher. (:issue:`191`)

.. _v2_22:

2.22 (2020-10-16)
-----------------

- New ``--encoding`` option for processing CSV and TSV files that use a non-utf-8 encoding, for both the ``insert`` and ``update`` commands. (:issue:`182`)
- The ``--load-extension`` option is now available to many more commands. (:issue:`137`)
- ``--load-extension=spatialite`` can be used to load SpatiaLite from common installation locations, if it is available. (:issue:`136`)
- Tests now also run against Python 3.9. (:issue:`184`)
- Passing ``pk=["id"]`` now has the same effect as passing ``pk="id"``. (:issue:`181`)

.. _v2_21:

2.21 (2020-09-24)
-----------------

- ``table.extract()`` and ``sqlite-utils extract`` now apply much, much faster - one example operation reduced from twelve minutes to just four seconds! (:issue:`172`)
- ``sqlite-utils extract`` no longer shows a progress bar, because it's fast enough not to need one.
- New ``column_order=`` option for ``table.transform()`` which can be used to alter the order of columns in a table. (:issue:`175`)
- ``sqlite-utils transform --column-order=`` option (with a ``-o`` shortcut) for changing column order. (:issue:`176`)
- The ``table.transform(drop_foreign_keys=)`` parameter and the ``sqlite-utils transform --drop-foreign-key`` option have changed. They now accept just the name of the column rather than requiring all three of the column, other table and other column. This is technically a backwards-incompatible change but I chose not to bump the major version number because the transform feature is so new. (:issue:`177`)
- The table ``.disable_fts()``, ``.rebuild_fts()``, ``.delete()``, ``.delete_where()`` and ``.add_missing_columns()`` methods all now ``return self``, which means they can be chained together with other table operations.

.. _v2_20:

2.20 (2020-09-22)
-----------------

This release introduces two key new capabilities: **transform** (:issue:`114`) and **extract** (:issue:`42`).

Transform
~~~~~~~~~

SQLite's ALTER TABLE has `several documented limitations <https://sqlite.org/lang_altertable.html>`__. The ``table.transform()`` Python method and ``sqlite-utils transform`` CLI command work around these limitations using a pattern where a new table with the desired structure is created, data is copied over to it and the old table is then dropped and replaced by the new one.

You can use these tools to change column types, rename columns, drop columns, add and remove ``NOT NULL`` and defaults, remove foreign key constraints and more. See the :ref:`transforming tables (CLI) <cli_transform_table>` and :ref:`transforming tables (Python library) <python_api_transform>` documentation for full details of how to use them.

Extract
~~~~~~~

Sometimes a database table - especially one imported from a CSV file - will contain duplicate data. A ``Trees`` table may include a ``Species`` column with only a few dozen unique values, when the table itself contains thousands of rows.

The ``table.extract()`` method and ``sqlite-utils extract`` commands can extract a column - or multiple columns - out into a separate lookup table, and set up a foreign key relationship from the original table.

The Python library :ref:`extract() documentation <python_api_extract>` describes how extraction works in detail, and :ref:`cli_extract` in the CLI documentation includes a detailed example.

Other changes
~~~~~~~~~~~~~

- The ``@db.register_function`` decorator can be used to quickly register Python functions as custom SQL functions, see :ref:`python_api_register_function`. (:issue:`162`)
- The ``table.rows_where()`` method now accepts an optional ``select=`` argument for specifying which columns should be selected, see :ref:`python_api_rows`.

.. _v2_19:

2.19 (2020-09-20)
-----------------

- New ``sqlite-utils add-foreign-keys`` command for :ref:`cli_add_foreign_keys`. (:issue:`157`)
- New ``table.enable_fts(..., replace=True)`` argument for replacing an existing FTS table with a new configuration. (:issue:`160`)
- New ``table.add_foreign_key(..., ignore=True)`` argument for ignoring a foreign key if it already exists. (:issue:`112`)

.. _v2_18:

2.18 (2020-09-08)
-----------------

- ``table.rebuild_fts()`` method for rebuilding a FTS index, see :ref:`python_api_fts_rebuild`. (:issue:`155`)
- ``sqlite-utils rebuild-fts data.db`` command for rebuilding FTS indexes across all tables, or just specific tables. (:issue:`155`)
- ``table.optimize()`` method no longer deletes junk rows from the ``*_fts_docsize`` table. This was added in 2.17 but it turns out running ``table.rebuild_fts()`` is a better solution to this problem.
- Fixed a bug where rows with additional columns that are inserted after the first batch of records could cause an error due to breaking SQLite's maximum number of parameters. Thanks, Simon Wiles. (:issue:`145`)

.. _v2_17:

2.17 (2020-09-07)
-----------------

This release handles a bug where replacing rows in FTS tables could result in growing numbers of unnecessary rows in the associated ``*_fts_docsize`` table. (:issue:`149`)

- ``PRAGMA recursive_triggers=on`` by default for all connections. You can turn it off with ``Database(recursive_triggers=False)``. (:issue:`152`)
- ``table.optimize()`` method now deletes unnecessary rows from the ``*_fts_docsize`` table. (:issue:`153`)
- New tracer method for tracking underlying SQL queries, see :ref:`python_api_tracing`. (:issue:`150`)
- Neater indentation for schema SQL. (:issue:`148`)
- Documentation for ``sqlite_utils.AlterError`` exception thrown by in ``add_foreign_keys()``.

.. _v2_16_1:

2.16.1 (2020-08-28)
-------------------

- ``insert_all(..., alter=True)`` now works for columns introduced after the first 100 records. Thanks, Simon Wiles! (:issue:`139`)
- Continuous Integration is now powered by GitHub Actions. (:issue:`143`)

.. _v2_16:

2.16 (2020-08-21)
-----------------

- ``--load-extension`` option for ``sqlite-utils query`` for loading SQLite extensions. (:issue:`134`)
- New ``sqlite_utils.utils.find_spatialite()`` function for finding SpatiaLite in common locations. (:issue:`135`)

.. _v2_15_1:

2.15.1 (2020-08-12)
-------------------

- Now available as a ``sdist`` package on PyPI in addition to a wheel. (:issue:`133`)

.. _v2_15:

2.15 (2020-08-10)
-----------------

- New ``db.enable_wal()`` and ``db.disable_wal()`` methods for enabling and disabling `Write-Ahead Logging <https://www.sqlite.org/wal.html>`__ for a database file - see :ref:`python_api_wal` in the Python API documentation.
- Also ``sqlite-utils enable-wal file.db`` and ``sqlite-utils disable-wal file.db`` commands for doing the same thing on the command-line, see :ref:`WAL mode (CLI) <cli_wal>`. (:issue:`132`)

.. _v2_14_1:

2.14.1 (2020-08-05)
-------------------

- Documentation improvements.

.. _v2_14:

2.14 (2020-08-01)
-----------------

- The :ref:`insert-files command <cli_insert_files>` can now read from standard input: ``cat dog.jpg | sqlite-utils insert-files dogs.db pics - --name=dog.jpg``. (:issue:`127`)
- You can now specify a full-text search tokenizer using the new ``tokenize=`` parameter to :ref:`enable_fts() <python_api_fts>`. This means you can enable Porter stemming on a table by running ``db["articles"].enable_fts(["headline", "body"], tokenize="porter")``. (:issue:`130`)
- You can also set a custom tokenizer using the :ref:`sqlite-utils enable-fts <cli_fts>` CLI command, via the new ``--tokenize`` option.

.. _v2_13:

2.13 (2020-07-29)
-----------------

- ``memoryview`` and ``uuid.UUID`` objects are now supported. ``memoryview`` objects will be stored using ``BLOB`` and ``uuid.UUID`` objects will be stored using ``TEXT``. (:issue:`128`)

.. _v2_12:

2.12 (2020-07-27)
-----------------

The theme of this release is better tools for working with binary data. The new ``insert-files`` command can be used to insert binary files directly into a database table, and other commands have been improved with better support for BLOB columns.

- ``sqlite-utils insert-files my.db gifs *.gif`` can now insert the contents of files into a specified table. The columns in the table can be customized to include different pieces of metadata derived from the files. See :ref:`cli_insert_files`. (:issue:`122`)
- ``--raw`` option to ``sqlite-utils query`` - for outputting just a single raw column value - see :ref:`cli_query_raw`. (:issue:`123`)
- JSON output now encodes BLOB values as special base64 objects - see :ref:`cli_query_json`. (:issue:`125`)
- The same format of JSON base64 objects can now be used to insert binary data - see :ref:`cli_inserting_data`. (:issue:`126`)
- The ``sqlite-utils query`` command can now accept named parameters, e.g. ``sqlite-utils :memory: "select :num * :num2" -p num 5 -p num2 6`` - see :ref:`cli_query_json`. (:issue:`124`)

.. _v2_11:

2.11 (2020-07-08)
-----------------

- New ``--truncate`` option to ``sqlite-utils insert``, and ``truncate=True`` argument to ``.insert_all()``. Thanks, Thomas Sibley. (`#118 <https://github.com/simonw/sqlite-utils/pull/118>`__)
- The ``sqlite-utils query`` command now runs updates in a transaction. Thanks, Thomas Sibley. (`#120 <https://github.com/simonw/sqlite-utils/pull/120>`__)

.. _v2_10_1:

2.10.1 (2020-06-23)
-------------------

- Added documentation for the ``table.pks`` introspection property. (:issue:`116`)

.. _v2_10:

2.10 (2020-06-12)
-----------------

- The ``sqlite-utils`` command now supports UPDATE/INSERT/DELETE in addition to SELECT. (:issue:`115`)

.. _v2_9_1:

2.9.1 (2020-05-11)
------------------

- Added custom project links to the `PyPI listing <https://pypi.org/project/sqlite-utils/>`__.

.. _v2_9:

2.9 (2020-05-10)
----------------

- New ``sqlite-utils drop-table`` command, see :ref:`cli_drop_table`. (:issue:`111`)
- New ``sqlite-utils drop-view`` command, see :ref:`cli_drop_view`.
- Python ``decimal.Decimal`` objects are now stored as ``FLOAT``. (:issue:`110`)

.. _v2_8:

2.8 (2020-05-03)
----------------

- New ``sqlite-utils create-table`` command, see :ref:`cli_create_table`. (:issue:`27`)
- New ``sqlite-utils create-view`` command, see :ref:`cli_create_view`. (:issue:`107`)

.. _v2_7.2:

2.7.2 (2020-05-02)
------------------

- ``db.create_view(...)`` now has additional parameters ``ignore=True`` or ``replace=True``, see :ref:`python_api_create_view`. (:issue:`106`)

.. _v2_7.1:

2.7.1 (2020-05-01)
------------------

- New ``sqlite-utils views my.db`` command for listing views in a database, see :ref:`cli_views`. (:issue:`105`)
- ``sqlite-utils tables`` (and ``views``) has a new ``--schema`` option which outputs the table/view schema, see :ref:`cli_tables`. (:issue:`104`)
- Nested structures containing invalid JSON values (e.g. Python bytestrings) are now serialized using ``repr()`` instead of throwing an error. (:issue:`102`)

.. _v2_7:

2.7 (2020-04-17)
----------------

- New ``columns=`` argument for the ``.insert()``, ``.insert_all()``, ``.upsert()`` and ``.upsert_all()`` methods, for over-riding the auto-detected types for columns and specifying additional columns that should be added when the table is created. See :ref:`python_api_custom_columns`. (:issue:`100`)

.. _v2_6:

2.6 (2020-04-15)
----------------

- New ``table.rows_where(..., order_by="age desc")`` argument, see :ref:`python_api_rows`. (:issue:`76`)

.. _v2_5:

2.5 (2020-04-12)
----------------

- Panda's Timestamp is now stored as a SQLite TEXT column. Thanks, b0b5h4rp13! (:issue:`96`)
- ``table.last_pk`` is now only available for inserts or upserts of a single record. (:issue:`98`)
- New ``Database(filepath, recreate=True)`` parameter for deleting and recreating the database. (:issue:`97`)

.. _v2_4_4:

2.4.4 (2020-03-23)
------------------

- Fixed bug where columns with only null values were not correctly created. (:issue:`95`)

.. _v2_4_3:

2.4.3 (2020-03-23)
------------------

- Column type suggestion code is no longer confused by null values. (:issue:`94`)

.. _v2_4_2:

2.4.2 (2020-03-14)
------------------

- ``table.column_dicts`` now works with all column types - previously it would throw errors on types other than ``TEXT``, ``BLOB``, ``INTEGER`` or ``FLOAT``. (:issue:`92`)
- Documentation for ``NotFoundError`` thrown by ``table.get(pk)`` - see :ref:`python_api_get`.

.. _v2_4_1:

2.4.1 (2020-03-01)
------------------

- ``table.enable_fts()`` now works with columns that contain spaces. (:issue:`90`)

.. _v2_4:

2.4 (2020-02-26)
----------------

- ``table.disable_fts()`` can now be used to remove FTS tables and triggers that were created using ``table.enable_fts(...)``. (:issue:`88`)
- The ``sqlite-utils disable-fts`` command can be used to remove FTS tables and triggers from the command-line. (:issue:`88`)
- Trying to create table columns with square braces ([ or ]) in the name now raises an error. (:issue:`86`)
- Subclasses of ``dict``, ``list`` and ``tuple`` are now detected as needing a JSON column. (:issue:`87`)

.. _v2_3_1:

2.3.1 (2020-02-10)
------------------

``table.create_index()`` now works for columns that contain spaces. (:issue:`85`)

.. _v2_3:

2.3 (2020-02-08)
----------------

``table.exists()`` is now a method, not a property. This was not a documented part of the API before so I'm considering this a non-breaking change. (:issue:`83`)

.. _v2_2_1:

2.2.1 (2020-02-06)
------------------

Fixed a bug where ``.upsert(..., hash_id="pk")`` threw an error (:issue:`84`).

.. _v2_2:

2.2 (2020-02-01)
----------------

New feature: ``sqlite_utils.suggest_column_types([records])`` returns the suggested column types for a list of records. See :ref:`python_api_suggest_column_types`. (:issue:`81`).

This replaces the undocumented ``table.detect_column_types()`` method.

.. _v2_1:

2.1 (2020-01-30)
----------------

New feature: ``conversions={...}`` can be passed to the ``.insert()`` family of functions to specify SQL conversions that should be applied to values that are being inserted or updated. See :ref:`python_api_conversions` . (`#77 <https://github.com/simonw/sqlite-utils/issues/73>`__).

.. _v2_0_1:

2.0.1 (2020-01-05)
------------------

The ``.upsert()`` and ``.upsert_all()`` methods now raise a ``sqlite_utils.db.PrimaryKeyRequired`` exception if you call them without specifying the primary key column using ``pk=`` (:issue:`73`).

.. _v2:

2.0 (2019-12-29)
----------------

This release changes the behaviour of ``upsert``. It's a breaking change, hence ``2.0``.

The ``upsert`` command-line utility and the ``.upsert()`` and ``.upsert_all()`` Python API methods have had their behaviour altered. They used to completely replace the affected records: now, they update the specified values on existing records but leave other columns unaffected.

See :ref:`Upserting data using the Python API <python_api_upsert>` and :ref:`Upserting data using the CLI <cli_upsert>` for full details.

If you want the old behaviour - where records were completely replaced - you can use ``$ sqlite-utils insert ... --replace`` on the command-line and ``.insert(..., replace=True)`` and ``.insert_all(..., replace=True)`` in the Python API. See :ref:`Insert-replacing data using the Python API <python_api_insert_replace>` and :ref:`Insert-replacing data using the CLI <cli_insert_replace>` for more.

For full background on this change, see `issue #66 <https://github.com/simonw/sqlite-utils/issues/66>`__.

.. _v1_12_1:

1.12.1 (2019-11-06)
-------------------

- Fixed error thrown when ``.insert_all()`` and ``.upsert_all()`` were called with empty lists (:issue:`52`)

.. _v1_12:

1.12 (2019-11-04)
-----------------

Python library utilities for deleting records (:issue:`62`)

- ``db["tablename"].delete(4)`` to delete by primary key, see :ref:`python_api_delete`
- ``db["tablename"].delete_where("id > ?", [3])`` to delete by a where clause, see :ref:`python_api_delete_where`

.. _v1_11:

1.11 (2019-09-02)
-----------------

Option to create triggers to automatically keep FTS tables up-to-date with newly inserted, updated and deleted records. Thanks, Amjith Ramanujam! (`#57 <https://github.com/simonw/sqlite-utils/pull/57>`__)

- ``sqlite-utils enable-fts ... --create-triggers`` - see :ref:`Configuring full-text search using the CLI <cli_fts>`
- ``db["tablename"].enable_fts(..., create_triggers=True)`` - see :ref:`Configuring full-text search using the Python library <python_api_fts>`
- Support for introspecting triggers for a database or table - see :ref:`python_api_introspection` (:issue:`59`)

.. _v1_10:

1.10 (2019-08-23)
-----------------

Ability to introspect and run queries against views (:issue:`54`)

- ``db.view_names()`` method and and ``db.views`` property
- Separate ``View`` and ``Table`` classes, both subclassing new ``Queryable`` class
- ``view.drop()`` method

See :ref:`python_api_views`.

.. _v1_9:

1.9 (2019-08-04)
----------------

- ``table.m2m(...)`` method for creating many-to-many relationships: :ref:`python_api_m2m` (:issue:`23`)

.. _v1_8:

1.8 (2019-07-28)
----------------

- ``table.update(pk, values)`` method: :ref:`python_api_update` (:issue:`35`)

.. _v1_7_1:

1.7.1 (2019-07-28)
------------------

- Fixed bug where inserting records with 11 columns in a batch of 100 triggered a "too many SQL variables" error (:issue:`50`)
- Documentation and tests for ``table.drop()`` method: :ref:`python_api_drop`

.. _v1_7:

1.7 (2019-07-24)
----------------

Support for lookup tables.

- New ``table.lookup({...})`` utility method for building and querying lookup tables - see :ref:`python_api_lookup_tables` (:issue:`44`)
- New ``extracts=`` table configuration option, see :ref:`python_api_extracts` (:issue:`46`)
- Use `pysqlite3 <https://github.com/coleifer/pysqlite3>`__ if it is available, otherwise use ``sqlite3`` from the standard library
- Table options can now be passed to the new ``db.table(name, **options)`` factory function in addition to being passed to ``insert_all(records, **options)`` and friends - see :ref:`python_api_table_configuration`
- In-memory databases can now be created using ``db = Database(memory=True)``

.. _v1_6:

1.6 (2019-07-18)
----------------

- ``sqlite-utils insert`` can now accept TSV data via the new ``--tsv`` option (:issue:`41`)

.. _v1_5:

1.5 (2019-07-14)
----------------

- Support for compound primary keys (:issue:`36`)

  - Configure these using the CLI tool by passing ``--pk`` multiple times
  - In Python, pass a tuple of columns to the ``pk=(..., ...)`` argument: :ref:`python_api_compound_primary_keys`

- New ``table.get()`` method for retrieving a record by its primary key: :ref:`python_api_get` (:issue:`39`)

.. _v1_4_1:

1.4.1 (2019-07-14)
------------------

- Assorted minor documentation fixes: `changes since 1.4 <https://github.com/simonw/sqlite-utils/compare/1.4...1.4.1>`__

.. _v1_4:

1.4 (2019-06-30)
----------------

- Added ``sqlite-utils index-foreign-keys`` command (:ref:`docs <cli_index_foreign_keys>`) and ``db.index_foreign_keys()`` method (:ref:`docs <python_api_index_foreign_keys>`) (:issue:`33`)

.. _v1_3:

1.3 (2019-06-28)
----------------

- New mechanism for adding multiple foreign key constraints at once: :ref:`db.add_foreign_keys() documentation <python_api_add_foreign_keys>` (:issue:`31`)

.. _v1_2_2:

1.2.2 (2019-06-25)
------------------

- Fixed bug where ``datetime.time`` was not being handled correctly

.. _v1_2_1:

1.2.1 (2019-06-20)
------------------

- Check the column exists before attempting to add a foreign key (:issue:`29`)

.. _v1_2:

1.2 (2019-06-12)
----------------

- Improved foreign key definitions: you no longer need to specify the ``column``, ``other_table`` AND ``other_column`` to define a foreign key - if you omit the ``other_table`` or ``other_column`` the script will attempt to guess the correct values by introspecting the database. See :ref:`python_api_add_foreign_key` for details. (:issue:`25`)
- Ability to set ``NOT NULL`` constraints and ``DEFAULT`` values when creating tables (:issue:`24`). Documentation: :ref:`Setting defaults and not null constraints (Python API) <python_api_defaults_not_null>`, :ref:`Setting defaults and not null constraints (CLI) <cli_defaults_not_null>`
- Support for ``not_null_default=X`` / ``--not-null-default`` for setting a ``NOT NULL DEFAULT 'x'`` when adding a new column. Documentation: :ref:`Adding columns (Python API) <python_api_add_column>`, :ref:`Adding columns (CLI) <cli_add_column>`

.. _v1_1:

1.1 (2019-05-28)
----------------

- Support for ``ignore=True`` / ``--ignore`` for ignoring inserted records if the primary key already exists (:issue:`21`) - documentation: :ref:`Inserting data (Python API) <python_api_bulk_inserts>`, :ref:`Inserting data (CLI) <cli_inserting_data>`
- Ability to add a column that is a foreign key reference using ``fk=...`` / ``--fk`` (:issue:`16`) - documentation: :ref:`Adding columns (Python API) <python_api_add_column>`, :ref:`Adding columns (CLI) <cli_add_column>`

.. _v1_0_1:

1.0.1 (2019-05-27)
------------------

- ``sqlite-utils rows data.db table --json-cols`` - fixed bug where ``--json-cols`` was not obeyed

.. _v1_0:

1.0 (2019-05-24)
----------------

- Option to automatically add new columns if you attempt to insert or upsert data with extra fields:
   ``sqlite-utils insert ... --alter`` - see :ref:`Adding columns automatically with the sqlite-utils CLI <cli_add_column_alter>`

   ``db["tablename"].insert(record, alter=True)`` - see :ref:`Adding columns automatically using the Python API <python_api_add_column_alter>`
- New ``--json-cols`` option for outputting nested JSON, see :ref:`cli_json_values`

.. _v0_14:

0.14 (2019-02-24)
-----------------

- Ability to create unique indexes: ``db["mytable"].create_index(["name"], unique=True)``
- ``db["mytable"].create_index(["name"], if_not_exists=True)``
- ``$ sqlite-utils create-index mydb.db mytable col1 [col2...]``, see :ref:`cli_create_index`
- ``table.add_column(name, type)`` method, see :ref:`python_api_add_column`
- ``$ sqlite-utils add-column mydb.db mytable nameofcolumn``, see :ref:`cli_add_column` (CLI)
- ``db["books"].add_foreign_key("author_id", "authors", "id")``, see :ref:`python_api_add_foreign_key`
- ``$ sqlite-utils add-foreign-key books.db books author_id authors id``, see :ref:`cli_add_foreign_key` (CLI)
- Improved (but backwards-incompatible) ``foreign_keys=`` argument to various methods, see :ref:`python_api_foreign_keys`

.. _v0_13:

0.13 (2019-02-23)
-----------------

- New ``--table`` and ``--fmt`` options can be used to output query results in a variety of visual table formats, see :ref:`cli_query_table`
- New ``hash_id=`` argument can now be used for :ref:`python_api_hash`
- Can now derive correct column types for numpy int, uint and float values
- ``table.last_id`` has been renamed to ``table.last_rowid``
- ``table.last_pk`` now contains the last inserted primary key, if ``pk=`` was specified
- Prettier indentation in the ``CREATE TABLE`` generated schemas

.. _v0_12:

0.12 (2019-02-22)
-----------------

- Added ``db[table].rows`` iterator - see :ref:`python_api_rows`
- Replaced ``sqlite-utils json`` and ``sqlite-utils csv`` with a new default subcommand called ``sqlite-utils query`` which defaults to JSON and takes formatting options ``--nl``, ``--csv`` and ``--no-headers`` - see :ref:`cli_query_json` and :ref:`cli_query_csv`
- New ``sqlite-utils rows data.db name-of-table`` command, see :ref:`cli_rows`
- ``sqlite-utils table`` command now takes options ``--counts`` and ``--columns`` plus the standard output format options, see :ref:`cli_tables`

.. _v0_11:

0.11 (2019-02-07)
-----------------

New commands for enabling FTS against a table and columns::

    sqlite-utils enable-fts db.db mytable col1 col2

See :ref:`cli_fts`.

.. _v0_10:

0.10 (2019-02-06)
-----------------

Handle ``datetime.date`` and ``datetime.time`` values.

New option for efficiently inserting rows from a CSV:
::

    sqlite-utils insert db.db foo - --csv

.. _v0_9:

0.9 (2019-01-27)
----------------

Improved support for newline-delimited JSON.

``sqlite-utils insert`` has two new command-line options:

* ``--nl`` means "expect newline-delimited JSON". This is an extremely efficient way of loading in large amounts of data, especially if you pipe it into standard input.
* ``--batch-size=1000`` lets you increase the batch size (default is 100). A commit will be issued every X records. This also control how many initial records are considered when detecting the desired SQL table schema for the data.

In the Python API, the ``table.insert_all(...)`` method can now accept a generator as well as a list of objects. This will be efficiently used to populate the table no matter how many records are produced by the generator.

The ``Database()`` constructor can now accept a ``pathlib.Path`` object in addition to a string or an existing SQLite connection object.

.. _v0_8:

0.8 (2019-01-25)
----------------

Two new commands: ``sqlite-utils csv`` and ``sqlite-utils json``

These commands execute a SQL query and return the results as CSV or JSON. See :ref:`cli_query_csv` and :ref:`cli_query_json` for more details.

::

    $ sqlite-utils json --help
    Usage: sqlite-utils json [OPTIONS] PATH SQL

      Execute SQL query and return the results as JSON

    Options:
      --nl      Output newline-delimited JSON
      --arrays  Output rows as arrays instead of objects
      --help    Show this message and exit.

    $ sqlite-utils csv --help
    Usage: sqlite-utils csv [OPTIONS] PATH SQL

      Execute SQL query and return the results as CSV

    Options:
      --no-headers  Exclude headers from CSV output
      --help        Show this message and exit.

.. _v0_7:

0.7 (2019-01-24)
----------------

This release implements the ``sqlite-utils`` command-line tool with a number of useful subcommands.

- ``sqlite-utils tables demo.db`` lists the tables in the database
- ``sqlite-utils tables demo.db --fts4`` shows just the FTS4 tables
- ``sqlite-utils tables demo.db --fts5`` shows just the FTS5 tables
- ``sqlite-utils vacuum demo.db`` runs VACUUM against the database
- ``sqlite-utils optimize demo.db`` runs OPTIMIZE against all FTS tables, then VACUUM
- ``sqlite-utils optimize demo.db --no-vacuum`` runs OPTIMIZE but skips VACUUM

The two most useful subcommands are ``upsert`` and ``insert``, which allow you to ingest JSON files with one or more records in them, creating the corresponding table with the correct columns if it does not already exist. See :ref:`cli_inserting_data` for more details.

- ``sqlite-utils insert demo.db dogs dogs.json --pk=id`` inserts new records from ``dogs.json`` into the ``dogs`` table
- ``sqlite-utils upsert demo.db dogs dogs.json --pk=id`` upserts records, replacing any records with duplicate primary keys


One backwards incompatible change: the ``db["table"].table_names`` property is now a method:

- ``db["table"].table_names()`` returns a list of table names
- ``db["table"].table_names(fts4=True)`` returns a list of just the FTS4 tables
- ``db["table"].table_names(fts5=True)`` returns a list of just the FTS5 tables

A few other changes:

- Plenty of updated documentation, including full coverage of the new command-line tool
- Allow column names to be reserved words (use correct SQL escaping)
- Added automatic column support for bytes and datetime.datetime

.. _v0_6:

0.6 (2018-08-12)
----------------

- ``.enable_fts()`` now takes optional argument ``fts_version``, defaults to ``FTS5``. Use ``FTS4`` if the version of SQLite bundled with your Python does not support FTS5
- New optional ``column_order=`` argument to ``.insert()`` and friends for providing a partial or full desired order of the columns when a database table is created
- :ref:`New documentation <python_api>` for ``.insert_all()`` and ``.upsert()`` and ``.upsert_all()``

.. _v0_5:

0.5 (2018-08-05)
----------------

- ``db.tables`` and ``db.table_names`` introspection properties
- ``db.indexes`` property for introspecting indexes
- ``table.create_index(columns, index_name)`` method
- ``db.create_view(name, sql)`` method
- Table methods can now be chained, plus added ``table.last_id`` for accessing the last inserted row ID

0.4 (2018-07-31)
----------------

- ``enable_fts()``, ``populate_fts()`` and ``search()`` table methods
