.. _cli_reference:

===============
 CLI reference
===============

This page lists the ``--help`` for every ``sqlite-utils`` CLI sub-command.

.. contents:: :local:

.. [[[cog
    from sqlite_utils import cli
    from click.testing import CliRunner
    import textwrap
    commands = list(cli.cli.commands.keys())
    go_first = [
        "query", "memory", "insert", "upsert", "bulk", "search", "transform", "extract",
        "schema", "insert-files", "analyze-tables", "convert", "tables", "views", "rows",
        "triggers", "indexes", "create-database", "create-table", "create-index",
        "enable-fts", "populate-fts", "rebuild-fts", "disable-fts"
    ]
    refs = {
        "query": "cli_query",
        "memory": "cli_memory",
        "insert": ["cli_inserting_data", "cli_insert_csv_tsv"],
        "upsert": "cli_upsert",
        "tables": "cli_tables",
        "views": "cli_views",
        "optimize": "cli_optimize",
        "rows": "cli_rows",
        "triggers": "cli_triggers",
        "indexes": "cli_indexes",
        "enable-fts": "cli_fts",
        "analyze": "cli_analyze",
        "vacuum": "cli_vacuum",
        "dump": "cli_dump",
        "add-column": "cli_add_column",
        "add-foreign-key": "cli_add_foreign_key",
        "add-foreign-keys": "cli_add_foreign_keys",
        "index-foreign-keys": "cli_index_foreign_keys",
        "create-index": "cli_create_index",
        "enable-wal": "cli_wal",
        "enable-counts": "cli_enable_counts",
        "bulk": "cli_bulk",
        "create-database": "cli_create_database",
        "create-table": "cli_create_table",
        "drop-table": "cli_drop_table",
        "create-view": "cli_create_view",
        "drop-view": "cli_drop_view",
        "search": "cli_search",
        "transform": "cli_transform_table",
        "extract": "cli_extract",
        "schema": "cli_schema",
        "insert-files": "cli_insert_files",
        "analyze-tables": "cli_analyze_tables",
        "convert": "cli_convert",
    }
    commands.sort(key = lambda command: go_first.index(command) if command in go_first else 999)
    cog.out("\n")
    for command in commands:
        cog.out(command + "\n")
        cog.out(("=" * len(command)) + "\n\n")
        if command in refs:
            command_refs = refs[command]
            if isinstance(command_refs, str):
                command_refs = [command_refs]
            cog.out(
                "See {}.\n\n".format(
                    ", ".join(":ref:`{}`".format(c) for c in command_refs)
                )
            )
        cog.out("::\n\n")
        result = CliRunner().invoke(cli.cli, [command, "--help"])
        output = result.output.replace("Usage: cli ", "Usage: sqlite-utils ")
        cog.out(textwrap.indent(output, '    '))
        cog.out("\n\n")
.. ]]]

query
=====

See :ref:`cli_query`.

::

    Usage: sqlite-utils query [OPTIONS] PATH SQL

      Execute SQL query and return the results as JSON

      Example:

          sqlite-utils data.db \
              "select * from chickens where age > :age" \
              -p age 1

    Options:
      --attach <TEXT FILE>...     Additional databases to attach - specify alias and
                                  filepath
      --nl                        Output newline-delimited JSON
      --arrays                    Output rows as arrays instead of objects
      --csv                       Output CSV
      --tsv                       Output TSV
      --no-headers                Omit CSV headers
      -t, --table                 Output as a formatted table
      --fmt TEXT                  Table format - one of fancy_grid, fancy_outline,
                                  github, grid, html, jira, latex, latex_booktabs,
                                  latex_longtable, latex_raw, mediawiki, moinmoin,
                                  orgtbl, pipe, plain, presto, pretty, psql, rst,
                                  simple, textile, tsv, unsafehtml, youtrack
      --json-cols                 Detect JSON cols and output them as JSON, not
                                  escaped strings
      -r, --raw                   Raw output, first column of first row
      -p, --param <TEXT TEXT>...  Named :parameters for SQL query
      --load-extension TEXT       SQLite extensions to load
      -h, --help                  Show this message and exit.


memory
======

See :ref:`cli_memory`.

::

    Usage: sqlite-utils memory [OPTIONS] [PATHS]... SQL

      Execute SQL query against an in-memory database, optionally populated by
      imported data

      To import data from CSV, TSV or JSON files pass them on the command-line:

          sqlite-utils memory one.csv two.json \
              "select * from one join two on one.two_id = two.id"

      For data piped into the tool from standard input, use "-" or "stdin":

          cat animals.csv | sqlite-utils memory - \
              "select * from stdin where species = 'dog'"

      The format of the data will be automatically detected. You can specify the
      format explicitly using :json, :csv, :tsv or :nl (for newline-delimited JSON)
      - for example:

          cat animals.csv | sqlite-utils memory stdin:csv places.dat:nl \
              "select * from stdin where place_id in (select id from places)"

      Use --schema to view the SQL schema of any imported files:

          sqlite-utils memory animals.csv --schema

    Options:
      --attach <TEXT FILE>...     Additional databases to attach - specify alias and
                                  filepath
      --flatten                   Flatten nested JSON objects, so {"foo": {"bar":
                                  1}} becomes {"foo_bar": 1}
      --nl                        Output newline-delimited JSON
      --arrays                    Output rows as arrays instead of objects
      --csv                       Output CSV
      --tsv                       Output TSV
      --no-headers                Omit CSV headers
      -t, --table                 Output as a formatted table
      --fmt TEXT                  Table format - one of fancy_grid, fancy_outline,
                                  github, grid, html, jira, latex, latex_booktabs,
                                  latex_longtable, latex_raw, mediawiki, moinmoin,
                                  orgtbl, pipe, plain, presto, pretty, psql, rst,
                                  simple, textile, tsv, unsafehtml, youtrack
      --json-cols                 Detect JSON cols and output them as JSON, not
                                  escaped strings
      -r, --raw                   Raw output, first column of first row
      -p, --param <TEXT TEXT>...  Named :parameters for SQL query
      --encoding TEXT             Character encoding for CSV input, defaults to
                                  utf-8
      -n, --no-detect-types       Treat all CSV/TSV columns as TEXT
      --schema                    Show SQL schema for in-memory database
      --dump                      Dump SQL for in-memory database
      --save FILE                 Save in-memory database to this file
      --analyze                   Analyze resulting tables and output results
      --load-extension TEXT       SQLite extensions to load
      -h, --help                  Show this message and exit.


insert
======

See :ref:`cli_inserting_data`, :ref:`cli_insert_csv_tsv`.

::

    Usage: sqlite-utils insert [OPTIONS] PATH TABLE FILE

      Insert records from FILE into a table, creating the table if it does not
      already exist.

      Example:

          echo '{"name": "Lila"}' | sqlite-utils insert data.db chickens -

      By default the input is expected to be a JSON object or array of objects.

      - Use --nl for newline-delimited JSON objects
      - Use --csv or --tsv for comma-separated or tab-separated input
      - Use --lines to write each incoming line to a column called "line"
      - Use --text to write the entire input to a column called "text"

      You can also use --convert to pass a fragment of Python code that will be used
      to convert each input.

      Your Python code will be passed a "row" variable representing the imported
      row, and can return a modified row.

      If you are using --lines your code will be passed a "line" variable, and for
      --text an "text" variable.

    Options:
      --pk TEXT                 Columns to use as the primary key, e.g. id
      --flatten                 Flatten nested JSON objects, so {"a": {"b": 1}}
                                becomes {"a_b": 1}
      --nl                      Expect newline-delimited JSON
      -c, --csv                 Expect CSV input
      --tsv                     Expect TSV input
      --lines                   Treat each line as a single value called 'line'
      --text                    Treat input as a single value called 'text'
      --convert TEXT            Python code to convert each item
      --import TEXT             Python modules to import
      --delimiter TEXT          Delimiter to use for CSV files
      --quotechar TEXT          Quote character to use for CSV/TSV
      --sniff                   Detect delimiter and quote character
      --no-headers              CSV file has no header row
      --encoding TEXT           Character encoding for input, defaults to utf-8
      --batch-size INTEGER      Commit every X records
      --alter                   Alter existing table to add any missing columns
      --not-null TEXT           Columns that should be created as NOT NULL
      --default <TEXT TEXT>...  Default value that should be set for a column
      -d, --detect-types        Detect types for columns in CSV/TSV data
      --analyze                 Run ANALYZE at the end of this operation
      --load-extension TEXT     SQLite extensions to load
      --silent                  Do not show progress bar
      --ignore                  Ignore records if pk already exists
      --replace                 Replace records if pk already exists
      --truncate                Truncate table before inserting records, if table
                                already exists
      -h, --help                Show this message and exit.


upsert
======

See :ref:`cli_upsert`.

::

    Usage: sqlite-utils upsert [OPTIONS] PATH TABLE FILE

      Upsert records based on their primary key. Works like 'insert' but if an
      incoming record has a primary key that matches an existing record the existing
      record will be updated.

      Example:

          echo '[
              {"id": 1, "name": "Lila"},
              {"id": 2, "name": "Suna"}
          ]' | sqlite-utils upsert data.db chickens - --pk id

    Options:
      --pk TEXT                 Columns to use as the primary key, e.g. id
                                [required]
      --flatten                 Flatten nested JSON objects, so {"a": {"b": 1}}
                                becomes {"a_b": 1}
      --nl                      Expect newline-delimited JSON
      -c, --csv                 Expect CSV input
      --tsv                     Expect TSV input
      --lines                   Treat each line as a single value called 'line'
      --text                    Treat input as a single value called 'text'
      --convert TEXT            Python code to convert each item
      --import TEXT             Python modules to import
      --delimiter TEXT          Delimiter to use for CSV files
      --quotechar TEXT          Quote character to use for CSV/TSV
      --sniff                   Detect delimiter and quote character
      --no-headers              CSV file has no header row
      --encoding TEXT           Character encoding for input, defaults to utf-8
      --batch-size INTEGER      Commit every X records
      --alter                   Alter existing table to add any missing columns
      --not-null TEXT           Columns that should be created as NOT NULL
      --default <TEXT TEXT>...  Default value that should be set for a column
      -d, --detect-types        Detect types for columns in CSV/TSV data
      --analyze                 Run ANALYZE at the end of this operation
      --load-extension TEXT     SQLite extensions to load
      --silent                  Do not show progress bar
      -h, --help                Show this message and exit.


bulk
====

See :ref:`cli_bulk`.

::

    Usage: sqlite-utils bulk [OPTIONS] PATH SQL FILE

      Execute parameterized SQL against the provided list of documents.

      Example:

          echo '[
              {"id": 1, "name": "Lila2"},
              {"id": 2, "name": "Suna2"}
          ]' | sqlite-utils bulk data.db '
              update chickens set name = :name where id = :id
          ' -

    Options:
      --batch-size INTEGER   Commit every X records
      --flatten              Flatten nested JSON objects, so {"a": {"b": 1}} becomes
                             {"a_b": 1}
      --nl                   Expect newline-delimited JSON
      -c, --csv              Expect CSV input
      --tsv                  Expect TSV input
      --lines                Treat each line as a single value called 'line'
      --text                 Treat input as a single value called 'text'
      --convert TEXT         Python code to convert each item
      --import TEXT          Python modules to import
      --delimiter TEXT       Delimiter to use for CSV files
      --quotechar TEXT       Quote character to use for CSV/TSV
      --sniff                Detect delimiter and quote character
      --no-headers           CSV file has no header row
      --encoding TEXT        Character encoding for input, defaults to utf-8
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


search
======

See :ref:`cli_search`.

::

    Usage: sqlite-utils search [OPTIONS] PATH DBTABLE Q

      Execute a full-text search against this table

      Example:

          sqlite-utils search data.db chickens lila

    Options:
      -o, --order TEXT       Order by ('column' or 'column desc')
      -c, --column TEXT      Columns to return
      --limit INTEGER        Number of rows to return - defaults to everything
      --sql                  Show SQL query that would be run
      --quote                Apply FTS quoting rules to search term
      --nl                   Output newline-delimited JSON
      --arrays               Output rows as arrays instead of objects
      --csv                  Output CSV
      --tsv                  Output TSV
      --no-headers           Omit CSV headers
      -t, --table            Output as a formatted table
      --fmt TEXT             Table format - one of fancy_grid, fancy_outline,
                             github, grid, html, jira, latex, latex_booktabs,
                             latex_longtable, latex_raw, mediawiki, moinmoin,
                             orgtbl, pipe, plain, presto, pretty, psql, rst, simple,
                             textile, tsv, unsafehtml, youtrack
      --json-cols            Detect JSON cols and output them as JSON, not escaped
                             strings
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


transform
=========

See :ref:`cli_transform_table`.

::

    Usage: sqlite-utils transform [OPTIONS] PATH TABLE

      Transform a table beyond the capabilities of ALTER TABLE

      Example:

          sqlite-utils transform mydb.db mytable \
              --drop column1 \
              --rename column2 column_renamed

    Options:
      --type <TEXT CHOICE>...   Change column type to INTEGER, TEXT, FLOAT or BLOB
      --drop TEXT               Drop this column
      --rename <TEXT TEXT>...   Rename this column to X
      -o, --column-order TEXT   Reorder columns
      --not-null TEXT           Set this column to NOT NULL
      --not-null-false TEXT     Remove NOT NULL from this column
      --pk TEXT                 Make this column the primary key
      --pk-none                 Remove primary key (convert to rowid table)
      --default <TEXT TEXT>...  Set default value for this column
      --default-none TEXT       Remove default from this column
      --drop-foreign-key TEXT   Drop foreign key constraint for this column
      --sql                     Output SQL without executing it
      --load-extension TEXT     SQLite extensions to load
      -h, --help                Show this message and exit.


extract
=======

See :ref:`cli_extract`.

::

    Usage: sqlite-utils extract [OPTIONS] PATH TABLE COLUMNS...

      Extract one or more columns into a separate table

      Example:

          sqlite-utils extract trees.db Street_Trees species

    Options:
      --table TEXT             Name of the other table to extract columns to
      --fk-column TEXT         Name of the foreign key column to add to the table
      --rename <TEXT TEXT>...  Rename this column in extracted table
      --load-extension TEXT    SQLite extensions to load
      -h, --help               Show this message and exit.


schema
======

See :ref:`cli_schema`.

::

    Usage: sqlite-utils schema [OPTIONS] PATH [TABLES]...

      Show full schema for this database or for specified tables

      Example:

          sqlite-utils schema trees.db

    Options:
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


insert-files
============

See :ref:`cli_insert_files`.

::

    Usage: sqlite-utils insert-files [OPTIONS] PATH TABLE FILE_OR_DIR...

      Insert one or more files using BLOB columns in the specified table

      Example:

          sqlite-utils insert-files pics.db images *.gif \
              -c name:name \
              -c content:content \
              -c content_hash:sha256 \
              -c created:ctime_iso \
              -c modified:mtime_iso \
              -c size:size \
              --pk name

    Options:
      -c, --column TEXT      Column definitions for the table
      --pk TEXT              Column to use as primary key
      --alter                Alter table to add missing columns
      --replace              Replace files with matching primary key
      --upsert               Upsert files with matching primary key
      --name TEXT            File name to use
      --text                 Store file content as TEXT, not BLOB
      --encoding TEXT        Character encoding for input, defaults to utf-8
      -s, --silent           Don't show a progress bar
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


analyze-tables
==============

See :ref:`cli_analyze_tables`.

::

    Usage: sqlite-utils analyze-tables [OPTIONS] PATH [TABLES]...

      Analyze the columns in one or more tables

      Example:

          sqlite-utils analyze-tables data.db trees

    Options:
      -c, --column TEXT      Specific columns to analyze
      --save                 Save results to _analyze_tables table
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


convert
=======

See :ref:`cli_convert`.

::

    Usage: sqlite-utils convert [OPTIONS] DB_PATH TABLE COLUMNS... CODE

      Convert columns using Python code you supply. For example:

          sqlite-utils convert my.db mytable mycolumn \
              '"\n".join(textwrap.wrap(value, 10))' \
              --import=textwrap

      "value" is a variable with the column value to be converted.

      Use "-" for CODE to read Python code from standard input.

      The following common operations are available as recipe functions:

      r.jsonsplit(value, delimiter=',', type=<class 'str'>)

        Convert a string like a,b,c into a JSON array ["a", "b", "c"]

      r.parsedate(value, dayfirst=False, yearfirst=False)

        Parse a date and convert it to ISO date format: yyyy-mm-dd

      r.parsedatetime(value, dayfirst=False, yearfirst=False)

        Parse a datetime and convert it to ISO datetime format: yyyy-mm-ddTHH:MM:SS

      You can use these recipes like so:

          sqlite-utils convert my.db mytable mycolumn \
              'r.jsonsplit(value, delimiter=":")'

    Options:
      --import TEXT                   Python modules to import
      --dry-run                       Show results of running this against first 10
                                      rows
      --multi                         Populate columns for keys in returned
                                      dictionary
      --where TEXT                    Optional where clause
      -p, --param <TEXT TEXT>...      Named :parameters for where clause
      --output TEXT                   Optional separate column to populate with the
                                      output
      --output-type [integer|float|blob|text]
                                      Column type to use for the output column
      --drop                          Drop original column afterwards
      -s, --silent                    Don't show a progress bar
      -h, --help                      Show this message and exit.


tables
======

See :ref:`cli_tables`.

::

    Usage: sqlite-utils tables [OPTIONS] PATH

      List the tables in the database

      Example:

          sqlite-utils tables trees.db

    Options:
      --fts4                 Just show FTS4 enabled tables
      --fts5                 Just show FTS5 enabled tables
      --counts               Include row counts per table
      --nl                   Output newline-delimited JSON
      --arrays               Output rows as arrays instead of objects
      --csv                  Output CSV
      --tsv                  Output TSV
      --no-headers           Omit CSV headers
      -t, --table            Output as a formatted table
      --fmt TEXT             Table format - one of fancy_grid, fancy_outline,
                             github, grid, html, jira, latex, latex_booktabs,
                             latex_longtable, latex_raw, mediawiki, moinmoin,
                             orgtbl, pipe, plain, presto, pretty, psql, rst, simple,
                             textile, tsv, unsafehtml, youtrack
      --json-cols            Detect JSON cols and output them as JSON, not escaped
                             strings
      --columns              Include list of columns for each table
      --schema               Include schema for each table
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


views
=====

See :ref:`cli_views`.

::

    Usage: sqlite-utils views [OPTIONS] PATH

      List the views in the database

      Example:

          sqlite-utils views trees.db

    Options:
      --counts               Include row counts per view
      --nl                   Output newline-delimited JSON
      --arrays               Output rows as arrays instead of objects
      --csv                  Output CSV
      --tsv                  Output TSV
      --no-headers           Omit CSV headers
      -t, --table            Output as a formatted table
      --fmt TEXT             Table format - one of fancy_grid, fancy_outline,
                             github, grid, html, jira, latex, latex_booktabs,
                             latex_longtable, latex_raw, mediawiki, moinmoin,
                             orgtbl, pipe, plain, presto, pretty, psql, rst, simple,
                             textile, tsv, unsafehtml, youtrack
      --json-cols            Detect JSON cols and output them as JSON, not escaped
                             strings
      --columns              Include list of columns for each view
      --schema               Include schema for each view
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


rows
====

See :ref:`cli_rows`.

::

    Usage: sqlite-utils rows [OPTIONS] PATH DBTABLE

      Output all rows in the specified table

      Example:

          sqlite-utils rows trees.db Trees

    Options:
      -c, --column TEXT           Columns to return
      --where TEXT                Optional where clause
      -p, --param <TEXT TEXT>...  Named :parameters for where clause
      --limit INTEGER             Number of rows to return - defaults to everything
      --offset INTEGER            SQL offset to use
      --nl                        Output newline-delimited JSON
      --arrays                    Output rows as arrays instead of objects
      --csv                       Output CSV
      --tsv                       Output TSV
      --no-headers                Omit CSV headers
      -t, --table                 Output as a formatted table
      --fmt TEXT                  Table format - one of fancy_grid, fancy_outline,
                                  github, grid, html, jira, latex, latex_booktabs,
                                  latex_longtable, latex_raw, mediawiki, moinmoin,
                                  orgtbl, pipe, plain, presto, pretty, psql, rst,
                                  simple, textile, tsv, unsafehtml, youtrack
      --json-cols                 Detect JSON cols and output them as JSON, not
                                  escaped strings
      --load-extension TEXT       SQLite extensions to load
      -h, --help                  Show this message and exit.


triggers
========

See :ref:`cli_triggers`.

::

    Usage: sqlite-utils triggers [OPTIONS] PATH [TABLES]...

      Show triggers configured in this database

      Example:

          sqlite-utils triggers trees.db

    Options:
      --nl                   Output newline-delimited JSON
      --arrays               Output rows as arrays instead of objects
      --csv                  Output CSV
      --tsv                  Output TSV
      --no-headers           Omit CSV headers
      -t, --table            Output as a formatted table
      --fmt TEXT             Table format - one of fancy_grid, fancy_outline,
                             github, grid, html, jira, latex, latex_booktabs,
                             latex_longtable, latex_raw, mediawiki, moinmoin,
                             orgtbl, pipe, plain, presto, pretty, psql, rst, simple,
                             textile, tsv, unsafehtml, youtrack
      --json-cols            Detect JSON cols and output them as JSON, not escaped
                             strings
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


indexes
=======

See :ref:`cli_indexes`.

::

    Usage: sqlite-utils indexes [OPTIONS] PATH [TABLES]...

      Show indexes for the whole database or specific tables

      Example:

          sqlite-utils indexes trees.db Trees

    Options:
      --aux                  Include auxiliary columns
      --nl                   Output newline-delimited JSON
      --arrays               Output rows as arrays instead of objects
      --csv                  Output CSV
      --tsv                  Output TSV
      --no-headers           Omit CSV headers
      -t, --table            Output as a formatted table
      --fmt TEXT             Table format - one of fancy_grid, fancy_outline,
                             github, grid, html, jira, latex, latex_booktabs,
                             latex_longtable, latex_raw, mediawiki, moinmoin,
                             orgtbl, pipe, plain, presto, pretty, psql, rst, simple,
                             textile, tsv, unsafehtml, youtrack
      --json-cols            Detect JSON cols and output them as JSON, not escaped
                             strings
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


create-database
===============

See :ref:`cli_create_database`.

::

    Usage: sqlite-utils create-database [OPTIONS] PATH

      Create a new empty database file

      Example:

          sqlite-utils create-database trees.db

    Options:
      --enable-wal  Enable WAL mode on the created database
      -h, --help    Show this message and exit.


create-table
============

See :ref:`cli_create_table`.

::

    Usage: sqlite-utils create-table [OPTIONS] PATH TABLE COLUMNS...

      Add a table with the specified columns. Columns should be specified using
      name, type pairs, for example:

          sqlite-utils create-table my.db people \
              id integer \
              name text \
              height float \
              photo blob --pk id

    Options:
      --pk TEXT                 Column to use as primary key
      --not-null TEXT           Columns that should be created as NOT NULL
      --default <TEXT TEXT>...  Default value that should be set for a column
      --fk <TEXT TEXT TEXT>...  Column, other table, other column to set as a
                                foreign key
      --ignore                  If table already exists, do nothing
      --replace                 If table already exists, replace it
      --load-extension TEXT     SQLite extensions to load
      -h, --help                Show this message and exit.


create-index
============

See :ref:`cli_create_index`.

::

    Usage: sqlite-utils create-index [OPTIONS] PATH TABLE COLUMN...

      Add an index to the specified table for the specified columns

      Example:

          sqlite-utils create-index chickens.db chickens name

      To create an index in descending order:

          sqlite-utils create-index chickens.db chickens -- -name

    Options:
      --name TEXT            Explicit name for the new index
      --unique               Make this a unique index
      --if-not-exists        Ignore if index already exists
      --analyze              Run ANALYZE after creating the index
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


enable-fts
==========

See :ref:`cli_fts`.

::

    Usage: sqlite-utils enable-fts [OPTIONS] PATH TABLE COLUMN...

      Enable full-text search for specific table and columns"

      Example:

          sqlite-utils enable-fts chickens.db chickens name

    Options:
      --fts4                 Use FTS4
      --fts5                 Use FTS5
      --tokenize TEXT        Tokenizer to use, e.g. porter
      --create-triggers      Create triggers to update the FTS tables when the
                             parent table changes.
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


populate-fts
============

::

    Usage: sqlite-utils populate-fts [OPTIONS] PATH TABLE COLUMN...

      Re-populate full-text search for specific table and columns

      Example:

          sqlite-utils populate-fts chickens.db chickens name

    Options:
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


rebuild-fts
===========

::

    Usage: sqlite-utils rebuild-fts [OPTIONS] PATH [TABLES]...

      Rebuild all or specific full-text search tables

      Example:

          sqlite-utils rebuild-fts chickens.db chickens

    Options:
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


disable-fts
===========

::

    Usage: sqlite-utils disable-fts [OPTIONS] PATH TABLE

      Disable full-text search for specific table

      Example:

          sqlite-utils disable-fts chickens.db chickens

    Options:
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


optimize
========

See :ref:`cli_optimize`.

::

    Usage: sqlite-utils optimize [OPTIONS] PATH [TABLES]...

      Optimize all full-text search tables and then run VACUUM - should shrink the
      database file

      Example:

          sqlite-utils optimize chickens.db

    Options:
      --no-vacuum            Don't run VACUUM
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


analyze
=======

See :ref:`cli_analyze`.

::

    Usage: sqlite-utils analyze [OPTIONS] PATH [NAMES]...

      Run ANALYZE against the whole database, or against specific named indexes and
      tables

      Example:

          sqlite-utils analyze chickens.db

    Options:
      -h, --help  Show this message and exit.


vacuum
======

See :ref:`cli_vacuum`.

::

    Usage: sqlite-utils vacuum [OPTIONS] PATH

      Run VACUUM against the database

      Example:

          sqlite-utils vacuum chickens.db

    Options:
      -h, --help  Show this message and exit.


dump
====

See :ref:`cli_dump`.

::

    Usage: sqlite-utils dump [OPTIONS] PATH

      Output a SQL dump of the schema and full contents of the database

      Example:

          sqlite-utils dump chickens.db

    Options:
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


add-column
==========

See :ref:`cli_add_column`.

::

    Usage: sqlite-utils add-column [OPTIONS] PATH TABLE COL_NAME
                          [[integer|float|blob|text|INTEGER|FLOAT|BLOB|TEXT]]

      Add a column to the specified table

      Example:

          sqlite-utils add-column chickens.db chickens weight float

    Options:
      --fk TEXT                Table to reference as a foreign key
      --fk-col TEXT            Referenced column on that foreign key table - if
                               omitted will automatically use the primary key
      --not-null-default TEXT  Add NOT NULL DEFAULT 'TEXT' constraint
      --load-extension TEXT    SQLite extensions to load
      -h, --help               Show this message and exit.


add-foreign-key
===============

See :ref:`cli_add_foreign_key`.

::

    Usage: sqlite-utils add-foreign-key [OPTIONS] PATH TABLE COLUMN [OTHER_TABLE]
                               [OTHER_COLUMN]

      Add a new foreign key constraint to an existing table

      Example:

          sqlite-utils add-foreign-key my.db books author_id authors id

      WARNING: Could corrupt your database! Back up your database file first.

    Options:
      --ignore               If foreign key already exists, do nothing
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


add-foreign-keys
================

See :ref:`cli_add_foreign_keys`.

::

    Usage: sqlite-utils add-foreign-keys [OPTIONS] PATH [FOREIGN_KEY]...

      Add multiple new foreign key constraints to a database

      Example:

          sqlite-utils add-foreign-keys my.db \
              books author_id authors id \
              authors country_id countries id

    Options:
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


index-foreign-keys
==================

See :ref:`cli_index_foreign_keys`.

::

    Usage: sqlite-utils index-foreign-keys [OPTIONS] PATH

      Ensure every foreign key column has an index on it

      Example:

          sqlite-utils index-foreign-keys chickens.db

    Options:
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


enable-wal
==========

See :ref:`cli_wal`.

::

    Usage: sqlite-utils enable-wal [OPTIONS] PATH...

      Enable WAL for database files

      Example:

          sqlite-utils enable-wal chickens.db

    Options:
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


disable-wal
===========

::

    Usage: sqlite-utils disable-wal [OPTIONS] PATH...

      Disable WAL for database files

      Example:

          sqlite-utils disable-wal chickens.db

    Options:
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


enable-counts
=============

See :ref:`cli_enable_counts`.

::

    Usage: sqlite-utils enable-counts [OPTIONS] PATH [TABLES]...

      Configure triggers to update a _counts table with row counts

      Example:

          sqlite-utils enable-counts chickens.db

    Options:
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


reset-counts
============

::

    Usage: sqlite-utils reset-counts [OPTIONS] PATH

      Reset calculated counts in the _counts table

      Example:

          sqlite-utils reset-counts chickens.db

    Options:
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


drop-table
==========

See :ref:`cli_drop_table`.

::

    Usage: sqlite-utils drop-table [OPTIONS] PATH TABLE

      Drop the specified table

      Example:

          sqlite-utils drop-table chickens.db chickens

    Options:
      --ignore
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


create-view
===========

See :ref:`cli_create_view`.

::

    Usage: sqlite-utils create-view [OPTIONS] PATH VIEW SELECT

      Create a view for the provided SELECT query

      Example:

          sqlite-utils create-view chickens.db heavy_chickens \
            'select * from chickens where weight > 3'

    Options:
      --ignore               If view already exists, do nothing
      --replace              If view already exists, replace it
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


drop-view
=========

See :ref:`cli_drop_view`.

::

    Usage: sqlite-utils drop-view [OPTIONS] PATH VIEW

      Drop the specified view

      Example:

          sqlite-utils drop-view chickens.db heavy_chickens

    Options:
      --ignore
      --load-extension TEXT  SQLite extensions to load
      -h, --help             Show this message and exit.


.. [[[end]]]
