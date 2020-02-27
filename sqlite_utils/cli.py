import click
from click_default_group import DefaultGroup
import sqlite_utils
from sqlite_utils.db import AlterError
import itertools
import json
import sys
import csv as csv_std
import tabulate
from .utils import sqlite3


def output_options(fn):
    for decorator in reversed(
        (
            click.option(
                "--nl",
                help="Output newline-delimited JSON",
                is_flag=True,
                default=False,
            ),
            click.option(
                "--arrays",
                help="Output rows as arrays instead of objects",
                is_flag=True,
                default=False,
            ),
            click.option("-c", "--csv", is_flag=True, help="Output CSV"),
            click.option("--no-headers", is_flag=True, help="Omit CSV headers"),
            click.option("-t", "--table", is_flag=True, help="Output as a table"),
            click.option(
                "-f",
                "--fmt",
                help="Table format - one of {}".format(
                    ", ".join(tabulate.tabulate_formats)
                ),
                default="simple",
            ),
            click.option(
                "--json-cols",
                help="Detect JSON cols and output them as JSON, not escaped strings",
                is_flag=True,
                default=False,
            ),
        )
    ):
        fn = decorator(fn)
    return fn


@click.group(cls=DefaultGroup, default="query", default_if_no_args=True)
@click.version_option()
def cli():
    "Commands for interacting with a SQLite database"
    pass


@cli.command()
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.option(
    "--fts4", help="Just show FTS4 enabled tables", default=False, is_flag=True
)
@click.option(
    "--fts5", help="Just show FTS5 enabled tables", default=False, is_flag=True
)
@click.option(
    "--counts", help="Include row counts per table", default=False, is_flag=True
)
@output_options
@click.option(
    "--columns",
    help="Include list of columns for each table",
    is_flag=True,
    default=False,
)
def tables(
    path,
    fts4,
    fts5,
    counts,
    nl,
    arrays,
    csv,
    no_headers,
    table,
    fmt,
    columns,
    json_cols,
):
    """List the tables in the database"""
    db = sqlite_utils.Database(path)
    headers = ["table"]
    if counts:
        headers.append("count")
    if columns:
        headers.append("columns")

    def _iter():
        for name in db.table_names(fts4=fts4, fts5=fts5):
            row = [name]
            if counts:
                row.append(db[name].count)
            if columns:
                cols = [c.name for c in db[name].columns]
                if csv:
                    row.append("\n".join(cols))
                else:
                    row.append(cols)
            yield row

    if table:
        print(tabulate.tabulate(_iter(), headers=headers, tablefmt=fmt))
    elif csv:
        writer = csv_std.writer(sys.stdout)
        if not no_headers:
            writer.writerow(headers)
        for row in _iter():
            writer.writerow(row)
    else:
        for line in output_rows(_iter(), headers, nl, arrays, json_cols):
            click.echo(line)


@cli.command()
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
def vacuum(path):
    """Run VACUUM against the database"""
    sqlite_utils.Database(path).vacuum()


@cli.command()
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.option("--no-vacuum", help="Don't run VACUUM", default=False, is_flag=True)
def optimize(path, no_vacuum):
    """Optimize all FTS tables and then run VACUUM - should shrink the database file"""
    db = sqlite_utils.Database(path)
    tables = db.table_names(fts4=True) + db.table_names(fts5=True)
    with db.conn:
        for table in tables:
            db[table].optimize()
    if not no_vacuum:
        db.vacuum()


@cli.command(name="add-column")
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("table")
@click.argument("col_name")
@click.argument(
    "col_type",
    type=click.Choice(
        ["integer", "float", "blob", "text", "INTEGER", "FLOAT", "BLOB", "TEXT"]
    ),
    required=False,
)
@click.option(
    "--fk", type=str, required=False, help="Table to reference as a foreign key"
)
@click.option(
    "--fk-col",
    type=str,
    required=False,
    help="Referenced column on that foreign key table - if omitted will automatically use the primary key",
)
@click.option(
    "--not-null-default",
    type=str,
    required=False,
    help="Add NOT NULL DEFAULT 'TEXT' constraint",
)
def add_column(path, table, col_name, col_type, fk, fk_col, not_null_default):
    "Add a column to the specified table"
    db = sqlite_utils.Database(path)
    db[table].add_column(
        col_name, col_type, fk=fk, fk_col=fk_col, not_null_default=not_null_default
    )


@cli.command(name="add-foreign-key")
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("table")
@click.argument("column")
@click.argument("other_table", required=False)
@click.argument("other_column", required=False)
def add_foreign_key(path, table, column, other_table, other_column):
    """
    Add a new foreign key constraint to an existing table. Example usage:

        $ sqlite-utils add-foreign-key my.db books author_id authors id

    WARNING: Could corrupt your database! Back up your database file first.
    """
    db = sqlite_utils.Database(path)
    try:
        db[table].add_foreign_key(column, other_table, other_column)
    except AlterError as e:
        raise click.ClickException(e)


@cli.command(name="index-foreign-keys")
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
def index_foreign_keys(path):
    """
    Ensure every foreign key column has an index on it.
    """
    db = sqlite_utils.Database(path)
    db.index_foreign_keys()


@cli.command(name="create-index")
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("table")
@click.argument("column", nargs=-1, required=True)
@click.option("--name", help="Explicit name for the new index")
@click.option("--unique", help="Make this a unique index", default=False, is_flag=True)
@click.option(
    "--if-not-exists",
    help="Ignore if index already exists",
    default=False,
    is_flag=True,
)
def create_index(path, table, column, name, unique, if_not_exists):
    "Add an index to the specified table covering the specified columns"
    db = sqlite_utils.Database(path)
    db[table].create_index(
        column, index_name=name, unique=unique, if_not_exists=if_not_exists
    )


@cli.command(name="enable-fts")
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("table")
@click.argument("column", nargs=-1, required=True)
@click.option("--fts4", help="Use FTS4", default=False, is_flag=True)
@click.option("--fts5", help="Use FTS5", default=False, is_flag=True)
@click.option(
    "--create-triggers",
    help="Create triggers to update the FTS tables when the parent table changes.",
    default=False,
    is_flag=True,
)
def enable_fts(path, table, column, fts4, fts5, create_triggers):
    "Enable FTS for specific table and columns"
    fts_version = "FTS5"
    if fts4 and fts5:
        click.echo("Can only use one of --fts4 or --fts5", err=True)
        return
    elif fts4:
        fts_version = "FTS4"

    db = sqlite_utils.Database(path)
    db[table].enable_fts(
        column, fts_version=fts_version, create_triggers=create_triggers
    )


@cli.command(name="populate-fts")
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("table")
@click.argument("column", nargs=-1, required=True)
def populate_fts(path, table, column):
    "Re-populate FTS for specific table and columns"
    db = sqlite_utils.Database(path)
    db[table].populate_fts(column)


@cli.command(name="disable-fts")
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("table")
def disable_fts(path, table):
    "Disable FTS for specific table"
    db = sqlite_utils.Database(path)
    db[table].disable_fts()


def insert_upsert_options(fn):
    for decorator in reversed(
        (
            click.argument(
                "path",
                type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
                required=True,
            ),
            click.argument("table"),
            click.argument("json_file", type=click.File(), required=True),
            click.option(
                "--pk", help="Columns to use as the primary key, e.g. id", multiple=True
            ),
            click.option("--nl", is_flag=True, help="Expect newline-delimited JSON"),
            click.option("-c", "--csv", is_flag=True, help="Expect CSV"),
            click.option("--tsv", is_flag=True, help="Expect TSV"),
            click.option(
                "--batch-size", type=int, default=100, help="Commit every X records"
            ),
            click.option(
                "--alter",
                is_flag=True,
                help="Alter existing table to add any missing columns",
            ),
            click.option(
                "--not-null",
                multiple=True,
                help="Columns that should be created as NOT NULL",
            ),
            click.option(
                "--default",
                multiple=True,
                type=(str, str),
                help="Default value that should be set for a column",
            ),
        )
    ):
        fn = decorator(fn)
    return fn


def insert_upsert_implementation(
    path,
    table,
    json_file,
    pk,
    nl,
    csv,
    tsv,
    batch_size,
    alter,
    upsert,
    ignore=False,
    replace=False,
    not_null=None,
    default=None,
):
    db = sqlite_utils.Database(path)
    if (nl + csv + tsv) >= 2:
        raise click.ClickException("Use just one of --nl, --csv or --tsv")
    if pk and len(pk) == 1:
        pk = pk[0]
    if csv or tsv:
        dialect = "excel-tab" if tsv else "excel"
        reader = csv_std.reader(json_file, dialect=dialect)
        headers = next(reader)
        docs = (dict(zip(headers, row)) for row in reader)
    elif nl:
        docs = (json.loads(line) for line in json_file)
    else:
        docs = json.load(json_file)
        if isinstance(docs, dict):
            docs = [docs]
    extra_kwargs = {"ignore": ignore, "replace": replace}
    if not_null:
        extra_kwargs["not_null"] = set(not_null)
    if default:
        extra_kwargs["defaults"] = dict(default)
    if upsert:
        extra_kwargs["upsert"] = upsert
    db[table].insert_all(
        docs, pk=pk, batch_size=batch_size, alter=alter, **extra_kwargs
    )


@cli.command()
@insert_upsert_options
@click.option(
    "--ignore", is_flag=True, default=False, help="Ignore records if pk already exists"
)
@click.option(
    "--replace",
    is_flag=True,
    default=False,
    help="Replace records if pk already exists",
)
def insert(
    path,
    table,
    json_file,
    pk,
    nl,
    csv,
    tsv,
    batch_size,
    alter,
    ignore,
    replace,
    not_null,
    default,
):
    """
    Insert records from JSON file into a table, creating the table if it
    does not already exist.

    Input should be a JSON array of objects, unless --nl or --csv is used.
    """
    insert_upsert_implementation(
        path,
        table,
        json_file,
        pk,
        nl,
        csv,
        tsv,
        batch_size,
        alter=alter,
        upsert=False,
        ignore=ignore,
        replace=replace,
        not_null=not_null,
        default=default,
    )


@cli.command()
@insert_upsert_options
def upsert(
    path, table, json_file, pk, nl, csv, tsv, batch_size, alter, not_null, default
):
    """
    Upsert records based on their primary key. Works like 'insert' but if
    an incoming record has a primary key that matches an existing record
    the existing record will be updated.
    """
    insert_upsert_implementation(
        path,
        table,
        json_file,
        pk,
        nl,
        csv,
        tsv,
        batch_size,
        alter=alter,
        upsert=True,
        not_null=not_null,
        default=default,
    )


@cli.command()
@click.argument(
    "path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("sql")
@output_options
def query(path, sql, nl, arrays, csv, no_headers, table, fmt, json_cols):
    "Execute SQL query and return the results as JSON"
    db = sqlite_utils.Database(path)
    cursor = iter(db.conn.execute(sql))
    headers = [c[0] for c in cursor.description]
    if table:
        print(tabulate.tabulate(list(cursor), headers=headers, tablefmt=fmt))
    elif csv:
        writer = csv_std.writer(sys.stdout)
        if not no_headers:
            writer.writerow([c[0] for c in cursor.description])
        for row in cursor:
            writer.writerow(row)
    else:
        for line in output_rows(cursor, headers, nl, arrays, json_cols):
            click.echo(line)


@cli.command()
@click.argument(
    "path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("dbtable")
@output_options
@click.pass_context
def rows(ctx, path, dbtable, nl, arrays, csv, no_headers, table, fmt, json_cols):
    "Output all rows in the specified table"
    ctx.invoke(
        query,
        path=path,
        sql="select * from [{}]".format(dbtable),
        nl=nl,
        arrays=arrays,
        csv=csv,
        no_headers=no_headers,
        table=table,
        fmt=fmt,
        json_cols=json_cols,
    )


def output_rows(iterator, headers, nl, arrays, json_cols):
    # We have to iterate two-at-a-time so we can know if we
    # should output a trailing comma or if we have reached
    # the last row.
    current_iter, next_iter = itertools.tee(iterator, 2)
    next(next_iter, None)
    first = True
    for row, next_row in itertools.zip_longest(current_iter, next_iter):
        is_last = next_row is None
        data = row
        if json_cols:
            # Any value that is a valid JSON string should be treated as JSON
            data = [maybe_json(value) for value in data]
        if not arrays:
            data = dict(zip(headers, data))
        line = "{firstchar}{serialized}{maybecomma}{lastchar}".format(
            firstchar=("[" if first else " ") if not nl else "",
            serialized=json.dumps(data),
            maybecomma="," if (not nl and not is_last) else "",
            lastchar="]" if (is_last and not nl) else "",
        )
        yield line
        first = False


def maybe_json(value):
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not (stripped.startswith("{") or stripped.startswith("[")):
        return value
    try:
        return json.loads(stripped)
    except ValueError:
        return value
