import click
from click_default_group import DefaultGroup
import sqlite_utils
import itertools
import json
import sys
import csv as csv_std
import tabulate
import sqlite3


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
def tables(path, fts4, fts5, counts, nl, arrays, csv, no_headers, table, fmt, columns):
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
        for line in output_rows(_iter(), headers, nl, arrays):
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


@cli.command(name="enable-fts")
@click.argument(
    "path",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("table")
@click.argument("column", nargs=-1, required=True)
@click.option(
    "--fts4", help="Just show FTS4 enabled tables", default=False, is_flag=True
)
@click.option(
    "--fts5", help="Just show FTS5 enabled tables", default=False, is_flag=True
)
def enable_fts(path, table, column, fts4, fts5):
    "Enable FTS for specific table and columns"
    fts_version = "FTS5"
    if fts4 and fts5:
        click.echo("Can only use one of --fts4 or --fts5", err=True)
        return
    elif fts4:
        fts_version = "FTS4"

    db = sqlite_utils.Database(path)
    db[table].enable_fts(column, fts_version=fts_version)


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
            click.option("--pk", help="Column to use as the primary key, e.g. id"),
            click.option("--nl", is_flag=True, help="Expect newline-delimited JSON"),
            click.option("-c", "--csv", is_flag=True, help="Expect CSV"),
            click.option(
                "--batch-size", type=int, default=100, help="Commit every X records"
            ),
        )
    ):
        fn = decorator(fn)
    return fn


def insert_upsert_implementation(
    path, table, json_file, pk, nl, csv, batch_size, upsert
):
    db = sqlite_utils.Database(path)
    if nl and csv:
        click.echo("Use just one of --nl and --csv", err=True)
        return
    if csv:
        reader = csv_std.reader(json_file)
        headers = next(reader)
        docs = (dict(zip(headers, row)) for row in reader)
    elif nl:
        docs = (json.loads(line) for line in json_file)
    else:
        docs = json.load(json_file)
        if isinstance(docs, dict):
            docs = [docs]
    if upsert:
        method = db[table].upsert_all
    else:
        method = db[table].insert_all
    method(docs, pk=pk, batch_size=batch_size)


@cli.command()
@insert_upsert_options
def insert(path, table, json_file, pk, nl, csv, batch_size):
    """
    Insert records from JSON file into a table, creating the table if it
    does not already exist.

    Input should be a JSON array of objects, unless --nl or --csv is used.
    """
    insert_upsert_implementation(
        path, table, json_file, pk, nl, csv, batch_size, upsert=False
    )


@cli.command()
@insert_upsert_options
def upsert(path, table, json_file, pk, nl, csv, batch_size):
    """
    Upsert records based on their primary key. Works like 'insert' but if
    an incoming record has a primary key that matches an existing record
    the existing record will be replaced.
    """
    insert_upsert_implementation(
        path, table, json_file, pk, nl, csv, batch_size, upsert=True
    )


@cli.command()
@click.argument(
    "path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("sql")
@output_options
def query(path, sql, nl, arrays, csv, no_headers, table, fmt):
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
        for line in output_rows(cursor, headers, nl, arrays):
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
def rows(ctx, path, dbtable, nl, arrays, csv, no_headers, table, fmt):
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
    )


def output_rows(iterator, headers, nl, arrays):
    # We have to iterate two-at-a-time so we can know if we
    # should output a trailing comma or if we have reached
    # the last row.
    current_iter, next_iter = itertools.tee(iterator, 2)
    next(next_iter, None)
    first = True
    for row, next_row in itertools.zip_longest(current_iter, next_iter):
        is_last = next_row is None
        data = row
        if not arrays:
            data = dict(zip(headers, row))
        line = "{firstchar}{serialized}{maybecomma}{lastchar}".format(
            firstchar=("[" if first else " ") if not nl else "",
            serialized=json.dumps(data),
            maybecomma="," if (not nl and not is_last) else "",
            lastchar="]" if (is_last and not nl) else "",
        )
        yield line
        first = False
