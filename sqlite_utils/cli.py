import click
import sqlite_utils
import json
import sys
import csv as csv_std


@click.group()
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
def tables(path, fts4, fts5):
    """List the tables in the database"""
    db = sqlite_utils.Database(path)
    for name in db.table_names(fts4=fts4, fts5=fts5):
        print(name)


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


@cli.command()
@click.argument(
    "path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("table")
@click.argument("json_file", type=click.File(), required=True)
@click.option("--pk", help="Column to use as the primary key, e.g. id")
def insert(path, table, json_file, pk):
    "Insert records from JSON file into the table, create table if it is missing"
    db = sqlite_utils.Database(path)
    docs = json.load(json_file)
    if isinstance(docs, dict):
        docs = [docs]
    db[table].insert_all(docs, pk=pk)


@cli.command()
@click.argument(
    "path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("table")
@click.argument("json_file", type=click.File(), required=True)
@click.option("--pk", help="Column to use as the primary key, e.g. id")
def upsert(path, table, json_file, pk):
    "Upsert records based on their primary key"
    db = sqlite_utils.Database(path)
    docs = json.load(json_file)
    if isinstance(docs, dict):
        docs = [docs]
    db[table].upsert_all(docs, pk=pk)


@cli.command()
@click.argument(
    "path",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.argument("sql")
@click.option(
    "--no-headers", help="Exclude headers from CSV output", is_flag=True, default=False
)
def csv(path, sql, no_headers):
    "Execute SQL query and return the results as CSV"
    db = sqlite_utils.Database(path)
    cursor = db.conn.execute(sql)
    writer = csv_std.writer(sys.stdout)
    if not no_headers:
        writer.writerow([c[0] for c in cursor.description])
    for row in cursor:
        writer.writerow(row)
