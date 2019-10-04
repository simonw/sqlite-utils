from sqlite_utils import cli, Database
from sqlite_utils.db import Index, ForeignKey
from click.testing import CliRunner
import json
import os
import pytest
from sqlite_utils.utils import sqlite3

from .utils import collapse_whitespace


CREATE_TABLES = """
create table Gosh (c1 text, c2 text, c3 text);
create table Gosh2 (c1 text, c2 text, c3 text);
"""


@pytest.fixture
def db_path(tmpdir):
    path = str(tmpdir / "test.db")
    db = sqlite3.connect(path)
    db.executescript(CREATE_TABLES)
    return path


def test_tables(db_path):
    result = CliRunner().invoke(cli.cli, ["tables", db_path])
    assert '[{"table": "Gosh"},\n {"table": "Gosh2"}]' == result.output.strip()


def test_tables_fts4(db_path):
    Database(db_path)["Gosh"].enable_fts(["c2"], fts_version="FTS4")
    result = CliRunner().invoke(cli.cli, ["tables", "--fts4", db_path])
    assert '[{"table": "Gosh_fts"}]' == result.output.strip()


def test_tables_fts5(db_path):
    Database(db_path)["Gosh"].enable_fts(["c2"], fts_version="FTS5")
    result = CliRunner().invoke(cli.cli, ["tables", "--fts5", db_path])
    assert '[{"table": "Gosh_fts"}]' == result.output.strip()


def test_tables_counts_and_columns(db_path):
    db = Database(db_path)
    with db.conn:
        db["lots"].insert_all([{"id": i, "age": i + 1} for i in range(30)])
    result = CliRunner().invoke(cli.cli, ["tables", "--counts", "--columns", db_path])
    assert (
        '[{"table": "Gosh", "count": 0, "columns": ["c1", "c2", "c3"]},\n'
        ' {"table": "Gosh2", "count": 0, "columns": ["c1", "c2", "c3"]},\n'
        ' {"table": "lots", "count": 30, "columns": ["id", "age"]}]'
    ) == result.output.strip()


def test_tables_counts_and_columns_csv(db_path):
    db = Database(db_path)
    with db.conn:
        db["lots"].insert_all([{"id": i, "age": i + 1} for i in range(30)])
    result = CliRunner().invoke(
        cli.cli, ["tables", "--counts", "--columns", "--csv", db_path]
    )
    assert (
        "table,count,columns\n"
        'Gosh,0,"c1\n'
        "c2\n"
        'c3"\n'
        'Gosh2,0,"c1\n'
        "c2\n"
        'c3"\n'
        'lots,30,"id\n'
        'age"'
    ) == result.output.strip()


@pytest.mark.parametrize(
    "fmt,expected",
    [
        (
            "simple",
            (
                "c1     c2     c3\n"
                "-----  -----  ----------\n"
                "verb0  noun0  adjective0\n"
                "verb1  noun1  adjective1\n"
                "verb2  noun2  adjective2\n"
                "verb3  noun3  adjective3"
            ),
        ),
        (
            "rst",
            (
                "=====  =====  ==========\n"
                "c1     c2     c3\n"
                "=====  =====  ==========\n"
                "verb0  noun0  adjective0\n"
                "verb1  noun1  adjective1\n"
                "verb2  noun2  adjective2\n"
                "verb3  noun3  adjective3\n"
                "=====  =====  =========="
            ),
        ),
    ],
)
def test_output_table(db_path, fmt, expected):
    db = Database(db_path)
    with db.conn:
        db["rows"].insert_all(
            [
                {
                    "c1": "verb{}".format(i),
                    "c2": "noun{}".format(i),
                    "c3": "adjective{}".format(i),
                }
                for i in range(4)
            ]
        )
    result = CliRunner().invoke(cli.cli, ["rows", db_path, "rows", "-t", "-f", fmt])
    assert 0 == result.exit_code
    assert expected == result.output.strip()


def test_create_index(db_path):
    db = Database(db_path)
    assert [] == db["Gosh"].indexes
    result = CliRunner().invoke(cli.cli, ["create-index", db_path, "Gosh", "c1"])
    assert 0 == result.exit_code
    assert [
        Index(
            seq=0, name="idx_Gosh_c1", unique=0, origin="c", partial=0, columns=["c1"]
        )
    ] == db["Gosh"].indexes
    # Try with a custom name
    result = CliRunner().invoke(
        cli.cli, ["create-index", db_path, "Gosh", "c2", "--name", "blah"]
    )
    assert 0 == result.exit_code
    assert [
        Index(seq=0, name="blah", unique=0, origin="c", partial=0, columns=["c2"]),
        Index(
            seq=1, name="idx_Gosh_c1", unique=0, origin="c", partial=0, columns=["c1"]
        ),
    ] == db["Gosh"].indexes
    # Try a two-column unique index
    create_index_unique_args = [
        "create-index",
        db_path,
        "Gosh2",
        "c1",
        "c2",
        "--unique",
    ]
    result = CliRunner().invoke(cli.cli, create_index_unique_args)
    assert 0 == result.exit_code
    assert [
        Index(
            seq=0,
            name="idx_Gosh2_c1_c2",
            unique=1,
            origin="c",
            partial=0,
            columns=["c1", "c2"],
        )
    ] == db["Gosh2"].indexes
    # Trying to create the same index should fail
    assert 0 != CliRunner().invoke(cli.cli, create_index_unique_args).exit_code
    # ... unless we use --if-not-exists
    assert (
        0
        == CliRunner()
        .invoke(cli.cli, create_index_unique_args + ["--if-not-exists"])
        .exit_code
    )


@pytest.mark.parametrize(
    "col_name,col_type,expected_schema",
    (
        ("text", "TEXT", "CREATE TABLE [dogs] ( [name] TEXT , [text] TEXT)"),
        (
            "integer",
            "INTEGER",
            "CREATE TABLE [dogs] ( [name] TEXT , [integer] INTEGER)",
        ),
        ("float", "FLOAT", "CREATE TABLE [dogs] ( [name] TEXT , [float] FLOAT)"),
        ("blob", "blob", "CREATE TABLE [dogs] ( [name] TEXT , [blob] BLOB)"),
        ("default", None, "CREATE TABLE [dogs] ( [name] TEXT , [default] TEXT)"),
    ),
)
def test_add_column(db_path, col_name, col_type, expected_schema):
    db = Database(db_path)
    db.create_table("dogs", {"name": str})
    assert "CREATE TABLE [dogs] ( [name] TEXT )" == collapse_whitespace(
        db["dogs"].schema
    )
    args = ["add-column", db_path, "dogs", col_name]
    if col_type is not None:
        args.append(col_type)
    assert 0 == CliRunner().invoke(cli.cli, args).exit_code
    assert expected_schema == collapse_whitespace(db["dogs"].schema)


def test_add_column_not_null_default(db_path):
    db = Database(db_path)
    db.create_table("dogs", {"name": str})
    assert "CREATE TABLE [dogs] ( [name] TEXT )" == collapse_whitespace(
        db["dogs"].schema
    )
    args = [
        "add-column",
        db_path,
        "dogs",
        "nickname",
        "--not-null-default",
        "dogs'dawg",
    ]
    assert 0 == CliRunner().invoke(cli.cli, args).exit_code
    assert (
        "CREATE TABLE [dogs] ( [name] TEXT , [nickname] TEXT NOT NULL DEFAULT 'dogs''dawg')"
        == collapse_whitespace(db["dogs"].schema)
    )


@pytest.mark.parametrize(
    "args,assert_message",
    (
        (
            ["books", "author_id", "authors", "id"],
            "Explicit other_table and other_column",
        ),
        (["books", "author_id", "authors"], "Explicit other_table, guess other_column"),
        (["books", "author_id"], "Automatically guess other_table and other_column"),
    ),
)
def test_add_foreign_key(db_path, args, assert_message):
    db = Database(db_path)
    db["authors"].insert_all(
        [{"id": 1, "name": "Sally"}, {"id": 2, "name": "Asheesh"}], pk="id"
    )
    db["books"].insert_all(
        [
            {"title": "Hedgehogs of the world", "author_id": 1},
            {"title": "How to train your wolf", "author_id": 2},
        ]
    )
    assert (
        0 == CliRunner().invoke(cli.cli, ["add-foreign-key", db_path] + args).exit_code
    ), assert_message
    assert [
        ForeignKey(
            table="books", column="author_id", other_table="authors", other_column="id"
        )
    ] == db["books"].foreign_keys
    # Error if we try to add it twice:
    result = CliRunner().invoke(
        cli.cli, ["add-foreign-key", db_path, "books", "author_id", "authors", "id"]
    )

    assert 0 != result.exit_code
    assert (
        "Error: Foreign key already exists for author_id => authors.id"
        == result.output.strip()
    )
    # Error if we try against an invalid column
    result = CliRunner().invoke(
        cli.cli, ["add-foreign-key", db_path, "books", "author_id", "authors", "bad"]
    )
    assert 0 != result.exit_code
    assert "Error: No such column: authors.bad" == result.output.strip()


def test_add_column_foreign_key(db_path):
    db = Database(db_path)
    db["authors"].insert({"id": 1, "name": "Sally"}, pk="id")
    db["books"].insert({"title": "Hedgehogs of the world"})
    # Add an author_id foreign key column to the books table
    result = CliRunner().invoke(
        cli.cli, ["add-column", db_path, "books", "author_id", "--fk", "authors"]
    )
    assert 0 == result.exit_code, result.output
    assert (
        "CREATE TABLE [books] ( [title] TEXT , [author_id] INTEGER, FOREIGN KEY(author_id) REFERENCES authors(id) )"
        == collapse_whitespace(db["books"].schema)
    )
    # Try it again with a custom --fk-col
    result = CliRunner().invoke(
        cli.cli,
        [
            "add-column",
            db_path,
            "books",
            "author_name_ref",
            "--fk",
            "authors",
            "--fk-col",
            "name",
        ],
    )
    assert 0 == result.exit_code, result.output
    assert (
        "CREATE TABLE [books] ( [title] TEXT , [author_id] INTEGER, [author_name_ref] TEXT, "
        "FOREIGN KEY(author_id) REFERENCES authors(id), "
        "FOREIGN KEY(author_name_ref) REFERENCES authors(name) )"
        == collapse_whitespace(db["books"].schema)
    )
    # Throw an error if the --fk table does not exist
    result = CliRunner().invoke(
        cli.cli, ["add-column", db_path, "books", "author_id", "--fk", "bobcats"]
    )
    assert 0 != result.exit_code
    assert "table 'bobcats' does not exist" in str(result.exception)


def test_index_foreign_keys(db_path):
    test_add_column_foreign_key(db_path)
    db = Database(db_path)
    assert [] == db["books"].indexes
    result = CliRunner().invoke(cli.cli, ["index-foreign-keys", db_path])
    assert 0 == result.exit_code
    assert [["author_id"], ["author_name_ref"]] == [
        i.columns for i in db["books"].indexes
    ]


def test_enable_fts(db_path):
    assert None == Database(db_path)["Gosh"].detect_fts()
    result = CliRunner().invoke(
        cli.cli, ["enable-fts", db_path, "Gosh", "c1", "--fts4"]
    )
    assert 0 == result.exit_code
    assert "Gosh_fts" == Database(db_path)["Gosh"].detect_fts()

    # Table names with restricted chars are handled correctly.
    # colons and dots are restricted characters for table names.
    Database(db_path)["http://example.com"].create({"c1": str, "c2": str, "c3": str})
    assert None == Database(db_path)["http://example.com"].detect_fts()
    result = CliRunner().invoke(
        cli.cli, ["enable-fts", db_path, "http://example.com", "c1", "--fts4"]
    )
    assert 0 == result.exit_code
    assert (
        "http://example.com_fts" == Database(db_path)["http://example.com"].detect_fts()
    )
    Database(db_path)["http://example.com"].drop()


def test_enable_fts_with_triggers(db_path):
    Database(db_path)["Gosh"].insert_all([{"c1": "baz"}])
    exit_code = (
        CliRunner()
        .invoke(
            cli.cli,
            ["enable-fts", db_path, "Gosh", "c1", "--fts4", "--create-triggers"],
        )
        .exit_code
    )
    assert 0 == exit_code

    def search(q):
        return (
            Database(db_path)
            .conn.execute("select c1 from Gosh_fts where c1 match ?", [q])
            .fetchall()
        )

    assert [("baz",)] == search("baz")
    Database(db_path)["Gosh"].insert_all([{"c1": "martha"}])
    assert [("martha",)] == search("martha")


def test_populate_fts(db_path):
    Database(db_path)["Gosh"].insert_all([{"c1": "baz"}])
    exit_code = (
        CliRunner()
        .invoke(cli.cli, ["enable-fts", db_path, "Gosh", "c1", "--fts4"])
        .exit_code
    )
    assert 0 == exit_code

    def search(q):
        return (
            Database(db_path)
            .conn.execute("select c1 from Gosh_fts where c1 match ?", [q])
            .fetchall()
        )

    assert [("baz",)] == search("baz")
    Database(db_path)["Gosh"].insert_all([{"c1": "martha"}])
    assert [] == search("martha")
    exit_code = (
        CliRunner().invoke(cli.cli, ["populate-fts", db_path, "Gosh", "c1"]).exit_code
    )
    assert 0 == exit_code
    assert [("martha",)] == search("martha")


def test_vacuum(db_path):
    result = CliRunner().invoke(cli.cli, ["vacuum", db_path])
    assert 0 == result.exit_code


def test_optimize(db_path):
    db = Database(db_path)
    with db.conn:
        for table in ("Gosh", "Gosh2"):
            db[table].insert_all(
                [
                    {
                        "c1": "verb{}".format(i),
                        "c2": "noun{}".format(i),
                        "c3": "adjective{}".format(i),
                    }
                    for i in range(10000)
                ]
            )
        db["Gosh"].enable_fts(["c1", "c2", "c3"], fts_version="FTS4")
        db["Gosh2"].enable_fts(["c1", "c2", "c3"], fts_version="FTS5")
    size_before_optimize = os.stat(db_path).st_size
    result = CliRunner().invoke(cli.cli, ["optimize", db_path])
    assert 0 == result.exit_code
    size_after_optimize = os.stat(db_path).st_size
    assert size_after_optimize < size_before_optimize
    # Sanity check that --no-vacuum doesn't throw errors:
    result = CliRunner().invoke(cli.cli, ["optimize", "--no-vacuum", db_path])
    assert 0 == result.exit_code


def test_insert_simple(tmpdir):
    json_path = str(tmpdir / "dog.json")
    db_path = str(tmpdir / "dogs.db")
    open(json_path, "w").write(json.dumps({"name": "Cleo", "age": 4}))
    result = CliRunner().invoke(cli.cli, ["insert", db_path, "dogs", json_path])
    assert 0 == result.exit_code
    assert [{"age": 4, "name": "Cleo"}] == Database(db_path).execute_returning_dicts(
        "select * from dogs"
    )
    db = Database(db_path)
    assert ["dogs"] == db.table_names()
    assert [] == db["dogs"].indexes


def test_insert_with_primary_key(db_path, tmpdir):
    json_path = str(tmpdir / "dog.json")
    open(json_path, "w").write(json.dumps({"id": 1, "name": "Cleo", "age": 4}))
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id"]
    )
    assert 0 == result.exit_code
    assert [{"id": 1, "age": 4, "name": "Cleo"}] == Database(
        db_path
    ).execute_returning_dicts("select * from dogs")
    db = Database(db_path)
    assert ["id"] == db["dogs"].pks


def test_insert_multiple_with_primary_key(db_path, tmpdir):
    json_path = str(tmpdir / "dogs.json")
    dogs = [{"id": i, "name": "Cleo {}".format(i), "age": i + 3} for i in range(1, 21)]
    open(json_path, "w").write(json.dumps(dogs))
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id"]
    )
    assert 0 == result.exit_code
    db = Database(db_path)
    assert dogs == db.execute_returning_dicts("select * from dogs order by id")
    assert ["id"] == db["dogs"].pks


def test_insert_multiple_with_compound_primary_key(db_path, tmpdir):
    json_path = str(tmpdir / "dogs.json")
    dogs = [
        {"breed": "mixed", "id": i, "name": "Cleo {}".format(i), "age": i + 3}
        for i in range(1, 21)
    ]
    open(json_path, "w").write(json.dumps(dogs))
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id", "--pk", "breed"]
    )
    assert 0 == result.exit_code
    db = Database(db_path)
    assert dogs == db.execute_returning_dicts("select * from dogs order by breed, id")
    assert {"breed", "id"} == set(db["dogs"].pks)
    assert (
        "CREATE TABLE [dogs] (\n"
        "   [breed] TEXT,\n"
        "   [id] INTEGER,\n"
        "   [name] TEXT,\n"
        "   [age] INTEGER,\n"
        "   PRIMARY KEY ([id], [breed])\n"
        ")"
    ) == db["dogs"].schema


def test_insert_not_null_default(db_path, tmpdir):
    json_path = str(tmpdir / "dogs.json")
    dogs = [
        {"id": i, "name": "Cleo {}".format(i), "age": i + 3, "score": 10}
        for i in range(1, 21)
    ]
    open(json_path, "w").write(json.dumps(dogs))
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "dogs", json_path, "--pk", "id"]
        + ["--not-null", "name", "--not-null", "age"]
        + ["--default", "score", "5", "--default", "age", "1"],
    )
    assert 0 == result.exit_code
    db = Database(db_path)
    assert (
        "CREATE TABLE [dogs] (\n"
        "   [id] INTEGER PRIMARY KEY,\n"
        "   [name] TEXT NOT NULL,\n"
        "   [age] INTEGER NOT NULL DEFAULT '1',\n"
        "   [score] INTEGER DEFAULT '5'\n)"
    ) == db["dogs"].schema


def test_insert_newline_delimited(db_path):
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "from_json_nl", "-", "--nl"],
        input='{"foo": "bar", "n": 1}\n{"foo": "baz", "n": 2}',
    )
    assert 0 == result.exit_code, result.output
    db = Database(db_path)
    assert [
        {"foo": "bar", "n": 1},
        {"foo": "baz", "n": 2},
    ] == db.execute_returning_dicts("select foo, n from from_json_nl")


def test_insert_ignore(db_path, tmpdir):
    db = Database(db_path)
    db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
    json_path = str(tmpdir / "dogs.json")
    open(json_path, "w").write(json.dumps([{"id": 1, "name": "Bailey"}]))
    # Should raise error without --ignore
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id"]
    )
    assert 0 != result.exit_code, result.output
    # If we use --ignore it should run OK
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id", "--ignore"]
    )
    assert 0 == result.exit_code, result.output
    # ... but it should actually have no effect
    assert [{"id": 1, "name": "Cleo"}] == db.execute_returning_dicts(
        "select * from dogs"
    )


@pytest.mark.parametrize(
    "content,option",
    (("foo\tbar\tbaz\n1\t2\t3", "--tsv"), ("foo,bar,baz\n1,2,3", "--csv")),
)
def test_insert_csv_tsv(content, option, db_path, tmpdir):
    db = Database(db_path)
    file_path = str(tmpdir / "insert.csv-tsv")
    open(file_path, "w").write(content)
    result = CliRunner().invoke(cli.cli, ["insert", db_path, "data", file_path, option])
    assert 0 == result.exit_code
    assert [{"foo": "1", "bar": "2", "baz": "3"}] == list(db["data"].rows)


@pytest.mark.parametrize(
    "options",
    (
        ["--tsv", "--nl"],
        ["--tsv", "--csv"],
        ["--csv", "--nl"],
        ["--csv", "--nl", "--tsv"],
    ),
)
def test_only_allow_one_of_nl_tsv_csv(options, db_path, tmpdir):
    file_path = str(tmpdir / "insert.csv-tsv")
    open(file_path, "w").write("foo")
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "data", file_path] + options
    )
    assert 0 != result.exit_code
    assert "Error: Use just one of --nl, --csv or --tsv" == result.output.strip()


def test_upsert(db_path, tmpdir):
    test_insert_multiple_with_primary_key(db_path, tmpdir)
    json_path = str(tmpdir / "upsert.json")
    db = Database(db_path)
    assert 20 == db["dogs"].count
    upsert_dogs = [
        {"id": 1, "name": "Upserted 1", "age": 4},
        {"id": 2, "name": "Upserted 2", "age": 4},
        {"id": 21, "name": "Fresh insert 21", "age": 6},
    ]
    open(json_path, "w").write(json.dumps(upsert_dogs))
    result = CliRunner().invoke(
        cli.cli, ["upsert", db_path, "dogs", json_path, "--pk", "id"]
    )
    assert 0 == result.exit_code
    assert 21 == db["dogs"].count
    assert upsert_dogs == db.execute_returning_dicts(
        "select * from dogs where id in (1, 2, 21) order by id"
    )


def test_insert_alter(db_path, tmpdir):
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "from_json_nl", "-", "--nl"],
        input='{"foo": "bar", "n": 1}\n{"foo": "baz", "n": 2}',
    )
    assert 0 == result.exit_code, result.output
    # Should get an error with incorrect shaped additional data
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "from_json_nl", "-", "--nl"],
        input='{"foo": "bar", "baz": 5}',
    )
    assert 0 != result.exit_code, result.output
    # If we run it again with --alter it should work correctly
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "from_json_nl", "-", "--nl", "--alter"],
        input='{"foo": "bar", "baz": 5}',
    )
    assert 0 == result.exit_code, result.output
    # Sanity check the database itself
    db = Database(db_path)
    assert {"foo": str, "n": int, "baz": int} == db["from_json_nl"].columns_dict
    assert [
        {"foo": "bar", "n": 1, "baz": None},
        {"foo": "baz", "n": 2, "baz": None},
        {"foo": "bar", "baz": 5, "n": None},
    ] == db.execute_returning_dicts("select foo, n, baz from from_json_nl")


def test_query_csv(db_path):
    db = Database(db_path)
    with db.conn:
        db["dogs"].insert_all(
            [
                {"id": 1, "age": 4, "name": "Cleo"},
                {"id": 2, "age": 2, "name": "Pancakes"},
            ]
        )
    result = CliRunner().invoke(
        cli.cli, [db_path, "select id, name, age from dogs", "--csv"]
    )
    assert 0 == result.exit_code
    assert "id,name,age\n1,Cleo,4\n2,Pancakes,2\n" == result.output
    # Test the no-headers option:
    result = CliRunner().invoke(
        cli.cli, [db_path, "select id, name, age from dogs", "--no-headers", "--csv"]
    )
    assert "1,Cleo,4\n2,Pancakes,2\n" == result.output


_all_query = "select id, name, age from dogs"
_one_query = "select id, name, age from dogs where id = 1"


@pytest.mark.parametrize(
    "sql,args,expected",
    [
        (
            _all_query,
            [],
            '[{"id": 1, "name": "Cleo", "age": 4},\n {"id": 2, "name": "Pancakes", "age": 2}]',
        ),
        (
            _all_query,
            ["--nl"],
            '{"id": 1, "name": "Cleo", "age": 4}\n{"id": 2, "name": "Pancakes", "age": 2}',
        ),
        (_all_query, ["--arrays"], '[[1, "Cleo", 4],\n [2, "Pancakes", 2]]'),
        (_all_query, ["--arrays", "--nl"], '[1, "Cleo", 4]\n[2, "Pancakes", 2]'),
        (_one_query, [], '[{"id": 1, "name": "Cleo", "age": 4}]'),
        (_one_query, ["--nl"], '{"id": 1, "name": "Cleo", "age": 4}'),
        (_one_query, ["--arrays"], '[[1, "Cleo", 4]]'),
        (_one_query, ["--arrays", "--nl"], '[1, "Cleo", 4]'),
    ],
)
def test_query_json(db_path, sql, args, expected):
    db = Database(db_path)
    with db.conn:
        db["dogs"].insert_all(
            [
                {"id": 1, "age": 4, "name": "Cleo"},
                {"id": 2, "age": 2, "name": "Pancakes"},
            ]
        )
    result = CliRunner().invoke(cli.cli, [db_path, sql] + args)
    assert expected == result.output.strip()


def test_query_json_with_json_cols(db_path):
    db = Database(db_path)
    with db.conn:
        db["dogs"].insert(
            {
                "id": 1,
                "name": "Cleo",
                "friends": [{"name": "Pancakes"}, {"name": "Bailey"}],
            }
        )
    result = CliRunner().invoke(
        cli.cli, [db_path, "select id, name, friends from dogs"]
    )
    assert (
        r"""
    [{"id": 1, "name": "Cleo", "friends": "[{\"name\": \"Pancakes\"}, {\"name\": \"Bailey\"}]"}]
    """.strip()
        == result.output.strip()
    )
    # With --json-cols:
    result = CliRunner().invoke(
        cli.cli, [db_path, "select id, name, friends from dogs", "--json-cols"]
    )
    expected = r"""
    [{"id": 1, "name": "Cleo", "friends": [{"name": "Pancakes"}, {"name": "Bailey"}]}]
    """.strip()
    assert expected == result.output.strip()
    # Test rows command too
    result_rows = CliRunner().invoke(cli.cli, ["rows", db_path, "dogs", "--json-cols"])
    assert expected == result_rows.output.strip()


def test_query_memory_does_not_create_file(tmpdir):
    owd = os.getcwd()
    try:
        os.chdir(tmpdir)
        # This should create a foo.db file
        CliRunner().invoke(cli.cli, ["foo.db", "select sqlite_version()"])
        # This should NOT create a file
        result = CliRunner().invoke(cli.cli, [":memory:", "select sqlite_version()"])
        assert ["sqlite_version()"] == list(json.loads(result.output)[0].keys())
    finally:
        os.chdir(owd)
    assert ["foo.db"] == os.listdir(tmpdir)


@pytest.mark.parametrize(
    "args,expected",
    [
        (
            [],
            '[{"id": 1, "name": "Cleo", "age": 4},\n {"id": 2, "name": "Pancakes", "age": 2}]',
        ),
        (
            ["--nl"],
            '{"id": 1, "name": "Cleo", "age": 4}\n{"id": 2, "name": "Pancakes", "age": 2}',
        ),
        (["--arrays"], '[[1, "Cleo", 4],\n [2, "Pancakes", 2]]'),
        (["--arrays", "--nl"], '[1, "Cleo", 4]\n[2, "Pancakes", 2]'),
    ],
)
def test_rows(db_path, args, expected):
    db = Database(db_path)
    with db.conn:
        db["dogs"].insert_all(
            [
                {"id": 1, "age": 4, "name": "Cleo"},
                {"id": 2, "age": 2, "name": "Pancakes"},
            ],
            column_order=("id", "name", "age"),
        )
    result = CliRunner().invoke(cli.cli, ["rows", db_path, "dogs"] + args)
    assert expected == result.output.strip()
