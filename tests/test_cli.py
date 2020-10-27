from sqlite_utils import cli, Database
from sqlite_utils.db import Index, ForeignKey
from click.testing import CliRunner
import json
import os
import pytest
from sqlite_utils.utils import sqlite3, find_spatialite

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


def test_views(db_path):
    Database(db_path).create_view("hello", "select sqlite_version()")
    result = CliRunner().invoke(cli.cli, ["views", db_path, "--table", "--schema"])
    assert (
        "view    schema\n"
        "------  --------------------------------------------\n"
        "hello   CREATE VIEW hello AS select sqlite_version()"
    ) == result.output.strip()


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


def test_tables_schema(db_path):
    db = Database(db_path)
    with db.conn:
        db["lots"].insert_all([{"id": i, "age": i + 1} for i in range(30)])
    result = CliRunner().invoke(cli.cli, ["tables", "--schema", db_path])
    assert (
        '[{"table": "Gosh", "schema": "CREATE TABLE Gosh (c1 text, c2 text, c3 text)"},\n'
        ' {"table": "Gosh2", "schema": "CREATE TABLE Gosh2 (c1 text, c2 text, c3 text)"},\n'
        ' {"table": "lots", "schema": "CREATE TABLE [lots] (\\n   [id] INTEGER,\\n   [age] INTEGER\\n)"}]'
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

    # No error if we add it twice with --ignore
    result = CliRunner().invoke(
        cli.cli,
        ["add-foreign-key", db_path, "books", "author_id", "authors", "id", "--ignore"],
    )
    assert 0 == result.exit_code

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
    db = Database(db_path)
    assert None == db["Gosh"].detect_fts()
    result = CliRunner().invoke(
        cli.cli, ["enable-fts", db_path, "Gosh", "c1", "--fts4"]
    )
    assert 0 == result.exit_code
    assert "Gosh_fts" == db["Gosh"].detect_fts()

    # Table names with restricted chars are handled correctly.
    # colons and dots are restricted characters for table names.
    db["http://example.com"].create({"c1": str, "c2": str, "c3": str})
    assert None == db["http://example.com"].detect_fts()
    result = CliRunner().invoke(
        cli.cli,
        [
            "enable-fts",
            db_path,
            "http://example.com",
            "c1",
            "--fts4",
            "--tokenize",
            "porter",
        ],
    )
    assert 0 == result.exit_code
    assert "http://example.com_fts" == db["http://example.com"].detect_fts()
    # Check tokenize was set to porter
    assert (
        "CREATE VIRTUAL TABLE [http://example.com_fts] USING FTS4 (\n"
        "    [c1],\n"
        "    tokenize='porter',\n"
        "    content=[http://example.com]"
        "\n)"
    ) == db["http://example.com_fts"].schema
    db["http://example.com"].drop()


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
            .execute("select c1 from Gosh_fts where c1 match ?", [q])
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
            .execute("select c1 from Gosh_fts where c1 match ?", [q])
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


def test_disable_fts(db_path):
    db = Database(db_path)
    assert {"Gosh", "Gosh2"} == set(db.table_names())
    db["Gosh"].enable_fts(["c1"], create_triggers=True)
    assert {
        "Gosh_fts",
        "Gosh_fts_idx",
        "Gosh_fts_data",
        "Gosh2",
        "Gosh_fts_config",
        "Gosh",
        "Gosh_fts_docsize",
    } == set(db.table_names())
    exit_code = CliRunner().invoke(cli.cli, ["disable-fts", db_path, "Gosh"]).exit_code
    assert 0 == exit_code
    assert {"Gosh", "Gosh2"} == set(db.table_names())


def test_vacuum(db_path):
    result = CliRunner().invoke(cli.cli, ["vacuum", db_path])
    assert 0 == result.exit_code


@pytest.mark.parametrize("tables", ([], ["Gosh"], ["Gosh2"]))
def test_optimize(db_path, tables):
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
    result = CliRunner().invoke(cli.cli, ["optimize", db_path] + tables)
    assert 0 == result.exit_code
    size_after_optimize = os.stat(db_path).st_size
    assert size_after_optimize < size_before_optimize
    # Soundness check that --no-vacuum doesn't throw errors:
    result = CliRunner().invoke(cli.cli, ["optimize", "--no-vacuum", db_path])
    assert 0 == result.exit_code


@pytest.mark.parametrize("tables", ([], ["fts4_table"], ["fts5_table"]))
def test_rebuild_fts(db_path, tables):
    db = Database(db_path, recursive_triggers=False)
    records = [
        {
            "c1": "verb{}".format(i),
            "c2": "noun{}".format(i),
            "c3": "adjective{}".format(i),
        }
        for i in range(10000)
    ]
    with db.conn:
        db["fts4_table"].insert_all(records, pk="c1")
        db["fts5_table"].insert_all(records, pk="c1")
        db["fts4_table"].enable_fts(
            ["c1", "c2", "c3"], fts_version="FTS4", create_triggers=True
        )
        db["fts5_table"].enable_fts(
            ["c1", "c2", "c3"], fts_version="FTS5", create_triggers=True
        )
    # Search should work
    assert db["fts4_table"].search("verb1")
    assert db["fts5_table"].search("verb1")
    # Deleting _fts_segments to break FTS4
    with db.conn:
        db["fts4_table_fts_segments"].delete_where()
    # Now this should error:
    with pytest.raises(sqlite3.DatabaseError):
        db["fts4_table"].search("verb1")
    # Replicate docsize error from this issue for FTS5
    # https://github.com/simonw/sqlite-utils/issues/149
    assert db["fts5_table_fts_docsize"].count == 10000
    db["fts5_table"].insert_all(records, replace=True)
    assert db["fts5_table"].count == 10000
    assert db["fts5_table_fts_docsize"].count == 20000
    # Running rebuild-fts should fix both issues
    print(["rebuild-fts", db_path] + tables)
    result = CliRunner().invoke(cli.cli, ["rebuild-fts", db_path] + tables)
    assert 0 == result.exit_code
    fixed_tables = tables or ["fts4_table", "fts5_table"]
    if "fts4_table" in fixed_tables:
        assert db["fts4_table"].search("verb1")
    else:
        with pytest.raises(sqlite3.DatabaseError):
            db["fts4_table"].search("verb1")
    if "fts5_table" in fixed_tables:
        assert db["fts5_table_fts_docsize"].count == 10000
    else:
        assert db["fts5_table_fts_docsize"].count == 20000


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


def test_insert_from_stdin(tmpdir):
    db_path = str(tmpdir / "dogs.db")
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "dogs", "-"],
        input=json.dumps({"name": "Cleo", "age": 4}),
    )
    assert 0 == result.exit_code
    assert [{"age": 4, "name": "Cleo"}] == Database(db_path).execute_returning_dicts(
        "select * from dogs"
    )


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


def test_insert_binary_base64(db_path):
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "files", "-"],
        input=r'{"content": {"$base64": true, "encoded": "aGVsbG8="}}',
    )
    assert 0 == result.exit_code, result.output
    db = Database(db_path)
    actual = db.execute_returning_dicts("select content from files")
    assert actual == [{"content": b"hello"}]


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
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "data", file_path, option], catch_exceptions=False
    )
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


def test_insert_replace(db_path, tmpdir):
    test_insert_multiple_with_primary_key(db_path, tmpdir)
    json_path = str(tmpdir / "insert-replace.json")
    db = Database(db_path)
    assert 20 == db["dogs"].count
    insert_replace_dogs = [
        {"id": 1, "name": "Insert replaced 1", "age": 4},
        {"id": 2, "name": "Insert replaced 2", "age": 4},
        {"id": 21, "name": "Fresh insert 21", "age": 6},
    ]
    open(json_path, "w").write(json.dumps(insert_replace_dogs))
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id", "--replace"]
    )
    assert 0 == result.exit_code, result.output
    assert 21 == db["dogs"].count
    assert insert_replace_dogs == db.execute_returning_dicts(
        "select * from dogs where id in (1, 2, 21) order by id"
    )


def test_insert_truncate(db_path):
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "from_json_nl", "-", "--nl", "--batch-size=1"],
        input='{"foo": "bar", "n": 1}\n{"foo": "baz", "n": 2}',
    )
    assert 0 == result.exit_code, result.output
    db = Database(db_path)
    assert [
        {"foo": "bar", "n": 1},
        {"foo": "baz", "n": 2},
    ] == db.execute_returning_dicts("select foo, n from from_json_nl")
    # Truncate and insert new rows
    result = CliRunner().invoke(
        cli.cli,
        [
            "insert",
            db_path,
            "from_json_nl",
            "-",
            "--nl",
            "--truncate",
            "--batch-size=1",
        ],
        input='{"foo": "bam", "n": 3}\n{"foo": "bat", "n": 4}',
    )
    assert 0 == result.exit_code, result.output
    assert [
        {"foo": "bam", "n": 3},
        {"foo": "bat", "n": 4},
    ] == db.execute_returning_dicts("select foo, n from from_json_nl")


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
    # Soundness check the database itself
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


LOREM_IPSUM_COMPRESSED = b"x\x9c\xed\xd1\xcdq\x03!\x0c\x05\xe0\xbb\xabP\x01\x1eW\x91\xdc|M\x01\n\xc8\x8ef\xf83H\x1e\x97\x1f\x91M\x8e\xe9\xe0\xdd\x96\x05\x84\xf4\xbek\x9fRI\xc7\xf2J\xb9\x97>i\xa9\x11W\xb13\xa5\xde\x96$\x13\xf3I\x9cu\xe8J\xda\xee$EcsI\x8e\x0b$\xea\xab\xf6L&u\xc4emI\xb3foFnT\xf83\xca\x93\xd8QZ\xa8\xf2\xbd1q\xd1\x87\xf3\x85>\x8c\xa4i\x8d\xdaTu\x7f<c\xc9\xf5L\x0f\xd7E\xad/\x9b\x9eI^2\x93\x1a\x9b\xf6F^\n\xd7\xd4\x8f\xca\xfb\x90.\xdd/\xfd\x94\xd4\x11\x87I8\x1a\xaf\xd1S?\x06\x88\xa7\xecBo\xbb$\xbb\t\xe9\xf4\xe8\xe4\x98U\x1bM\x19S\xbe\xa4e\x991x\xfcx\xf6\xe2#\x9e\x93h'&%YK(i)\x7f\t\xc5@N7\xbf+\x1b\xb5\xdd\x10\r\x9e\xb1\xf0y\xa1\xf7W\x92a\xe2;\xc6\xc8\xa0\xa7\xc4\x92\xe2\\\xf2\xa1\x99m\xdf\x88)\xc6\xec\x9a\xa5\xed\x14wR\xf1h\xf22x\xcfM\xfdv\xd3\xa4LY\x96\xcc\xbd[{\xd9m\xf0\x0eH#\x8e\xf5\x9b\xab\xd7\xcb\xe9t\x05\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\xfb\x8f\xef\x1b\x9b\x06\x83}"


def test_query_json_binary(db_path):
    db = Database(db_path)
    with db.conn:
        db["files"].insert(
            {
                "name": "lorem.txt",
                "sz": 16984,
                "data": LOREM_IPSUM_COMPRESSED,
            },
            pk="name",
        )
    result = CliRunner().invoke(cli.cli, [db_path, "select name, sz, data from files"])
    assert result.exit_code == 0, str(result)
    assert json.loads(result.output.strip()) == [
        {
            "name": "lorem.txt",
            "sz": 16984,
            "data": {
                "$base64": True,
                "encoded": "eJzt0c1xAyEMBeC7q1ABHleR3HxNAQrIjmb4M0gelx+RTY7p4N2WBYT0vmufUknH8kq5lz5pqRFXsTOl3pYkE/NJnHXoStruJEVjc0mOCyTqq/ZMJnXEZW1Js2ZvRm5U+DPKk9hRWqjyvTFx0YfzhT6MpGmN2lR1fzxjyfVMD9dFrS+bnkleMpMam/ZGXgrX1I/K+5Au3S/9lNQRh0k4Gq/RUz8GiKfsQm+7JLsJ6fTo5JhVG00ZU76kZZkxePx49uIjnpNoJyYlWUsoaSl/CcVATje/Kxu13RANnrHweaH3V5Jh4jvGyKCnxJLiXPKhmW3fiCnG7Jql7RR3UvFo8jJ4z039dtOkTFmWzL1be9lt8A5II471m6vXy+l0BR/4wAc+8IEPfOADH/jABz7wgQ984AMf+MAHPvCBD3zgAx/4wAc+8IEPfOADH/jABz7wgQ984AMf+MAHPvCBD3zgAx/4wAc+8IEPfOADH/jABz7wgQ984PuP7xubBoN9",
            },
        }
    ]


@pytest.mark.parametrize(
    "sql,params,expected",
    [
        ("select 1 + 1 as out", {"p": "2"}, 2),
        ("select 1 + :p as out", {"p": "2"}, 3),
        (
            "select :hello as out",
            {"hello": """This"has'many'quote"s"""},
            """This"has'many'quote"s""",
        ),
    ],
)
def test_query_params(db_path, sql, params, expected):
    extra_args = []
    for key, value in params.items():
        extra_args.extend(["-p", key, value])
    result = CliRunner().invoke(cli.cli, [db_path, sql] + extra_args)
    assert result.exit_code == 0, str(result)
    assert json.loads(result.output.strip()) == [{"out": expected}]


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


@pytest.mark.parametrize(
    "content,is_binary",
    [(b"\x00\x0Fbinary", True), ("this is text", False), (1, False), (1.5, False)],
)
def test_query_raw(db_path, content, is_binary):
    Database(db_path)["files"].insert({"content": content})
    result = CliRunner().invoke(
        cli.cli, [db_path, "select content from files", "--raw"]
    )
    if is_binary:
        assert result.stdout_bytes == content
    else:
        assert result.output == str(content)


@pytest.mark.skipif(not find_spatialite(), reason="Could not find SpatiaLite extension")
@pytest.mark.skipif(
    not hasattr(sqlite3.Connection, "enable_load_extension"),
    reason="sqlite3.Connection missing enable_load_extension",
)
@pytest.mark.parametrize("use_spatialite_shortcut", [True, False])
def test_query_load_extension(use_spatialite_shortcut):
    # Without --load-extension:
    result = CliRunner().invoke(cli.cli, [":memory:", "select spatialite_version()"])
    assert result.exit_code == 1
    assert "no such function: spatialite_version" in repr(result)
    # With --load-extension:
    if use_spatialite_shortcut:
        load_extension = "spatialite"
    else:
        load_extension = find_spatialite()
    result = CliRunner().invoke(
        cli.cli,
        [
            ":memory:",
            "select spatialite_version()",
            "--load-extension={}".format(load_extension),
        ],
    )
    assert result.exit_code == 0, result.stdout
    assert ["spatialite_version()"] == list(json.loads(result.output)[0].keys())


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


def test_upsert(db_path, tmpdir):
    json_path = str(tmpdir / "dogs.json")
    db = Database(db_path)
    insert_dogs = [
        {"id": 1, "name": "Cleo", "age": 4},
        {"id": 2, "name": "Nixie", "age": 4},
    ]
    open(json_path, "w").write(json.dumps(insert_dogs))
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "dogs", json_path, "--pk", "id"],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, result.output
    assert 2 == db["dogs"].count
    # Now run the upsert to update just their ages
    upsert_dogs = [
        {"id": 1, "age": 5},
        {"id": 2, "age": 5},
    ]
    open(json_path, "w").write(json.dumps(insert_dogs))
    result = CliRunner().invoke(
        cli.cli,
        ["upsert", db_path, "dogs", json_path, "--pk", "id"],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, result.output
    assert [
        {"id": 1, "name": "Cleo", "age": 4},
        {"id": 2, "name": "Nixie", "age": 4},
    ] == db.execute_returning_dicts("select * from dogs order by id")


def test_upsert_alter(db_path, tmpdir):
    json_path = str(tmpdir / "dogs.json")
    db = Database(db_path)
    insert_dogs = [{"id": 1, "name": "Cleo"}]
    open(json_path, "w").write(json.dumps(insert_dogs))
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id"]
    )
    assert 0 == result.exit_code, result.output
    # Should fail with error code if no --alter
    upsert_dogs = [{"id": 1, "age": 5}]
    open(json_path, "w").write(json.dumps(upsert_dogs))
    result = CliRunner().invoke(
        cli.cli, ["upsert", db_path, "dogs", json_path, "--pk", "id"]
    )
    assert 1 == result.exit_code
    assert "no such column: age" == str(result.exception)
    # Should succeed with --alter
    result = CliRunner().invoke(
        cli.cli, ["upsert", db_path, "dogs", json_path, "--pk", "id", "--alter"]
    )
    assert 0 == result.exit_code
    assert [
        {"id": 1, "name": "Cleo", "age": 5},
    ] == db.execute_returning_dicts("select * from dogs order by id")


@pytest.mark.parametrize(
    "args,schema",
    [
        # No primary key
        (
            [
                "name",
                "text",
                "age",
                "integer",
            ],
            ("CREATE TABLE [t] (\n   [name] TEXT,\n   [age] INTEGER\n)"),
        ),
        # All types:
        (
            [
                "id",
                "integer",
                "name",
                "text",
                "age",
                "integer",
                "weight",
                "float",
                "thumbnail",
                "blob",
                "--pk",
                "id",
            ],
            (
                "CREATE TABLE [t] (\n"
                "   [id] INTEGER PRIMARY KEY,\n"
                "   [name] TEXT,\n"
                "   [age] INTEGER,\n"
                "   [weight] FLOAT,\n"
                "   [thumbnail] BLOB\n"
                ")"
            ),
        ),
        # Not null:
        (
            ["name", "text", "--not-null", "name"],
            ("CREATE TABLE [t] (\n" "   [name] TEXT NOT NULL\n" ")"),
        ),
        # Default:
        (
            ["age", "integer", "--default", "age", "3"],
            ("CREATE TABLE [t] (\n" "   [age] INTEGER DEFAULT '3'\n" ")"),
        ),
    ],
)
def test_create_table(args, schema):
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            cli.cli,
            [
                "create-table",
                "test.db",
                "t",
            ]
            + args,
            catch_exceptions=False,
        )
        assert 0 == result.exit_code
        db = Database("test.db")
        assert schema == db["t"].schema


def test_create_table_foreign_key():
    runner = CliRunner()
    creates = (
        ["authors", "id", "integer", "name", "text", "--pk", "id"],
        [
            "books",
            "id",
            "integer",
            "title",
            "text",
            "author_id",
            "integer",
            "--pk",
            "id",
            "--fk",
            "author_id",
            "authors",
            "id",
        ],
    )
    with runner.isolated_filesystem():
        for args in creates:
            result = runner.invoke(
                cli.cli, ["create-table", "books.db"] + args, catch_exceptions=False
            )
            assert 0 == result.exit_code
        db = Database("books.db")
        assert (
            "CREATE TABLE [authors] (\n"
            "   [id] INTEGER PRIMARY KEY,\n"
            "   [name] TEXT\n"
            ")"
        ) == db["authors"].schema
        assert (
            "CREATE TABLE [books] (\n"
            "   [id] INTEGER PRIMARY KEY,\n"
            "   [title] TEXT,\n"
            "   [author_id] INTEGER REFERENCES [authors]([id])\n"
            ")"
        ) == db["books"].schema


def test_create_table_error_if_table_exists():
    runner = CliRunner()
    with runner.isolated_filesystem():
        db = Database("test.db")
        db["dogs"].insert({"name": "Cleo"})
        result = runner.invoke(
            cli.cli, ["create-table", "test.db", "dogs", "id", "integer"]
        )
        assert 1 == result.exit_code
        assert (
            'Error: Table "dogs" already exists. Use --replace to delete and replace it.'
            == result.output.strip()
        )


def test_create_table_ignore():
    runner = CliRunner()
    with runner.isolated_filesystem():
        db = Database("test.db")
        db["dogs"].insert({"name": "Cleo"})
        result = runner.invoke(
            cli.cli, ["create-table", "test.db", "dogs", "id", "integer", "--ignore"]
        )
        assert 0 == result.exit_code
        assert "CREATE TABLE [dogs] (\n   [name] TEXT\n)" == db["dogs"].schema


def test_create_table_replace():
    runner = CliRunner()
    with runner.isolated_filesystem():
        db = Database("test.db")
        db["dogs"].insert({"name": "Cleo"})
        result = runner.invoke(
            cli.cli, ["create-table", "test.db", "dogs", "id", "integer", "--replace"]
        )
        assert 0 == result.exit_code
        assert "CREATE TABLE [dogs] (\n   [id] INTEGER\n)" == db["dogs"].schema


def test_create_view():
    runner = CliRunner()
    with runner.isolated_filesystem():
        db = Database("test.db")
        result = runner.invoke(
            cli.cli, ["create-view", "test.db", "version", "select sqlite_version()"]
        )
        assert 0 == result.exit_code
        assert "CREATE VIEW version AS select sqlite_version()" == db["version"].schema


def test_create_view_error_if_view_exists():
    runner = CliRunner()
    with runner.isolated_filesystem():
        db = Database("test.db")
        db.create_view("version", "select sqlite_version() + 1")
        result = runner.invoke(
            cli.cli, ["create-view", "test.db", "version", "select sqlite_version()"]
        )
        assert 1 == result.exit_code
        assert (
            'Error: View "version" already exists. Use --replace to delete and replace it.'
            == result.output.strip()
        )


def test_create_view_ignore():
    runner = CliRunner()
    with runner.isolated_filesystem():
        db = Database("test.db")
        db.create_view("version", "select sqlite_version() + 1")
        result = runner.invoke(
            cli.cli,
            [
                "create-view",
                "test.db",
                "version",
                "select sqlite_version()",
                "--ignore",
            ],
        )
        assert 0 == result.exit_code
        assert (
            "CREATE VIEW version AS select sqlite_version() + 1" == db["version"].schema
        )


def test_create_view_replace():
    runner = CliRunner()
    with runner.isolated_filesystem():
        db = Database("test.db")
        db.create_view("version", "select sqlite_version() + 1")
        result = runner.invoke(
            cli.cli,
            [
                "create-view",
                "test.db",
                "version",
                "select sqlite_version()",
                "--replace",
            ],
        )
        assert 0 == result.exit_code
        assert "CREATE VIEW version AS select sqlite_version()" == db["version"].schema


def test_drop_table():
    runner = CliRunner()
    with runner.isolated_filesystem():
        db = Database("test.db")
        db["t"].create({"pk": int}, pk="pk")
        assert "t" in db.table_names()
        result = runner.invoke(
            cli.cli,
            [
                "drop-table",
                "test.db",
                "t",
            ],
        )
        assert 0 == result.exit_code
        assert "t" not in db.table_names()


def test_drop_table_error():
    runner = CliRunner()
    with runner.isolated_filesystem():
        db = Database("test.db")
        db["t"].create({"pk": int}, pk="pk")
        result = runner.invoke(
            cli.cli,
            [
                "drop-table",
                "test.db",
                "t2",
            ],
        )
        assert 1 == result.exit_code
        assert 'Error: Table "t2" does not exist' == result.output.strip()


def test_drop_view():
    runner = CliRunner()
    with runner.isolated_filesystem():
        db = Database("test.db")
        db.create_view("hello", "select 1")
        assert "hello" in db.view_names()
        result = runner.invoke(
            cli.cli,
            [
                "drop-view",
                "test.db",
                "hello",
            ],
        )
        assert 0 == result.exit_code
        assert "hello" not in db.view_names()


def test_drop_view_error():
    runner = CliRunner()
    with runner.isolated_filesystem():
        db = Database("test.db")
        db["t"].create({"pk": int}, pk="pk")
        result = runner.invoke(
            cli.cli,
            [
                "drop-view",
                "test.db",
                "t2",
            ],
        )
        assert 1 == result.exit_code
        assert 'Error: View "t2" does not exist' == result.output.strip()


def test_enable_wal():
    runner = CliRunner()
    dbs = ["test.db", "test2.db"]
    with runner.isolated_filesystem():
        for dbname in dbs:
            db = Database(dbname)
            db["t"].create({"pk": int}, pk="pk")
            assert db.journal_mode == "delete"
        result = runner.invoke(cli.cli, ["enable-wal"] + dbs)
        assert 0 == result.exit_code
        for dbname in dbs:
            db = Database(dbname)
            assert db.journal_mode == "wal"


def test_disable_wal():
    runner = CliRunner()
    dbs = ["test.db", "test2.db"]
    with runner.isolated_filesystem():
        for dbname in dbs:
            db = Database(dbname)
            db["t"].create({"pk": int}, pk="pk")
            db.enable_wal()
            assert db.journal_mode == "wal"
        result = runner.invoke(cli.cli, ["disable-wal"] + dbs)
        assert 0 == result.exit_code
        for dbname in dbs:
            db = Database(dbname)
            assert db.journal_mode == "delete"


@pytest.mark.parametrize(
    "args,expected",
    [
        (
            [],
            '[{"rows_affected": 1}]',
        ),
        (["-t"], "rows_affected\n---------------\n              1"),
    ],
)
def test_query_update(db_path, args, expected):
    db = Database(db_path)
    with db.conn:
        db["dogs"].insert_all(
            [
                {"id": 1, "age": 4, "name": "Cleo"},
            ]
        )
    result = CliRunner().invoke(
        cli.cli, [db_path, "update dogs set age = 5 where name = 'Cleo'"] + args
    )
    assert expected == result.output.strip()
    assert db.execute_returning_dicts("select * from dogs") == [
        {"id": 1, "age": 5, "name": "Cleo"},
    ]


def test_add_foreign_keys(db_path):
    db = Database(db_path)
    db["countries"].insert({"id": 7, "name": "Panama"}, pk="id")
    db["authors"].insert({"id": 3, "name": "Matilda", "country_id": 7}, pk="id")
    db["books"].insert({"id": 2, "title": "Wolf anatomy", "author_id": 3}, pk="id")
    assert db["authors"].foreign_keys == []
    assert db["books"].foreign_keys == []
    result = CliRunner().invoke(
        cli.cli,
        [
            "add-foreign-keys",
            db_path,
            "authors",
            "country_id",
            "countries",
            "id",
            "books",
            "author_id",
            "authors",
            "id",
        ],
    )
    assert result.exit_code == 0
    assert db["authors"].foreign_keys == [
        ForeignKey(
            table="authors",
            column="country_id",
            other_table="countries",
            other_column="id",
        )
    ]
    assert db["books"].foreign_keys == [
        ForeignKey(
            table="books", column="author_id", other_table="authors", other_column="id"
        )
    ]


@pytest.mark.parametrize(
    "args,expected_schema",
    [
        (
            [],
            "CREATE TABLE \"dogs\" (\n   [id] INTEGER PRIMARY KEY,\n   [age] INTEGER NOT NULL DEFAULT '1',\n   [name] TEXT\n)",
        ),
        (
            ["--type", "age", "text"],
            "CREATE TABLE \"dogs\" (\n   [id] INTEGER PRIMARY KEY,\n   [age] TEXT NOT NULL DEFAULT '1',\n   [name] TEXT\n)",
        ),
        (
            ["--drop", "age"],
            'CREATE TABLE "dogs" (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT\n)',
        ),
        (
            ["--rename", "age", "age2", "--rename", "id", "pk"],
            "CREATE TABLE \"dogs\" (\n   [pk] INTEGER PRIMARY KEY,\n   [age2] INTEGER NOT NULL DEFAULT '1',\n   [name] TEXT\n)",
        ),
        (
            ["--not-null", "name"],
            "CREATE TABLE \"dogs\" (\n   [id] INTEGER PRIMARY KEY,\n   [age] INTEGER NOT NULL DEFAULT '1',\n   [name] TEXT NOT NULL\n)",
        ),
        (
            ["--not-null-false", "age"],
            "CREATE TABLE \"dogs\" (\n   [id] INTEGER PRIMARY KEY,\n   [age] INTEGER DEFAULT '1',\n   [name] TEXT\n)",
        ),
        (
            ["--pk", "name"],
            "CREATE TABLE \"dogs\" (\n   [id] INTEGER,\n   [age] INTEGER NOT NULL DEFAULT '1',\n   [name] TEXT PRIMARY KEY\n)",
        ),
        (
            ["--pk-none"],
            "CREATE TABLE \"dogs\" (\n   [id] INTEGER,\n   [age] INTEGER NOT NULL DEFAULT '1',\n   [name] TEXT\n)",
        ),
        (
            ["--default", "name", "Turnip"],
            "CREATE TABLE \"dogs\" (\n   [id] INTEGER PRIMARY KEY,\n   [age] INTEGER NOT NULL DEFAULT '1',\n   [name] TEXT DEFAULT 'Turnip'\n)",
        ),
        (
            ["--default-none", "age"],
            'CREATE TABLE "dogs" (\n   [id] INTEGER PRIMARY KEY,\n   [age] INTEGER NOT NULL,\n   [name] TEXT\n)',
        ),
        (
            ["-o", "name", "--column-order", "age", "-o", "id"],
            "CREATE TABLE \"dogs\" (\n   [name] TEXT,\n   [age] INTEGER NOT NULL DEFAULT '1',\n   [id] INTEGER PRIMARY KEY\n)",
        ),
    ],
)
def test_transform(db_path, args, expected_schema):
    db = Database(db_path)
    with db.conn:
        db["dogs"].insert(
            {"id": 1, "age": 4, "name": "Cleo"},
            not_null={"age"},
            defaults={"age": 1},
            pk="id",
        )
    result = CliRunner().invoke(cli.cli, ["transform", db_path, "dogs"] + args)
    print(result.output)
    assert result.exit_code == 0
    schema = db["dogs"].schema
    assert schema == expected_schema


def test_transform_drop_foreign_key(db_path):
    db = Database(db_path)
    with db.conn:
        # Create table with three foreign keys so we can drop two of them
        db["country"].insert({"id": 1, "name": "France"}, pk="id")
        db["city"].insert({"id": 24, "name": "Paris"}, pk="id")
        db["places"].insert(
            {
                "id": 32,
                "name": "Caveau de la Huchette",
                "country": 1,
                "city": 24,
            },
            foreign_keys=("country", "city"),
            pk="id",
        )
    result = CliRunner().invoke(
        cli.cli,
        [
            "transform",
            db_path,
            "places",
            "--drop-foreign-key",
            "country",
        ],
    )
    print(result.output)
    assert result.exit_code == 0
    schema = db["places"].schema
    assert (
        schema
        == 'CREATE TABLE "places" (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [country] INTEGER,\n   [city] INTEGER REFERENCES [city]([id])\n)'
    )


_common_other_schema = (
    "CREATE TABLE [species] (\n   [id] INTEGER PRIMARY KEY,\n   [species] TEXT\n)"
)


@pytest.mark.parametrize(
    "args,expected_table_schema,expected_other_schema",
    [
        (
            [],
            'CREATE TABLE "trees" (\n   [id] INTEGER PRIMARY KEY,\n   [address] TEXT,\n   [species_id] INTEGER,\n   FOREIGN KEY(species_id) REFERENCES species(id)\n)',
            _common_other_schema,
        ),
        (
            ["--table", "custom_table"],
            'CREATE TABLE "trees" (\n   [id] INTEGER PRIMARY KEY,\n   [address] TEXT,\n   [custom_table_id] INTEGER,\n   FOREIGN KEY(custom_table_id) REFERENCES custom_table(id)\n)',
            "CREATE TABLE [custom_table] (\n   [id] INTEGER PRIMARY KEY,\n   [species] TEXT\n)",
        ),
        (
            ["--fk-column", "custom_fk"],
            'CREATE TABLE "trees" (\n   [id] INTEGER PRIMARY KEY,\n   [address] TEXT,\n   [custom_fk] INTEGER,\n   FOREIGN KEY(custom_fk) REFERENCES species(id)\n)',
            _common_other_schema,
        ),
        (
            ["--rename", "name", "name2"],
            'CREATE TABLE "trees" (\n   [id] INTEGER PRIMARY KEY,\n   [address] TEXT,\n   [species_id] INTEGER,\n   FOREIGN KEY(species_id) REFERENCES species(id)\n)',
            "CREATE TABLE [species] (\n   [id] INTEGER PRIMARY KEY,\n   [species] TEXT\n)",
        ),
    ],
)
def test_extract(db_path, args, expected_table_schema, expected_other_schema):
    db = Database(db_path)
    with db.conn:
        db["trees"].insert(
            {"id": 1, "address": "4 Park Ave", "species": "Palm"},
            pk="id",
        )
    result = CliRunner().invoke(
        cli.cli, ["extract", db_path, "trees", "species"] + args
    )
    print(result.output)
    assert result.exit_code == 0
    schema = db["trees"].schema
    assert schema == expected_table_schema
    other_schema = [t for t in db.tables if t.name not in ("trees", "Gosh", "Gosh2")][
        0
    ].schema
    assert other_schema == expected_other_schema


def test_insert_encoding(tmpdir):
    db_path = str(tmpdir / "test.db")
    latin1_csv = (
        b"date,name,latitude,longitude\n"
        b"2020-01-01,Barra da Lagoa,-27.574,-48.422\n"
        b"2020-03-04,S\xe3o Paulo,-23.561,-46.645\n"
        b"2020-04-05,Salta,-24.793:-65.408"
    )
    assert latin1_csv.decode("latin-1").split("\n")[2].split(",")[1] == "São Paulo"
    csv_path = str(tmpdir / "test.csv")
    open(csv_path, "wb").write(latin1_csv)
    # First attempt should error:
    bad_result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "places", csv_path, "--csv"],
        catch_exceptions=False,
    )
    assert bad_result.exit_code == 1
    assert (
        "The input you provided uses a character encoding other than utf-8"
        in bad_result.output
    )
    # Using --encoding=latin-1 should work
    good_result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "places", csv_path, "--encoding", "latin-1", "--csv"],
        catch_exceptions=False,
    )
    assert good_result.exit_code == 0
    db = Database(db_path)
    assert list(db["places"].rows) == [
        {
            "date": "2020-01-01",
            "name": "Barra da Lagoa",
            "latitude": "-27.574",
            "longitude": "-48.422",
        },
        {
            "date": "2020-03-04",
            "name": "São Paulo",
            "latitude": "-23.561",
            "longitude": "-46.645",
        },
        {
            "date": "2020-04-05",
            "name": "Salta",
            "latitude": "-24.793:-65.408",
            "longitude": None,
        },
    ]
