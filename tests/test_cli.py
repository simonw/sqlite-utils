from sqlite_utils import cli, Database
from sqlite_utils.db import Index, ForeignKey
from click.testing import CliRunner
from pathlib import Path
import subprocess
import sys
from unittest import mock
import json
import os
import pytest
import textwrap

from .utils import collapse_whitespace


def write_json(file_path, data):
    with open(file_path, "w") as fp:
        json.dump(data, fp)


def _supports_pragma_function_list():
    db = Database(memory=True)
    try:
        db.execute("select * from pragma_function_list()")
    except Exception:
        return False
    return True


def _has_compiled_ext():
    for ext in ["dylib", "so", "dll"]:
        path = Path(__file__).parent / f"ext.{ext}"
        if path.is_file():
            return True
    return False


COMPILED_EXTENSION_PATH = str(Path(__file__).parent / "ext")


@pytest.mark.parametrize(
    "options",
    (
        ["-h"],
        ["--help"],
        ["insert", "-h"],
        ["insert", "--help"],
    ),
)
def test_help(options):
    result = CliRunner().invoke(cli.cli, options)
    assert result.exit_code == 0
    assert result.output.startswith("Usage: ")
    assert "-h, --help" in result.output


def test_tables(db_path):
    result = CliRunner().invoke(cli.cli, ["tables", db_path], catch_exceptions=False)
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


@pytest.mark.parametrize(
    "format,expected",
    [
        (
            "--csv",
            (
                "table,count,columns\n"
                'Gosh,0,"c1\n'
                "c2\n"
                'c3"\n'
                'Gosh2,0,"c1\n'
                "c2\n"
                'c3"\n'
                'lots,30,"id\n'
                'age"'
            ),
        ),
        (
            "--tsv",
            "table\tcount\tcolumns\nGosh\t0\t['c1', 'c2', 'c3']\nGosh2\t0\t['c1', 'c2', 'c3']\nlots\t30\t['id', 'age']",
        ),
    ],
)
def test_tables_counts_and_columns_csv(db_path, format, expected):
    db = Database(db_path)
    with db.conn:
        db["lots"].insert_all([{"id": i, "age": i + 1} for i in range(30)])
    result = CliRunner().invoke(
        cli.cli, ["tables", "--counts", "--columns", format, db_path]
    )
    assert result.output.strip().replace("\r", "") == expected


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
    "options,expected",
    [
        (
            ["--fmt", "simple"],
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
            ["-t"],
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
            ["--fmt", "rst"],
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
def test_output_table(db_path, options, expected):
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
    result = CliRunner().invoke(cli.cli, ["rows", db_path, "rows"] + options)
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
    # ... unless we use --if-not-exists or --ignore
    for option in ("--if-not-exists", "--ignore"):
        assert (
            CliRunner().invoke(cli.cli, create_index_unique_args + [option]).exit_code
            == 0
        )


def test_create_index_analyze(db_path):
    db = Database(db_path)
    assert "sqlite_stat1" not in db.table_names()
    assert [] == db["Gosh"].indexes
    result = CliRunner().invoke(
        cli.cli, ["create-index", db_path, "Gosh", "c1", "--analyze"]
    )
    assert result.exit_code == 0
    assert "sqlite_stat1" in db.table_names()


def test_create_index_desc(db_path):
    db = Database(db_path)
    assert [] == db["Gosh"].indexes
    result = CliRunner().invoke(cli.cli, ["create-index", db_path, "Gosh", "--", "-c1"])
    assert result.exit_code == 0
    assert (
        db.execute("select sql from sqlite_master where type='index'").fetchone()[0]
        == "CREATE INDEX [idx_Gosh_c1]\n    ON [Gosh] ([c1] desc)"
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


@pytest.mark.parametrize("ignore", (True, False))
def test_add_column_ignore(db_path, ignore):
    db = Database(db_path)
    db.create_table("dogs", {"name": str})
    args = ["add-column", db_path, "dogs", "name"] + (["--ignore"] if ignore else [])
    result = CliRunner().invoke(cli.cli, args)
    if ignore:
        assert result.exit_code == 0
    else:
        assert result.exit_code == 1
        assert result.output == "Error: duplicate column name: name\n"


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
        "CREATE TABLE [books] ( [title] TEXT , [author_id] INTEGER, FOREIGN KEY([author_id]) REFERENCES [authors]([id]) )"
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
        "FOREIGN KEY([author_id]) REFERENCES [authors]([id]), "
        "FOREIGN KEY([author_name_ref]) REFERENCES [authors]([name]) )"
        == collapse_whitespace(db["books"].schema)
    )
    # Throw an error if the --fk table does not exist
    result = CliRunner().invoke(
        cli.cli, ["add-column", db_path, "books", "author_id", "--fk", "bobcats"]
    )
    assert 0 != result.exit_code
    assert "table 'bobcats' does not exist" in str(result.exception)


def test_suggest_alter_if_column_missing(db_path):
    db = Database(db_path)
    db["authors"].insert({"id": 1, "name": "Sally"}, pk="id")
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "authors", "-"],
        input='{"id": 2, "name": "Barry", "age": 43}',
    )
    assert result.exit_code != 0
    assert result.output.strip() == (
        "Error: table authors has no column named age\n\n"
        "Try using --alter to add additional columns"
    )


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
    assert db["Gosh"].detect_fts() is None
    result = CliRunner().invoke(
        cli.cli, ["enable-fts", db_path, "Gosh", "c1", "--fts4"]
    )
    assert 0 == result.exit_code
    assert "Gosh_fts" == db["Gosh"].detect_fts()

    # Table names with restricted chars are handled correctly.
    # colons and dots are restricted characters for table names.
    db["http://example.com"].create({"c1": str, "c2": str, "c3": str})
    assert db["http://example.com"].detect_fts() is None
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


def test_enable_fts_replace(db_path):
    db = Database(db_path)
    assert db["Gosh"].detect_fts() is None
    result = CliRunner().invoke(
        cli.cli, ["enable-fts", db_path, "Gosh", "c1", "--fts4"]
    )
    assert result.exit_code == 0
    assert "Gosh_fts" == db["Gosh"].detect_fts()
    assert db["Gosh_fts"].columns_dict == {"c1": str}

    # This should throw an error
    result2 = CliRunner().invoke(
        cli.cli, ["enable-fts", db_path, "Gosh", "c1", "--fts4"]
    )
    assert result2.exit_code == 1
    assert result2.output == "Error: table [Gosh_fts] already exists\n"

    # This should work
    result3 = CliRunner().invoke(
        cli.cli, ["enable-fts", db_path, "Gosh", "c2", "--fts4", "--replace"]
    )
    assert result3.exit_code == 0
    assert db["Gosh_fts"].columns_dict == {"c2": str}


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


def test_dump(db_path):
    result = CliRunner().invoke(cli.cli, ["dump", db_path])
    assert result.exit_code == 0
    assert result.output.startswith("BEGIN TRANSACTION;")
    assert result.output.strip().endswith("COMMIT;")


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
    # Weirdest thing: tests started failing because size after
    # ended up larger than size before in some cases. I think
    # it's OK to tolerate that happening, though it's very strange.
    assert size_after_optimize <= (size_before_optimize + 10000)
    # Soundness check that --no-vacuum doesn't throw errors:
    result = CliRunner().invoke(cli.cli, ["optimize", "--no-vacuum", db_path])
    assert 0 == result.exit_code


def test_rebuild_fts_fixes_docsize_error(db_path):
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
        db["fts5_table"].insert_all(records, pk="c1")
        db["fts5_table"].enable_fts(
            ["c1", "c2", "c3"], fts_version="FTS5", create_triggers=True
        )
    # Search should work
    assert list(db["fts5_table"].search("verb1"))
    # Replicate docsize error from this issue for FTS5
    # https://github.com/simonw/sqlite-utils/issues/149
    assert db["fts5_table_fts_docsize"].count == 10000
    db["fts5_table"].insert_all(records, replace=True)
    assert db["fts5_table"].count == 10000
    assert db["fts5_table_fts_docsize"].count == 20000
    # Running rebuild-fts should fix this
    result = CliRunner().invoke(cli.cli, ["rebuild-fts", db_path, "fts5_table"])
    assert 0 == result.exit_code
    assert db["fts5_table_fts_docsize"].count == 10000


@pytest.mark.parametrize(
    "format,expected",
    [
        ("--csv", "id,name,age\n1,Cleo,4\n2,Pancakes,2\n"),
        ("--tsv", "id\tname\tage\n1\tCleo\t4\n2\tPancakes\t2\n"),
    ],
)
def test_query_csv(db_path, format, expected):
    db = Database(db_path)
    with db.conn:
        db["dogs"].insert_all(
            [
                {"id": 1, "age": 4, "name": "Cleo"},
                {"id": 2, "age": 2, "name": "Pancakes"},
            ]
        )
    result = CliRunner().invoke(
        cli.cli, [db_path, "select id, name, age from dogs", format]
    )
    assert 0 == result.exit_code
    assert result.output.replace("\r", "") == expected
    # Test the no-headers option:
    result = CliRunner().invoke(
        cli.cli, [db_path, "select id, name, age from dogs", "--no-headers", format]
    )
    expected_rest = "\n".join(expected.split("\n")[1:]).strip()
    assert result.output.strip().replace("\r", "") == expected_rest


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
        (
            "select id, dog(age) from dogs",
            ["--functions", "def dog(i):\n  return i * 7"],
            '[{"id": 1, "dog(age)": 28},\n {"id": 2, "dog(age)": 14}]',
        ),
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


def test_query_json_empty(db_path):
    result = CliRunner().invoke(
        cli.cli,
        [db_path, "select * from sqlite_master where 0"],
    )
    assert result.output.strip() == "[]"


def test_query_invalid_function(db_path):
    result = CliRunner().invoke(
        cli.cli, [db_path, "select bad()", "--functions", "def invalid_python"]
    )
    assert result.exit_code == 1
    assert result.output.startswith("Error: Error in functions definition:")


TEST_FUNCTIONS = """
def zero():
    return 0

def one(a):
    return a

def _two(a, b):
    return a + b

def two(a, b):
    return _two(a, b)
"""


def test_query_complex_function(db_path):
    result = CliRunner().invoke(
        cli.cli,
        [
            db_path,
            "select zero(), one(1), two(1, 2)",
            "--functions",
            TEST_FUNCTIONS,
        ],
    )
    assert result.exit_code == 0
    assert json.loads(result.output.strip()) == [
        {"zero()": 0, "one(1)": 1, "two(1, 2)": 3}
    ]


@pytest.mark.skipif(
    not _supports_pragma_function_list(),
    reason="Needs SQLite version that supports pragma_function_list()",
)
def test_hidden_functions_are_hidden(db_path):
    result = CliRunner().invoke(
        cli.cli,
        [
            db_path,
            "select name from pragma_function_list()",
            "--functions",
            TEST_FUNCTIONS,
        ],
    )
    assert result.exit_code == 0
    functions = {r["name"] for r in json.loads(result.output.strip())}
    assert "zero" in functions
    assert "one" in functions
    assert "two" in functions
    assert "_two" not in functions


LOREM_IPSUM_COMPRESSED = (
    b"x\x9c\xed\xd1\xcdq\x03!\x0c\x05\xe0\xbb\xabP\x01\x1eW\x91\xdc|M\x01\n\xc8\x8e"
    b"f\xf83H\x1e\x97\x1f\x91M\x8e\xe9\xe0\xdd\x96\x05\x84\xf4\xbek\x9fRI\xc7\xf2J"
    b"\xb9\x97>i\xa9\x11W\xb13\xa5\xde\x96$\x13\xf3I\x9cu\xe8J\xda\xee$EcsI\x8e\x0b"
    b"$\xea\xab\xf6L&u\xc4emI\xb3foFnT\xf83\xca\x93\xd8QZ\xa8\xf2\xbd1q\xd1\x87\xf3"
    b"\x85>\x8c\xa4i\x8d\xdaTu\x7f<c\xc9\xf5L\x0f\xd7E\xad/\x9b\x9eI^2\x93\x1a\x9b"
    b"\xf6F^\n\xd7\xd4\x8f\xca\xfb\x90.\xdd/\xfd\x94\xd4\x11\x87I8\x1a\xaf\xd1S?\x06"
    b"\x88\xa7\xecBo\xbb$\xbb\t\xe9\xf4\xe8\xe4\x98U\x1bM\x19S\xbe\xa4e\x991x\xfc"
    b"x\xf6\xe2#\x9e\x93h'&%YK(i)\x7f\t\xc5@N7\xbf+\x1b\xb5\xdd\x10\r\x9e\xb1\xf0"
    b"y\xa1\xf7W\x92a\xe2;\xc6\xc8\xa0\xa7\xc4\x92\xe2\\\xf2\xa1\x99m\xdf\x88)\xc6"
    b"\xec\x9a\xa5\xed\x14wR\xf1h\xf22x\xcfM\xfdv\xd3\xa4LY\x96\xcc\xbd[{\xd9m\xf0"
    b"\x0eH#\x8e\xf5\x9b\xab\xd7\xcb\xe9t\x05\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03"
    b"\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03"
    b"\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03"
    b"\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\x03"
    b"\x1f\xf8\xc0\x07>\xf0\x81\x0f|\xe0\xfb\x8f\xef\x1b\x9b\x06\x83}"
)


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
                "encoded": (
                    (
                        "eJzt0c1xAyEMBeC7q1ABHleR3HxNAQrIjmb4M0gelx+RTY7p4N2WBYT0vmufUknH"
                        "8kq5lz5pqRFXsTOl3pYkE/NJnHXoStruJEVjc0mOCyTqq/ZMJnXEZW1Js2ZvRm5U+"
                        "DPKk9hRWqjyvTFx0YfzhT6MpGmN2lR1fzxjyfVMD9dFrS+bnkleMpMam/ZGXgrX1I"
                        "/K+5Au3S/9lNQRh0k4Gq/RUz8GiKfsQm+7JLsJ6fTo5JhVG00ZU76kZZkxePx49uI"
                        "jnpNoJyYlWUsoaSl/CcVATje/Kxu13RANnrHweaH3V5Jh4jvGyKCnxJLiXPKhmW3f"
                        "iCnG7Jql7RR3UvFo8jJ4z039dtOkTFmWzL1be9lt8A5II471m6vXy+l0BR/4wAc+8"
                        "IEPfOADH/jABz7wgQ984AMf+MAHPvCBD3zgAx/4wAc+8IEPfOADH/jABz7wgQ984A"
                        "Mf+MAHPvCBD3zgAx/4wAc+8IEPfOADH/jABz7wgQ984PuP7xubBoN9"
                    )
                ),
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


@pytest.mark.parametrize(
    "content,is_binary",
    [(b"\x00\x0Fbinary", True), ("this is text", False), (1, False), (1.5, False)],
)
def test_query_raw_lines(db_path, content, is_binary):
    Database(db_path)["files"].insert_all({"content": content} for _ in range(3))
    result = CliRunner().invoke(
        cli.cli, [db_path, "select content from files", "--raw-lines"]
    )
    if is_binary:
        assert result.stdout_bytes == b"\n".join(content for _ in range(3)) + b"\n"
    else:
        assert result.output == "\n".join(str(content) for _ in range(3)) + "\n"


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
        (
            ["--nl", "-c", "age", "-c", "name"],
            '{"age": 4, "name": "Cleo"}\n{"age": 2, "name": "Pancakes"}',
        ),
        # --limit and --offset
        (
            ["-c", "name", "--limit", "1"],
            '[{"name": "Cleo"}]',
        ),
        (
            ["-c", "name", "--limit", "1", "--offset", "1"],
            '[{"name": "Pancakes"}]',
        ),
        # --where
        (
            ["-c", "name", "--where", "id = 1"],
            '[{"name": "Cleo"}]',
        ),
        (
            ["-c", "name", "--where", "id = :id", "-p", "id", "1"],
            '[{"name": "Cleo"}]',
        ),
        (
            ["-c", "name", "--where", "id = :id", "--param", "id", "1"],
            '[{"name": "Cleo"}]',
        ),
        # --order
        (
            ["-c", "id", "--order", "id desc", "--limit", "1"],
            '[{"id": 2}]',
        ),
        (
            ["-c", "id", "--order", "id", "--limit", "1"],
            '[{"id": 1}]',
        ),
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
    write_json(json_path, insert_dogs)
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
    write_json(json_path, upsert_dogs)
    result = CliRunner().invoke(
        cli.cli,
        ["upsert", db_path, "dogs", json_path, "--pk", "id"],
        catch_exceptions=False,
    )
    assert 0 == result.exit_code, result.output
    assert list(db.query("select * from dogs order by id")) == [
        {"id": 1, "name": "Cleo", "age": 5},
        {"id": 2, "name": "Nixie", "age": 5},
    ]


def test_upsert_pk_required(db_path, tmpdir):
    json_path = str(tmpdir / "dogs.json")
    insert_dogs = [
        {"id": 1, "name": "Cleo", "age": 4},
        {"id": 2, "name": "Nixie", "age": 4},
    ]
    write_json(json_path, insert_dogs)
    result = CliRunner().invoke(
        cli.cli,
        ["upsert", db_path, "dogs", json_path],
        catch_exceptions=False,
    )
    assert result.exit_code == 2
    assert "Error: Missing option '--pk'" in result.output


def test_upsert_analyze(db_path, tmpdir):
    db = Database(db_path)
    db["rows"].insert({"id": 1, "foo": "x", "n": 3}, pk="id")
    db["rows"].create_index(["n"])
    assert "sqlite_stat1" not in db.table_names()
    result = CliRunner().invoke(
        cli.cli,
        ["upsert", db_path, "rows", "-", "--nl", "--analyze", "--pk", "id"],
        input='{"id": 2, "foo": "bar", "n": 1}',
    )
    assert 0 == result.exit_code, result.output
    assert "sqlite_stat1" in db.table_names()


def test_upsert_flatten(tmpdir):
    db_path = str(tmpdir / "flat.db")
    db = Database(db_path)
    db["upsert_me"].insert({"id": 1, "name": "Example"}, pk="id")
    result = CliRunner().invoke(
        cli.cli,
        ["upsert", db_path, "upsert_me", "-", "--flatten", "--pk", "id", "--alter"],
        input=json.dumps({"id": 1, "nested": {"two": 2}}),
    )
    assert result.exit_code == 0
    assert list(db.query("select * from upsert_me")) == [
        {"id": 1, "name": "Example", "nested_two": 2}
    ]


def test_upsert_alter(db_path, tmpdir):
    json_path = str(tmpdir / "dogs.json")
    db = Database(db_path)
    insert_dogs = [{"id": 1, "name": "Cleo"}]
    write_json(json_path, insert_dogs)
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id"]
    )
    assert 0 == result.exit_code, result.output
    # Should fail with error code if no --alter
    upsert_dogs = [{"id": 1, "age": 5}]
    write_json(json_path, upsert_dogs)
    result = CliRunner().invoke(
        cli.cli, ["upsert", db_path, "dogs", json_path, "--pk", "id"]
    )
    assert 1 == result.exit_code
    assert (
        "Error: no such column: age\n\n"
        "sql = UPDATE [dogs] SET [age] = ? WHERE [id] = ?\n"
        "parameters = [5, 1]"
    ) == result.output.strip()
    # Should succeed with --alter
    result = CliRunner().invoke(
        cli.cli, ["upsert", db_path, "dogs", json_path, "--pk", "id", "--alter"]
    )
    assert 0 == result.exit_code
    assert [
        {"id": 1, "name": "Cleo", "age": 5},
    ] == list(db.query("select * from dogs order by id"))


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
        # Using --ignore suppresses that error
        result = runner.invoke(
            cli.cli,
            ["drop-table", "test.db", "t2", "--ignore"],
        )
        assert 0 == result.exit_code


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
        # Using --ignore suppresses that error
        result = runner.invoke(
            cli.cli,
            ["drop-view", "test.db", "t2", "--ignore"],
        )
        assert 0 == result.exit_code


def test_enable_wal():
    runner = CliRunner()
    dbs = ["test.db", "test2.db"]
    with runner.isolated_filesystem():
        for dbname in dbs:
            db = Database(dbname)
            db["t"].create({"pk": int}, pk="pk")
            assert db.journal_mode == "delete"
        result = runner.invoke(cli.cli, ["enable-wal"] + dbs, catch_exceptions=False)
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
    assert list(db.query("select * from dogs")) == [
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
            (
                'CREATE TABLE "dogs" (\n'
                "   [id] INTEGER PRIMARY KEY,\n"
                "   [age] INTEGER NOT NULL DEFAULT '1',\n"
                "   [name] TEXT\n"
                ")"
            ),
        ),
        (
            ["--type", "age", "text"],
            (
                'CREATE TABLE "dogs" (\n'
                "   [id] INTEGER PRIMARY KEY,\n"
                "   [age] TEXT NOT NULL DEFAULT '1',\n"
                "   [name] TEXT\n"
                ")"
            ),
        ),
        (
            ["--drop", "age"],
            (
                'CREATE TABLE "dogs" (\n'
                "   [id] INTEGER PRIMARY KEY,\n"
                "   [name] TEXT\n"
                ")"
            ),
        ),
        (
            ["--rename", "age", "age2", "--rename", "id", "pk"],
            (
                'CREATE TABLE "dogs" (\n'
                "   [pk] INTEGER PRIMARY KEY,\n"
                "   [age2] INTEGER NOT NULL DEFAULT '1',\n"
                "   [name] TEXT\n"
                ")"
            ),
        ),
        (
            ["--not-null", "name"],
            (
                'CREATE TABLE "dogs" (\n'
                "   [id] INTEGER PRIMARY KEY,\n"
                "   [age] INTEGER NOT NULL DEFAULT '1',\n"
                "   [name] TEXT NOT NULL\n"
                ")"
            ),
        ),
        (
            ["--not-null-false", "age"],
            (
                'CREATE TABLE "dogs" (\n'
                "   [id] INTEGER PRIMARY KEY,\n"
                "   [age] INTEGER DEFAULT '1',\n"
                "   [name] TEXT\n"
                ")"
            ),
        ),
        (
            ["--pk", "name"],
            (
                'CREATE TABLE "dogs" (\n'
                "   [id] INTEGER,\n"
                "   [age] INTEGER NOT NULL DEFAULT '1',\n"
                "   [name] TEXT PRIMARY KEY\n"
                ")"
            ),
        ),
        (
            ["--pk-none"],
            (
                'CREATE TABLE "dogs" (\n'
                "   [id] INTEGER,\n"
                "   [age] INTEGER NOT NULL DEFAULT '1',\n"
                "   [name] TEXT\n"
                ")"
            ),
        ),
        (
            ["--default", "name", "Turnip"],
            (
                'CREATE TABLE "dogs" (\n'
                "   [id] INTEGER PRIMARY KEY,\n"
                "   [age] INTEGER NOT NULL DEFAULT '1',\n"
                "   [name] TEXT DEFAULT 'Turnip'\n"
                ")"
            ),
        ),
        (
            ["--default-none", "age"],
            (
                'CREATE TABLE "dogs" (\n'
                "   [id] INTEGER PRIMARY KEY,\n"
                "   [age] INTEGER NOT NULL,\n"
                "   [name] TEXT\n"
                ")"
            ),
        ),
        (
            ["-o", "name", "--column-order", "age", "-o", "id"],
            (
                'CREATE TABLE "dogs" (\n'
                "   [name] TEXT,\n"
                "   [age] INTEGER NOT NULL DEFAULT '1',\n"
                "   [id] INTEGER PRIMARY KEY\n"
                ")"
            ),
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
    assert schema == (
        'CREATE TABLE "places" (\n'
        "   [id] INTEGER PRIMARY KEY,\n"
        "   [name] TEXT,\n"
        "   [country] INTEGER,\n"
        "   [city] INTEGER REFERENCES [city]([id])\n"
        ")"
    )


_common_other_schema = (
    "CREATE TABLE [species] (\n   [id] INTEGER PRIMARY KEY,\n   [species] TEXT\n)"
)


@pytest.mark.parametrize(
    "args,expected_table_schema,expected_other_schema",
    [
        (
            [],
            (
                'CREATE TABLE "trees" (\n'
                "   [id] INTEGER PRIMARY KEY,\n"
                "   [address] TEXT,\n"
                "   [species_id] INTEGER,\n"
                "   FOREIGN KEY([species_id]) REFERENCES [species]([id])\n"
                ")"
            ),
            _common_other_schema,
        ),
        (
            ["--table", "custom_table"],
            (
                'CREATE TABLE "trees" (\n'
                "   [id] INTEGER PRIMARY KEY,\n"
                "   [address] TEXT,\n"
                "   [custom_table_id] INTEGER,\n"
                "   FOREIGN KEY([custom_table_id]) REFERENCES [custom_table]([id])\n"
                ")"
            ),
            "CREATE TABLE [custom_table] (\n   [id] INTEGER PRIMARY KEY,\n   [species] TEXT\n)",
        ),
        (
            ["--fk-column", "custom_fk"],
            (
                'CREATE TABLE "trees" (\n'
                "   [id] INTEGER PRIMARY KEY,\n"
                "   [address] TEXT,\n"
                "   [custom_fk] INTEGER,\n"
                "   FOREIGN KEY([custom_fk]) REFERENCES [species]([id])\n"
                ")"
            ),
            _common_other_schema,
        ),
        (
            ["--rename", "name", "name2"],
            'CREATE TABLE "trees" (\n'
            "   [id] INTEGER PRIMARY KEY,\n"
            "   [address] TEXT,\n"
            "   [species_id] INTEGER,\n"
            "   FOREIGN KEY([species_id]) REFERENCES [species]([id])\n"
            ")",
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
    with open(csv_path, "wb") as fp:
        fp.write(latin1_csv)
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


@pytest.mark.parametrize("fts", ["FTS4", "FTS5"])
@pytest.mark.parametrize(
    "extra_arg,expected",
    [
        (
            None,
            '[{"rowid": 2, "id": 2, "title": "Title the second"}]\n',
        ),
        ("--csv", "rowid,id,title\n2,2,Title the second\n"),
    ],
)
def test_search(tmpdir, fts, extra_arg, expected):
    db_path = str(tmpdir / "test.db")
    db = Database(db_path)
    db["articles"].insert_all(
        [
            {"id": 1, "title": "Title the first"},
            {"id": 2, "title": "Title the second"},
            {"id": 3, "title": "Title the third"},
        ],
        pk="id",
    )
    db["articles"].enable_fts(["title"], fts_version=fts)
    result = CliRunner().invoke(
        cli.cli,
        ["search", db_path, "articles", "second"] + ([extra_arg] if extra_arg else []),
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.output.replace("\r", "") == expected


def test_search_quote(tmpdir):
    db_path = str(tmpdir / "test.db")
    db = Database(db_path)
    db["creatures"].insert({"name": "dog."}).enable_fts(["name"])
    # Without --quote should return an error
    error_result = CliRunner().invoke(cli.cli, ["search", db_path, "creatures", 'dog"'])
    assert error_result.exit_code == 1
    assert error_result.output == (
        "Error: unterminated string\n\n"
        "Try running this again with the --quote option\n"
    )
    # With --quote it should work
    result = CliRunner().invoke(
        cli.cli, ["search", db_path, "creatures", 'dog"', "--quote"]
    )
    assert result.exit_code == 0
    assert result.output.strip() == '[{"rowid": 1, "name": "dog."}]'


def test_indexes(tmpdir):
    db_path = str(tmpdir / "test.db")
    db = Database(db_path)
    db.conn.executescript(
        """
        create table Gosh (c1 text, c2 text, c3 text);
        create index Gosh_idx on Gosh(c2, c3 desc);
    """
    )
    result = CliRunner().invoke(
        cli.cli,
        ["indexes", str(db_path)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert json.loads(result.output) == [
        {
            "table": "Gosh",
            "index_name": "Gosh_idx",
            "seqno": 0,
            "cid": 1,
            "name": "c2",
            "desc": 0,
            "coll": "BINARY",
            "key": 1,
        },
        {
            "table": "Gosh",
            "index_name": "Gosh_idx",
            "seqno": 1,
            "cid": 2,
            "name": "c3",
            "desc": 1,
            "coll": "BINARY",
            "key": 1,
        },
    ]
    result2 = CliRunner().invoke(
        cli.cli,
        ["indexes", str(db_path), "--aux"],
        catch_exceptions=False,
    )
    assert result2.exit_code == 0
    assert json.loads(result2.output) == [
        {
            "table": "Gosh",
            "index_name": "Gosh_idx",
            "seqno": 0,
            "cid": 1,
            "name": "c2",
            "desc": 0,
            "coll": "BINARY",
            "key": 1,
        },
        {
            "table": "Gosh",
            "index_name": "Gosh_idx",
            "seqno": 1,
            "cid": 2,
            "name": "c3",
            "desc": 1,
            "coll": "BINARY",
            "key": 1,
        },
        {
            "table": "Gosh",
            "index_name": "Gosh_idx",
            "seqno": 2,
            "cid": -1,
            "name": None,
            "desc": 0,
            "coll": "BINARY",
            "key": 0,
        },
    ]


_TRIGGERS_EXPECTED = (
    '[{"name": "blah", "table": "articles", "sql": "CREATE TRIGGER blah '
    'AFTER INSERT ON articles\\nBEGIN\\n    UPDATE counter SET count = count + 1;\\nEND"}]\n'
)


@pytest.mark.parametrize(
    "extra_args,expected",
    [
        ([], _TRIGGERS_EXPECTED),
        (["articles"], _TRIGGERS_EXPECTED),
        (["counter"], "[]\n"),
    ],
)
def test_triggers(tmpdir, extra_args, expected):
    db_path = str(tmpdir / "test.db")
    db = Database(db_path)
    db["articles"].insert(
        {"id": 1, "title": "Title the first"},
        pk="id",
    )
    db["counter"].insert({"count": 1})
    db.conn.execute(
        textwrap.dedent(
            """
        CREATE TRIGGER blah AFTER INSERT ON articles
        BEGIN
            UPDATE counter SET count = count + 1;
        END
    """
        )
    )
    args = ["triggers", db_path]
    if extra_args:
        args.extend(extra_args)
    result = CliRunner().invoke(
        cli.cli,
        args,
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.output == expected


@pytest.mark.parametrize(
    "options,expected",
    (
        (
            [],
            (
                "CREATE TABLE [dogs] (\n"
                "   [id] INTEGER,\n"
                "   [name] TEXT\n"
                ");\n"
                "CREATE TABLE [chickens] (\n"
                "   [id] INTEGER,\n"
                "   [name] TEXT,\n"
                "   [breed] TEXT\n"
                ");\n"
                "CREATE INDEX [idx_chickens_breed]\n"
                "    ON [chickens] ([breed]);\n"
            ),
        ),
        (
            ["dogs"],
            ("CREATE TABLE [dogs] (\n" "   [id] INTEGER,\n" "   [name] TEXT\n" ")\n"),
        ),
        (
            ["chickens", "dogs"],
            (
                "CREATE TABLE [chickens] (\n"
                "   [id] INTEGER,\n"
                "   [name] TEXT,\n"
                "   [breed] TEXT\n"
                ")\n"
                "CREATE TABLE [dogs] (\n"
                "   [id] INTEGER,\n"
                "   [name] TEXT\n"
                ")\n"
            ),
        ),
    ),
)
def test_schema(tmpdir, options, expected):
    db_path = str(tmpdir / "test.db")
    db = Database(db_path)
    db["dogs"].create({"id": int, "name": str})
    db["chickens"].create({"id": int, "name": str, "breed": str})
    db["chickens"].create_index(["breed"])
    result = CliRunner().invoke(
        cli.cli,
        ["schema", db_path] + options,
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.output == expected


def test_long_csv_column_value(tmpdir):
    db_path = str(tmpdir / "test.db")
    csv_path = str(tmpdir / "test.csv")
    csv_file = open(csv_path, "w")
    long_string = "a" * 131073
    csv_file.write("id,text\n")
    csv_file.write("1,{}\n".format(long_string))
    csv_file.close()
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "bigtable", csv_path, "--csv"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    db = Database(db_path)
    rows = list(db["bigtable"].rows)
    assert len(rows) == 1
    assert rows[0]["text"] == long_string


@pytest.mark.parametrize(
    "args,tsv",
    (
        (["--csv", "--no-headers"], False),
        (["--no-headers"], False),
        (["--tsv", "--no-headers"], True),
    ),
)
def test_import_no_headers(tmpdir, args, tsv):
    db_path = str(tmpdir / "test.db")
    csv_path = str(tmpdir / "test.csv")
    csv_file = open(csv_path, "w")
    sep = "\t" if tsv else ","
    csv_file.write("Cleo{sep}Dog{sep}5\n".format(sep=sep))
    csv_file.write("Tracy{sep}Spider{sep}7\n".format(sep=sep))
    csv_file.close()
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "creatures", csv_path] + args,
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    db = Database(db_path)
    schema = db["creatures"].schema
    assert schema == (
        "CREATE TABLE [creatures] (\n"
        "   [untitled_1] TEXT,\n"
        "   [untitled_2] TEXT,\n"
        "   [untitled_3] TEXT\n"
        ")"
    )
    rows = list(db["creatures"].rows)
    assert rows == [
        {"untitled_1": "Cleo", "untitled_2": "Dog", "untitled_3": "5"},
        {"untitled_1": "Tracy", "untitled_2": "Spider", "untitled_3": "7"},
    ]


def test_attach(tmpdir):
    foo_path = str(tmpdir / "foo.db")
    bar_path = str(tmpdir / "bar.db")
    db = Database(foo_path)
    with db.conn:
        db["foo"].insert({"id": 1, "text": "foo"})
    db2 = Database(bar_path)
    with db2.conn:
        db2["bar"].insert({"id": 1, "text": "bar"})
    db.attach("bar", bar_path)
    sql = "select * from foo union all select * from bar.bar"
    result = CliRunner().invoke(
        cli.cli,
        [foo_path, "--attach", "bar", bar_path, sql],
        catch_exceptions=False,
    )
    assert json.loads(result.output) == [
        {"id": 1, "text": "foo"},
        {"id": 1, "text": "bar"},
    ]


def test_csv_insert_bom(tmpdir):
    db_path = str(tmpdir / "test.db")
    bom_csv_path = str(tmpdir / "bom.csv")
    with open(bom_csv_path, "wb") as fp:
        fp.write(b"\xef\xbb\xbfname,age\nCleo,5")
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "broken", bom_csv_path, "--encoding", "utf-8", "--csv"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    result2 = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "fixed", bom_csv_path, "--csv"],
        catch_exceptions=False,
    )
    assert result2.exit_code == 0
    db = Database(db_path)
    tables = db.execute("select name, sql from sqlite_master").fetchall()
    assert tables == [
        ("broken", "CREATE TABLE [broken] (\n   [\ufeffname] TEXT,\n   [age] TEXT\n)"),
        ("fixed", "CREATE TABLE [fixed] (\n   [name] TEXT,\n   [age] TEXT\n)"),
    ]


@pytest.mark.parametrize("option_or_env_var", (None, "-d", "--detect-types"))
def test_insert_detect_types(tmpdir, option_or_env_var):
    db_path = str(tmpdir / "test.db")
    data = "name,age,weight\nCleo,6,45.5\nDori,1,3.5"
    extra = []
    if option_or_env_var:
        extra = [option_or_env_var]

    def _test():
        result = CliRunner().invoke(
            cli.cli,
            ["insert", db_path, "creatures", "-", "--csv"] + extra,
            catch_exceptions=False,
            input=data,
        )
        assert result.exit_code == 0
        db = Database(db_path)
        assert list(db["creatures"].rows) == [
            {"name": "Cleo", "age": 6, "weight": 45.5},
            {"name": "Dori", "age": 1, "weight": 3.5},
        ]

    if option_or_env_var is None:
        # Use environment variable instead of option
        with mock.patch.dict(os.environ, {"SQLITE_UTILS_DETECT_TYPES": "1"}):
            _test()
    else:
        _test()


@pytest.mark.parametrize("option", ("-d", "--detect-types"))
def test_upsert_detect_types(tmpdir, option):
    db_path = str(tmpdir / "test.db")
    data = "id,name,age,weight\n1,Cleo,6,45.5\n2,Dori,1,3.5"
    result = CliRunner().invoke(
        cli.cli,
        ["upsert", db_path, "creatures", "-", "--csv", "--pk", "id"] + [option],
        catch_exceptions=False,
        input=data,
    )
    assert result.exit_code == 0
    db = Database(db_path)
    assert list(db["creatures"].rows) == [
        {"id": 1, "name": "Cleo", "age": 6, "weight": 45.5},
        {"id": 2, "name": "Dori", "age": 1, "weight": 3.5},
    ]


def test_integer_overflow_error(tmpdir):
    db_path = str(tmpdir / "test.db")
    result = CliRunner().invoke(
        cli.cli,
        ["insert", db_path, "items", "-"],
        input=json.dumps({"bignumber": 34223049823094832094802398430298048240}),
    )
    assert result.exit_code == 1
    assert result.output == (
        "Error: Python int too large to convert to SQLite INTEGER\n\n"
        "sql = INSERT INTO [items] ([bignumber]) VALUES (?);\n"
        "parameters = [34223049823094832094802398430298048240]\n"
    )


def test_python_dash_m():
    "Tool can be run using python -m sqlite_utils"
    result = subprocess.run(
        [sys.executable, "-m", "sqlite_utils", "--help"], stdout=subprocess.PIPE
    )
    assert result.returncode == 0
    assert b"Commands for interacting with a SQLite database" in result.stdout


@pytest.mark.parametrize("enable_wal", (False, True))
def test_create_database(tmpdir, enable_wal):
    db_path = tmpdir / "test.db"
    assert not db_path.exists()
    args = ["create-database", str(db_path)]
    if enable_wal:
        args.append("--enable-wal")
    result = CliRunner().invoke(cli.cli, args)
    assert result.exit_code == 0, result.output
    assert db_path.exists()
    assert db_path.read_binary()[:16] == b"SQLite format 3\x00"
    db = Database(str(db_path))
    if enable_wal:
        assert db.journal_mode == "wal"
    else:
        assert db.journal_mode == "delete"


@pytest.mark.parametrize(
    "options,expected",
    (
        (
            [],
            [
                {"tbl": "two_indexes", "idx": "idx_two_indexes_species", "stat": "1 1"},
                {"tbl": "two_indexes", "idx": "idx_two_indexes_name", "stat": "1 1"},
                {"tbl": "one_index", "idx": "idx_one_index_name", "stat": "1 1"},
            ],
        ),
        (
            ["one_index"],
            [
                {"tbl": "one_index", "idx": "idx_one_index_name", "stat": "1 1"},
            ],
        ),
        (
            ["idx_two_indexes_name"],
            [
                {"tbl": "two_indexes", "idx": "idx_two_indexes_name", "stat": "1 1"},
            ],
        ),
    ),
)
def test_analyze(tmpdir, options, expected):
    db_path = str(tmpdir / "test.db")
    db = Database(db_path)
    db["one_index"].insert({"id": 1, "name": "Cleo"}, pk="id")
    db["one_index"].create_index(["name"])
    db["two_indexes"].insert({"id": 1, "name": "Cleo", "species": "dog"}, pk="id")
    db["two_indexes"].create_index(["name"])
    db["two_indexes"].create_index(["species"])
    result = CliRunner().invoke(cli.cli, ["analyze", db_path] + options)
    assert result.exit_code == 0
    assert list(db["sqlite_stat1"].rows) == expected


def test_rename_table(tmpdir):
    db_path = str(tmpdir / "test.db")
    db = Database(db_path)
    db["one"].insert({"id": 1, "name": "Cleo"}, pk="id")
    # First try a non-existent table
    result_error = CliRunner().invoke(
        cli.cli,
        ["rename-table", db_path, "missing", "two"],
        catch_exceptions=False,
    )
    assert result_error.exit_code == 1
    assert result_error.output == (
        'Error: Table "missing" could not be renamed. ' "no such table: missing\n"
    )
    # And check --ignore works
    result_error2 = CliRunner().invoke(
        cli.cli,
        ["rename-table", db_path, "missing", "two", "--ignore"],
        catch_exceptions=False,
    )
    assert result_error2.exit_code == 0
    previous_columns = db["one"].columns_dict
    # Now try for a table that exists
    result = CliRunner().invoke(
        cli.cli,
        ["rename-table", db_path, "one", "two"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert db["two"].columns_dict == previous_columns


def test_duplicate_table(tmpdir):
    db_path = str(tmpdir / "test.db")
    db = Database(db_path)
    db["one"].insert({"id": 1, "name": "Cleo"}, pk="id")
    # First try a non-existent table
    result_error = CliRunner().invoke(
        cli.cli,
        ["duplicate", db_path, "missing", "two"],
        catch_exceptions=False,
    )
    assert result_error.exit_code == 1
    assert result_error.output == 'Error: Table "missing" does not exist\n'
    # And check --ignore works
    result_error2 = CliRunner().invoke(
        cli.cli,
        ["duplicate", db_path, "missing", "two", "--ignore"],
        catch_exceptions=False,
    )
    assert result_error2.exit_code == 0
    # Now try for a table that exists
    result = CliRunner().invoke(
        cli.cli,
        ["duplicate", db_path, "one", "two"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert db["one"].columns_dict == db["two"].columns_dict
    assert list(db["one"].rows) == list(db["two"].rows)


@pytest.mark.skipif(not _has_compiled_ext(), reason="Requires compiled ext.c")
@pytest.mark.parametrize(
    "entrypoint,should_pass,should_fail",
    (
        (None, ("a",), ("b", "c")),
        ("sqlite3_ext_b_init", ("b"), ("a", "c")),
        ("sqlite3_ext_c_init", ("c"), ("a", "b")),
    ),
)
def test_load_extension(entrypoint, should_pass, should_fail):
    ext = COMPILED_EXTENSION_PATH
    if entrypoint:
        ext += ":" + entrypoint
    for func in should_pass:
        result = CliRunner().invoke(
            cli.cli,
            ["memory", "select {}()".format(func), "--load-extension", ext],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
    for func in should_fail:
        result = CliRunner().invoke(
            cli.cli,
            ["memory", "select {}()".format(func), "--load-extension", ext],
            catch_exceptions=False,
        )
        assert result.exit_code == 1
