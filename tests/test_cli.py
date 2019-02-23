from sqlite_utils import cli, Database
from click.testing import CliRunner
import json
import os
import pytest
import sqlite3


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
    assert "Gosh\nGosh2" == result.output.strip()


def test_tables_fts4(db_path):
    Database(db_path)["Gosh"].enable_fts(["c2"], fts_version="FTS4")
    result = CliRunner().invoke(cli.cli, ["tables", "--fts4", db_path])
    assert "Gosh_fts" == result.output.strip()


def test_tables_fts5(db_path):
    Database(db_path)["Gosh"].enable_fts(["c2"], fts_version="FTS5")
    result = CliRunner().invoke(cli.cli, ["tables", "--fts5", db_path])
    assert "Gosh_fts" == result.output.strip()


def test_enable_fts(db_path):
    assert None == Database(db_path)["Gosh"].detect_fts()
    result = CliRunner().invoke(
        cli.cli, ["enable-fts", db_path, "Gosh", "c1", "--fts4"]
    )
    assert 0 == result.exit_code
    assert "Gosh_fts" == Database(db_path)["Gosh"].detect_fts()


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
