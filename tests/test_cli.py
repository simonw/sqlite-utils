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


def test_table_names(db_path):
    result = CliRunner().invoke(cli.cli, ["table_names", db_path])
    assert "Gosh\nGosh2" == result.output.strip()


def test_table_names_fts4(db_path):
    Database(db_path)["Gosh"].enable_fts(["c2"], fts_version="FTS4")
    result = CliRunner().invoke(cli.cli, ["table_names", "--fts4", db_path])
    assert "Gosh_fts" == result.output.strip()


def test_table_names_fts5(db_path):
    Database(db_path)["Gosh"].enable_fts(["c2"], fts_version="FTS5")
    result = CliRunner().invoke(cli.cli, ["table_names", "--fts5", db_path])
    assert "Gosh_fts" == result.output.strip()


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
    open("/tmp/dogs.json", "w").write(json.dumps(dogs, indent=4))
    result = CliRunner().invoke(
        cli.cli, ["insert", db_path, "dogs", json_path, "--pk", "id"]
    )
    assert 0 == result.exit_code
    assert dogs == Database(db_path).execute_returning_dicts(
        "select * from dogs order by id"
    )
    db = Database(db_path)
    assert ["id"] == db["dogs"].pks
