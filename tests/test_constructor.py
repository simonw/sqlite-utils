from sqlite_utils import Database
from sqlite_utils.utils import sqlite3
import pytest


def test_recursive_triggers():
    db = Database(memory=True)
    assert db.execute("PRAGMA recursive_triggers").fetchone()[0]


def test_recursive_triggers_off():
    db = Database(memory=True, recursive_triggers=False)
    assert not db.execute("PRAGMA recursive_triggers").fetchone()[0]


def test_memory_name():
    db1 = Database(memory_name="shared")
    db2 = Database(memory_name="shared")
    db1["dogs"].insert({"name": "Cleo"})
    assert list(db2["dogs"].rows) == [{"name": "Cleo"}]


def test_sqlite_version():
    db = Database(memory=True)
    version = db.sqlite_version
    assert isinstance(version, tuple)
    as_string = ".".join(map(str, version))
    actual = next(db.query("select sqlite_version() as v"))["v"]
    assert actual == as_string


def test_database_context_manager(tmpdir):
    path = str(tmpdir / "test.db")
    with Database(path) as db:
        db["t"].insert({"id": 1})
        # A raw write left uncommitted on purpose:
        db.execute("insert into t (id) values (2)")
    # The connection is closed...
    with pytest.raises(sqlite3.ProgrammingError):
        db.execute("select 1")
    # ... and the uncommitted change was rolled back, not committed
    db2 = Database(path)
    assert [r["id"] for r in db2["t"].rows] == [1]
    db2.close()


@pytest.mark.parametrize("memory", [True, False])
def test_database_close(tmpdir, memory):
    if memory:
        db = Database(memory=True)
    else:
        db = Database(str(tmpdir / "test.db"))
    assert db.execute("select 1 + 1").fetchone()[0] == 2
    db.close()
    with pytest.raises(sqlite3.ProgrammingError):
        db.execute("select 1 + 1")
