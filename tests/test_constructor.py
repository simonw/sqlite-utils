from sqlite_utils import Database
from sqlite_utils.db import TransactionError
from sqlite_utils.utils import sqlite3
import pytest
import sys


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
        # Raw writes commit automatically too
        db.execute("insert into t (id) values (2)")
        # An explicitly opened transaction left uncommitted on purpose:
        db.begin()
        db.execute("insert into t (id) values (3)")
    # The connection is closed...
    with pytest.raises(sqlite3.ProgrammingError):
        db.execute("select 1")
    # ... and the open explicit transaction was rolled back, not committed
    db2 = Database(path)
    assert [r["id"] for r in db2["t"].rows] == [1, 2]
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


@pytest.mark.skipif(
    sys.version_info < (3, 12),
    reason="sqlite3.connect(autocommit=) requires Python 3.12",
)
def test_autocommit_false_connections_are_rejected(tmpdir):
    # autocommit=False keeps an implicit transaction open at all times,
    # which breaks the explicit transaction handling used by every write
    # method, so the constructor refuses these connections
    conn = sqlite3.connect(str(tmpdir / "test.db"), autocommit=False)
    with pytest.raises(TransactionError):
        Database(conn)
    conn.close()


@pytest.mark.skipif(
    sys.version_info < (3, 12),
    reason="sqlite3.connect(autocommit=) requires Python 3.12",
)
def test_autocommit_true_connection_writes_persist(tmpdir):
    path = str(tmpdir / "test.db")
    conn = sqlite3.connect(path, autocommit=True)
    db = Database(conn)
    db["t"].insert({"id": 1}, pk="id")
    db.execute("insert into t (id) values (2)")
    db.close()
    # The writes survived closing the connection
    db2 = Database(path)
    assert [r["id"] for r in db2["t"].rows] == [1, 2]
    db2.close()


@pytest.mark.skipif(
    sys.version_info < (3, 12),
    reason="sqlite3.connect(autocommit=) requires Python 3.12",
)
def test_autocommit_true_connection_transactions(tmpdir):
    conn = sqlite3.connect(str(tmpdir / "test.db"), autocommit=True)
    db = Database(conn)
    db["t"].insert({"id": 1}, pk="id")
    # atomic() rolls back on error
    with pytest.raises(ZeroDivisionError):
        with db.atomic():
            db["t"].insert({"id": 2}, pk="id")
            1 / 0
    assert [r["id"] for r in db["t"].rows] == [1]
    # atomic() commits on success, including a nested block
    with db.atomic():
        db["t"].insert({"id": 3}, pk="id")
        with db.atomic():
            db["t"].insert({"id": 4}, pk="id")
    assert [r["id"] for r in db["t"].rows] == [1, 3, 4]
    # begin()/rollback() work too
    db.begin()
    db["t"].insert({"id": 5}, pk="id")
    db.rollback()
    assert [r["id"] for r in db["t"].rows] == [1, 3, 4]
    db.close()


@pytest.mark.skipif(
    sys.version_info < (3, 12),
    reason="sqlite3.connect(autocommit=) requires Python 3.12",
)
def test_autocommit_true_connection_wal(tmpdir):
    conn = sqlite3.connect(str(tmpdir / "test.db"), autocommit=True)
    db = Database(conn)
    db.enable_wal()
    assert db.journal_mode == "wal"
    db.disable_wal()
    assert db.journal_mode == "delete"
    db.close()


@pytest.mark.skipif(
    sys.version_info < (3, 12),
    reason="sqlite3.LEGACY_TRANSACTION_CONTROL requires Python 3.12",
)
def test_legacy_transaction_control_connection_is_accepted(tmpdir):
    conn = sqlite3.connect(
        str(tmpdir / "test.db"), autocommit=sqlite3.LEGACY_TRANSACTION_CONTROL
    )
    db = Database(conn)
    db["t"].insert({"id": 1}, pk="id")
    assert [r["id"] for r in db["t"].rows] == [1]
    db.close()


def test_memory_attribute_for_memory_true():
    db = Database(memory=True)
    assert db.memory is True
    assert db.memory_name is None


def test_memory_attribute_for_memory_name():
    db = Database(memory_name="shared_attr")
    assert db.memory is True
    assert db.memory_name == "shared_attr"


def test_memory_attribute_for_memory_string_path():
    db = Database(":memory:")
    assert db.memory is True
    assert db.memory_name is None


def test_memory_attribute_for_file_path(tmpdir):
    db = Database(str(tmpdir / "file.db"))
    assert db.memory is False
    assert db.memory_name is None


def test_memory_attribute_for_existing_connection():
    conn = sqlite3.connect(":memory:")
    db = Database(conn)
    assert db.memory is False
    assert db.memory_name is None
