import pytest
import types

from sqlite_utils.utils import sqlite3


def test_query(fresh_db):
    fresh_db["dogs"].insert_all([{"name": "Cleo"}, {"name": "Pancakes"}])
    results = fresh_db.query("select * from dogs order by name desc")
    assert isinstance(results, types.GeneratorType)
    assert list(results) == [{"name": "Pancakes"}, {"name": "Cleo"}]


def test_query_executes_eagerly(fresh_db):
    # The SQL runs when query() is called, not when the result is iterated,
    # so errors are raised at the call site
    with pytest.raises(sqlite3.OperationalError):
        fresh_db.query("select * from missing_table")


def test_query_rejects_statements_that_return_no_rows(fresh_db):
    fresh_db["dogs"].insert({"name": "Cleo"})
    with pytest.raises(ValueError) as ex:
        fresh_db.query("update dogs set name = 'Cleopaws'")
    assert "execute()" in str(ex.value)
    # The rejected update was rolled back, and no transaction is left open
    assert not fresh_db.conn.in_transaction
    assert [row["name"] for row in fresh_db["dogs"].rows] == ["Cleo"]


def test_query_rejected_ddl_is_rolled_back(fresh_db):
    with pytest.raises(ValueError):
        fresh_db.query("create table dogs (id integer primary key)")
    assert not fresh_db.conn.in_transaction
    assert fresh_db.table_names() == []


def test_query_rejected_write_inside_transaction_is_rolled_back(fresh_db):
    fresh_db["dogs"].insert({"name": "Cleo"})
    fresh_db.begin()
    fresh_db.execute("insert into dogs (name) values ('Pancakes')")
    with pytest.raises(ValueError):
        fresh_db.query("update dogs set name = 'Cleopaws'")
    # The transaction is still open and the earlier insert is intact
    assert fresh_db.conn.in_transaction
    fresh_db.commit()
    assert [row["name"] for row in fresh_db["dogs"].rows] == ["Cleo", "Pancakes"]


@pytest.mark.parametrize(
    "sql", ["begin", "commit", "rollback", "vacuum", "detach database foo"]
)
def test_query_rejects_transaction_control_and_vacuum(fresh_db, sql):
    with pytest.raises(ValueError) as ex:
        fresh_db.query(sql)
    assert "execute()" in str(ex.value)
    assert not fresh_db.conn.in_transaction


def test_query_error_leaves_no_transaction_open(fresh_db):
    with pytest.raises(sqlite3.OperationalError):
        fresh_db.query("select * from missing_table")
    assert not fresh_db.conn.in_transaction


def test_query_pragma(tmpdir):
    from sqlite_utils import Database

    db = Database(str(tmpdir / "test.db"))
    # A row-returning PRAGMA works, including one that cannot run in a transaction
    assert list(db.query("pragma journal_mode = wal")) == [{"journal_mode": "wal"}]
    # A PRAGMA that returns no rows raises ValueError
    with pytest.raises(ValueError):
        db.query("pragma user_version = 5")
    db.close()


@pytest.mark.skipif(
    sqlite3.sqlite_version_info < (3, 35, 0),
    reason="RETURNING requires SQLite 3.35.0 or higher",
)
def test_query_insert_returning(fresh_db):
    fresh_db["dogs"].insert({"name": "Cleo"})
    rows = list(
        fresh_db.query("insert into dogs (name) values ('Pancakes') returning name")
    )
    assert rows == [{"name": "Pancakes"}]
    assert fresh_db["dogs"].count == 2


@pytest.mark.skipif(
    sqlite3.sqlite_version_info < (3, 35, 0),
    reason="RETURNING requires SQLite 3.35.0 or higher",
)
def test_query_insert_returning_commits_without_iteration(tmpdir):
    from sqlite_utils import Database

    path = str(tmpdir / "test.db")
    db = Database(path)
    db["dogs"].insert({"name": "Cleo"})
    # Never iterate over the results
    db.query("insert into dogs (name) values ('Pancakes') returning name")
    assert not db.conn.in_transaction
    # A completely separate connection sees the new row straight away
    other = sqlite3.connect(path)
    assert other.execute("select count(*) from dogs").fetchone()[0] == 2
    other.close()
    db.close()


@pytest.mark.skipif(
    sqlite3.sqlite_version_info < (3, 35, 0),
    reason="RETURNING requires SQLite 3.35.0 or higher",
)
def test_query_insert_returning_partial_iteration_still_commits(tmpdir):
    from sqlite_utils import Database

    path = str(tmpdir / "test.db")
    db = Database(path)
    db["dogs"].insert({"name": "Cleo"})
    row = next(
        db.query(
            "insert into dogs (name) values ('Pancakes'), ('Marnie') returning name"
        )
    )
    assert row == {"name": "Pancakes"}
    assert not db.conn.in_transaction
    other = sqlite3.connect(path)
    assert other.execute("select count(*) from dogs").fetchone()[0] == 3
    other.close()
    db.close()


@pytest.mark.skipif(
    sqlite3.sqlite_version_info < (3, 35, 0),
    reason="RETURNING requires SQLite 3.35.0 or higher",
)
def test_query_insert_returning_respects_explicit_transaction(fresh_db):
    fresh_db["dogs"].insert({"name": "Cleo"})
    fresh_db.begin()
    rows = list(
        fresh_db.query("insert into dogs (name) values ('Pancakes') returning name")
    )
    assert rows == [{"name": "Pancakes"}]
    # Still inside the explicit transaction - not committed
    assert fresh_db.conn.in_transaction
    fresh_db.rollback()
    assert [row["name"] for row in fresh_db["dogs"].rows] == ["Cleo"]


def test_execute_returning_dicts(fresh_db):
    # Like db.query() but returns a list, included for backwards compatibility
    # see https://github.com/simonw/sqlite-utils/issues/290
    fresh_db["test"].insert({"id": 1, "bar": 2}, pk="id")
    assert fresh_db.execute_returning_dicts("select * from test") == [
        {"id": 1, "bar": 2}
    ]
