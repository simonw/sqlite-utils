import pytest

from sqlite_utils import Database
from sqlite_utils.utils import sqlite3

BASELINE_ROWS = [(1, "one"), (2, "two")]


def insert(table):
    table.insert({"id": 3, "value": "three"}, pk="id")


def insert_all(table):
    table.insert_all(
        [
            {"id": 3, "value": "three"},
            {"id": 4, "value": "four"},
        ],
        pk="id",
        batch_size=1,
    )


def upsert(table):
    table.upsert({"id": 2, "value": "TWO"}, pk="id")


def upsert_all(table):
    table.upsert_all(
        [
            {"id": 2, "value": "TWO"},
            {"id": 3, "value": "three"},
        ],
        pk="id",
        batch_size=1,
    )


def update(table):
    table.update(2, {"value": "TWO"})


def delete(table):
    table.delete(2)


def delete_where(table):
    table.delete_where("id > ?", [1])


MUTATOR_CASES = (
    pytest.param(
        insert,
        [(1, "one"), (2, "two"), (3, "three")],
        id="insert",
    ),
    pytest.param(
        insert_all,
        [(1, "one"), (2, "two"), (3, "three"), (4, "four")],
        id="insert_all",
    ),
    pytest.param(
        upsert,
        [(1, "one"), (2, "TWO")],
        id="upsert",
    ),
    pytest.param(
        upsert_all,
        [(1, "one"), (2, "TWO"), (3, "three")],
        id="upsert_all",
    ),
    pytest.param(
        update,
        [(1, "one"), (2, "TWO")],
        id="update",
    ),
    pytest.param(delete, [(1, "one")], id="delete"),
    pytest.param(delete_where, [(1, "one")], id="delete_where"),
)


class RollbackTest(Exception):
    pass


def seed_database(path):
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("create table items (id integer primary key, value text)")
        conn.executemany("insert into items values (?, ?)", BASELINE_ROWS)
        conn.commit()
    finally:
        conn.close()
    return Database(path)


def current_rows(db):
    return db.conn.execute("select id, value from items order by id").fetchall()


def persisted_rows(path):
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("select id, value from items order by id").fetchall()
    finally:
        conn.close()


@pytest.mark.parametrize("mutate,expected_rows", MUTATOR_CASES)
def test_mutator_commits_by_default(tmp_path, mutate, expected_rows):
    path = tmp_path / "default.db"
    db = seed_database(path)

    assert not db.conn.in_transaction
    mutate(db["items"])
    assert current_rows(db) == expected_rows
    assert not db.conn.in_transaction

    db.close()
    assert persisted_rows(path) == expected_rows


@pytest.mark.parametrize("mutate,expected_rows", MUTATOR_CASES)
def test_mutator_commits_with_outer_atomic(tmp_path, mutate, expected_rows):
    path = tmp_path / "atomic.db"
    db = seed_database(path)

    with db.atomic():
        assert db.conn.in_transaction
        mutate(db["items"])
        assert current_rows(db) == expected_rows
        assert db.conn.in_transaction

    assert current_rows(db) == expected_rows
    assert not db.conn.in_transaction
    db.close()
    assert persisted_rows(path) == expected_rows


@pytest.mark.parametrize("mutate,expected_rows", MUTATOR_CASES)
def test_mutator_rolls_back_outer_atomic(tmp_path, mutate, expected_rows):
    path = tmp_path / "rollback.db"
    db = seed_database(path)

    with pytest.raises(RollbackTest), db.atomic():
        mutate(db["items"])
        assert current_rows(db) == expected_rows
        assert db.conn.in_transaction
        raise RollbackTest

    assert current_rows(db) == BASELINE_ROWS
    assert not db.conn.in_transaction
    db.close()
    assert persisted_rows(path) == BASELINE_ROWS
