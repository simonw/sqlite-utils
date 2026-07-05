import pytest

from sqlite_utils.db import Database, _iter_complete_sql_statements
from sqlite_utils.utils import sqlite3


@pytest.mark.parametrize(
    "sql,expected",
    (
        (
            "CREATE TABLE t(id); INSERT INTO t VALUES (1)",
            ["CREATE TABLE t(id);", "INSERT INTO t VALUES (1)"],
        ),
        (
            "INSERT INTO t VALUES ('a;b');",
            ["INSERT INTO t VALUES ('a;b');"],
        ),
        (
            "-- comment;\nCREATE TABLE t(id);",
            ["-- comment;\nCREATE TABLE t(id);"],
        ),
        (
            """
            CREATE TRIGGER t_ai AFTER INSERT ON t
            BEGIN
                UPDATE t SET value = 'a;b' WHERE id = new.id;
                INSERT INTO log VALUES ('x;y');
            END;
            """,
            [
                "CREATE TRIGGER t_ai AFTER INSERT ON t\n"
                "            BEGIN\n"
                "                UPDATE t SET value = 'a;b' WHERE id = new.id;\n"
                "                INSERT INTO log VALUES ('x;y');\n"
                "            END;"
            ],
        ),
    ),
)
def test_iter_complete_sql_statements(sql, expected):
    assert list(_iter_complete_sql_statements(sql)) == expected


def test_atomic_commits(fresh_db):
    with fresh_db.atomic():
        fresh_db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")

    assert list(fresh_db["dogs"].rows) == [{"id": 1, "name": "Cleo"}]


def test_atomic_rolls_back(fresh_db):
    with pytest.raises(RuntimeError):
        with fresh_db.atomic():
            fresh_db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
            raise RuntimeError("boom")

    assert not fresh_db["dogs"].exists()


def test_nested_atomic_rolls_back_to_savepoint(fresh_db):
    fresh_db["dogs"].create({"id": int, "name": str}, pk="id")

    with fresh_db.atomic():
        fresh_db["dogs"].insert({"id": 1, "name": "Cleo"})
        with pytest.raises(RuntimeError):
            with fresh_db.atomic():
                fresh_db["dogs"].insert({"id": 2, "name": "Pancakes"})
                raise RuntimeError("boom")
        fresh_db["dogs"].insert({"id": 3, "name": "Marnie"})

    assert list(fresh_db["dogs"].rows) == [
        {"id": 1, "name": "Cleo"},
        {"id": 3, "name": "Marnie"},
    ]


def test_outer_atomic_rolls_back_released_savepoint(fresh_db):
    with pytest.raises(RuntimeError):
        with fresh_db.atomic():
            fresh_db["dogs"].insert({"id": 1, "name": "Cleo"}, pk="id")
            with fresh_db.atomic():
                fresh_db["dogs"].insert({"id": 2, "name": "Pancakes"})
            raise RuntimeError("boom")

    assert not fresh_db["dogs"].exists()


def test_executescript_does_not_commit_open_atomic_block(fresh_db):
    with pytest.raises(RuntimeError):
        with fresh_db.atomic():
            fresh_db.executescript("""
                CREATE TABLE dogs(id INTEGER PRIMARY KEY, name TEXT);
                CREATE TRIGGER dogs_ai AFTER INSERT ON dogs
                BEGIN
                    UPDATE dogs SET name = upper(new.name) || '; updated' WHERE id = new.id;
                END;
                -- This comment has a semicolon;
                INSERT INTO dogs VALUES (1, 'Cleo; the first');
            """)
            raise RuntimeError("boom")

    assert not fresh_db["dogs"].exists()


def test_transform_does_not_commit_open_atomic_block(fresh_db):
    fresh_db["dogs"].insert({"id": 1, "name": "Cleo", "age": "5"}, pk="id")

    with pytest.raises(RuntimeError):
        with fresh_db.atomic():
            fresh_db["dogs"].insert({"id": 2, "name": "Pancakes", "age": "6"})
            fresh_db["dogs"].transform(rename={"age": "dog_age"})
            raise RuntimeError("boom")

    assert (
        fresh_db["dogs"].schema
        == 'CREATE TABLE "dogs" (\n   "id" INTEGER PRIMARY KEY,\n   "name" TEXT,\n   "age" TEXT\n)'
    )
    assert list(fresh_db["dogs"].rows) == [
        {"id": 1, "name": "Cleo", "age": "5"},
    ]


def test_transform_parent_table_with_foreign_keys_in_atomic(fresh_db):
    fresh_db.conn.execute("PRAGMA foreign_keys=ON")
    fresh_db["authors"].insert({"id": 1, "name": "Tina"}, pk="id")
    fresh_db["books"].insert(
        {"id": 1, "title": "Book", "author_id": 1},
        pk="id",
        foreign_keys={"author_id"},
    )

    with fresh_db.atomic():
        fresh_db["authors"].transform(rename={"name": "full_name"})
        assert fresh_db.conn.execute("PRAGMA foreign_keys").fetchone()[0]

    assert (
        fresh_db["authors"].schema
        == 'CREATE TABLE "authors" (\n   "id" INTEGER PRIMARY KEY,\n   "full_name" TEXT\n)'
    )
    assert fresh_db.execute("PRAGMA foreign_key_check").fetchall() == []


def test_transform_parent_table_with_foreign_keys_rolls_back(fresh_db):
    fresh_db.conn.execute("PRAGMA foreign_keys=ON")
    fresh_db["authors"].insert({"id": 1, "name": "Tina"}, pk="id")
    fresh_db["books"].insert(
        {"id": 1, "title": "Book", "author_id": 1},
        pk="id",
        foreign_keys={"author_id"},
    )

    with pytest.raises(RuntimeError):
        with fresh_db.atomic():
            fresh_db["authors"].transform(rename={"name": "full_name"})
            raise RuntimeError("boom")

    assert (
        fresh_db["authors"].schema
        == 'CREATE TABLE "authors" (\n   "id" INTEGER PRIMARY KEY,\n   "name" TEXT\n)'
    )
    assert fresh_db.conn.execute("PRAGMA foreign_keys").fetchone()[0]
    assert fresh_db.execute("PRAGMA foreign_key_check").fetchall() == []


def test_transform_detects_foreign_key_check_violations(fresh_db):
    fresh_db.conn.execute("PRAGMA foreign_keys=ON")
    fresh_db["authors"].insert({"id": 1, "name": "Tina"}, pk="id")
    fresh_db["books"].insert({"id": 1, "author_id": 2}, pk="id")

    with pytest.raises(sqlite3.IntegrityError):
        fresh_db["books"].transform(add_foreign_keys=(("author_id", "authors", "id"),))

    assert fresh_db["books"].foreign_keys == []
    assert fresh_db.conn.execute("PRAGMA foreign_keys").fetchone()[0]


def test_atomic_inside_manual_transaction_uses_savepoint(fresh_db):
    fresh_db["t"].insert({"id": 1}, pk="id")
    fresh_db.execute("begin")
    with fresh_db.atomic():
        fresh_db["t"].insert({"id": 2}, pk="id")
    # Nothing is committed until the user's own transaction commits
    assert fresh_db.conn.in_transaction
    fresh_db.rollback()
    assert [r["id"] for r in fresh_db["t"].rows] == [1]
    # And with a commit instead, the atomic block's writes persist
    fresh_db.execute("begin")
    with fresh_db.atomic():
        fresh_db["t"].insert({"id": 3}, pk="id")
    fresh_db.commit()
    assert [r["id"] for r in fresh_db["t"].rows] == [1, 3]


def test_begin_commit_rollback(tmpdir):
    path = str(tmpdir / "test.db")
    db = Database(path)
    db["t"].insert({"id": 1}, pk="id")
    db.begin()
    db["t"].insert({"id": 2}, pk="id")
    assert db.conn.in_transaction
    db.rollback()
    assert not db.conn.in_transaction
    assert [r["id"] for r in db["t"].rows] == [1]
    db.begin()
    db["t"].insert({"id": 3}, pk="id")
    db.commit()
    db.close()
    db2 = Database(path)
    assert [r["id"] for r in db2["t"].rows] == [1, 3]
    db2.close()


def test_begin_inside_transaction_errors(fresh_db):
    fresh_db.begin()
    with pytest.raises(sqlite3.OperationalError):
        fresh_db.begin()
    fresh_db.rollback()


def test_commit_and_rollback_without_transaction_are_noops(fresh_db):
    fresh_db.commit()
    fresh_db.rollback()
    assert not fresh_db.conn.in_transaction


def test_execute_write_commits_immediately(tmpdir):
    path = str(tmpdir / "test.db")
    db = Database(path)
    db["t"].insert({"id": 1}, pk="id")
    db.execute("insert into t (id) values (2)")
    # No implicit transaction is left open
    assert not db.conn.in_transaction
    # A completely separate connection sees the row straight away
    other = sqlite3.connect(path)
    assert other.execute("select count(*) from t").fetchone()[0] == 2
    other.close()
    db.close()


def test_execute_write_respects_explicit_transaction(fresh_db):
    fresh_db["t"].insert({"id": 1}, pk="id")
    fresh_db.begin()
    fresh_db.execute("insert into t (id) values (2)")
    # Still inside the explicit transaction - not committed
    assert fresh_db.conn.in_transaction
    fresh_db.rollback()
    assert [r["id"] for r in fresh_db["t"].rows] == [1]


def test_execute_comment_prefixed_begin_leaves_transaction_open(fresh_db):
    # A BEGIN hidden behind a leading comment must not be auto-committed
    # out from under the caller
    fresh_db["t"].insert({"id": 1}, pk="id")
    fresh_db.execute("-- start a transaction\nbegin")
    assert fresh_db.conn.in_transaction
    fresh_db.execute("insert into t (id) values (2)")
    fresh_db.rollback()
    assert [r["id"] for r in fresh_db["t"].rows] == [1]


def test_query_returning_commits_after_iteration(tmpdir):
    if sqlite3.sqlite_version_info < (3, 35, 0):
        import pytest as _pytest

        _pytest.skip("RETURNING requires SQLite 3.35.0 or higher")
    path = str(tmpdir / "test.db")
    db = Database(path)
    db["t"].insert({"id": 1}, pk="id")
    rows = list(db.query("insert into t (id) values (2) returning id"))
    assert rows == [{"id": 2}]
    assert not db.conn.in_transaction
    other = sqlite3.connect(path)
    assert other.execute("select count(*) from t").fetchone()[0] == 2
    other.close()
    db.close()
