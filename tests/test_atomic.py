import pytest

from sqlite_utils.utils import sqlite3


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
                    UPDATE dogs SET name = upper(new.name) WHERE id = new.id;
                END;
                INSERT INTO dogs VALUES (1, 'Cleo');
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
