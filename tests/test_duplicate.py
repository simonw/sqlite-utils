import datetime
import sqlite3

import pytest

from sqlite_utils.db import NoTable


def test_duplicate(fresh_db):
    # Create table using native Sqlite statement:
    fresh_db.execute("""CREATE TABLE "table1" (
    "text_col" TEXT,
    "real_col" REAL,
    "int_col" INTEGER,
    "bool_col" INTEGER,
    "datetime_col" TEXT)""")
    # Insert one row of mock data:
    dt = datetime.datetime.now(datetime.timezone.utc)
    data = {
        "text_col": "Cleo",
        "real_col": 3.14,
        "int_col": -255,
        "bool_col": True,
        "datetime_col": str(dt),
    }
    table1 = fresh_db.table("table1")
    row_id = table1.insert(data).last_rowid
    # Duplicate table:
    table2 = table1.duplicate("table2")
    # Ensure data integrity:
    assert data == table2.get(row_id)
    # Ensure schema integrity:
    assert [
        {"name": "text_col", "type": "TEXT"},
        {"name": "real_col", "type": "REAL"},
        {"name": "int_col", "type": "INTEGER"},
        {"name": "bool_col", "type": "INTEGER"},
        {"name": "datetime_col", "type": "TEXT"},
    ] == [{"name": col.name, "type": col.type} for col in table2.columns]


def test_duplicate_fails_if_table_does_not_exist(fresh_db):
    with pytest.raises(NoTable):
        fresh_db.table("not_a_table").duplicate("duplicated")


def test_duplicate_copies_create_table_statement(fresh_db):
    fresh_db.execute("CREATE TABLE owners (id INTEGER PRIMARY KEY)")
    fresh_db.execute("""
        CREATE TABLE pets (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            age INTEGER CHECK (age >= 0) DEFAULT 1,
            owner_id INTEGER REFERENCES owners(id)
        )
    """)
    fresh_db.table("owners").insert({"id": 1})
    fresh_db.table("pets").insert({"id": 5, "name": "Cleo", "age": 3, "owner_id": 1})

    copy = fresh_db.table("pets").duplicate("pets_copy")

    assert copy.schema == fresh_db.table("pets").schema.replace(
        "TABLE pets", "TABLE pets_copy"
    )
    assert copy.pks == ["id"]
    assert [(fk.column, fk.other_table) for fk in copy.foreign_keys] == [
        ("owner_id", "owners")
    ]
    assert list(copy.rows) == [{"id": 5, "name": "Cleo", "age": 3, "owner_id": 1}]
    copy.insert({"id": 6, "name": "Pancakes"})
    assert copy.get(6)["age"] == 1
    for bad_row in (
        {"id": 7, "name": "Cleo"},
        {"id": 8, "name": None},
        {"id": 9, "name": "Rex", "age": -1},
    ):
        with pytest.raises(sqlite3.IntegrityError):
            copy.insert(bad_row)


def test_duplicate_preserves_rowids(fresh_db):
    table = fresh_db.table("plain")
    table.insert_all([{"v": "a"}, {"v": "b"}, {"v": "c"}])
    fresh_db.execute("DELETE FROM plain WHERE v = 'a'")

    table.duplicate("plain_copy")

    assert fresh_db.execute("SELECT rowid, v FROM plain_copy").fetchall() == [
        (2, "b"),
        (3, "c"),
    ]


def test_duplicate_without_rowid(fresh_db):
    fresh_db.execute("CREATE TABLE tags (k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID")
    fresh_db.table("tags").insert({"k": "a", "v": "one"})

    copy = fresh_db.table("tags").duplicate("tags_copy")

    assert copy.schema.endswith("WITHOUT ROWID")
    assert list(copy.rows) == [{"k": "a", "v": "one"}]


def test_duplicate_strict(fresh_db):
    if not fresh_db.supports_strict:
        pytest.skip("SQLite version does not support strict tables")
    fresh_db.table("t").create({"id": int}, strict=True)

    assert fresh_db.table("t").duplicate("t_copy").strict


@pytest.mark.skipif(
    sqlite3.sqlite_version_info < (3, 31, 0),
    reason="Generated columns need SQLite 3.31",
)
def test_duplicate_generated_columns(fresh_db):
    fresh_db.execute("CREATE TABLE t (a INTEGER, b INTEGER AS (a + 1))")
    fresh_db.table("t").insert({"a": 1})

    fresh_db.table("t").duplicate("t_copy")

    assert fresh_db.execute("SELECT a, b FROM t_copy").fetchall() == [(1, 2)]


def test_duplicate_recreates_indexes_with_new_names(fresh_db):
    fresh_db.execute("CREATE TABLE pets (id INTEGER PRIMARY KEY, name TEXT, age INT)")
    fresh_db.execute("CREATE UNIQUE INDEX idx_pets_name ON pets (name)")
    fresh_db.execute("CREATE INDEX young ON pets (age) WHERE age < 2")
    fresh_db.table("pets").insert({"id": 1, "name": "Cleo", "age": 1})

    copy = fresh_db.table("pets").duplicate("pets_copy")

    assert {index.name for index in fresh_db.table("pets").indexes} == {
        "idx_pets_name",
        "young",
    }
    assert {(index.name, index.unique) for index in copy.indexes} == {
        ("idx_pets_copy_name", 1),
        ("pets_copy_young", 0),
    }
    assert fresh_db.execute(
        "SELECT sql FROM sqlite_master WHERE name = 'pets_copy_young'"
    ).fetchone() == ("CREATE INDEX pets_copy_young ON pets_copy (age) WHERE age < 2",)
    with pytest.raises(sqlite3.IntegrityError):
        copy.insert({"id": 2, "name": "Cleo"})


def test_duplicate_index_name_avoids_existing_names(fresh_db):
    fresh_db.execute("CREATE TABLE pets (id INTEGER PRIMARY KEY, name TEXT)")
    fresh_db.execute("CREATE INDEX idx_pets_name ON pets (name)")
    fresh_db.execute("CREATE TABLE other (name TEXT)")
    fresh_db.execute("CREATE INDEX idx_copy_name ON other (name)")

    copy = fresh_db.table("pets").duplicate("copy")

    assert [index.name for index in copy.indexes] == ["idx_copy_name_2"]


def test_duplicate_does_not_copy_triggers(fresh_db):
    fresh_db.execute("CREATE TABLE pets (id INTEGER PRIMARY KEY, name TEXT)")
    fresh_db.execute("CREATE TABLE log (name TEXT)")
    fresh_db.execute("""
        CREATE TRIGGER pets_insert AFTER INSERT ON pets
        BEGIN INSERT INTO log VALUES (new.name); END
    """)

    copy = fresh_db.table("pets").duplicate("pets_copy")
    copy.insert({"id": 1, "name": "Cleo"})

    assert copy.triggers == []
    assert fresh_db.table("log").count == 0


def test_duplicate_fts_table_copies_rows(fresh_db):
    fresh_db.table("docs").insert({"title": "hello"})
    fresh_db.table("docs").enable_fts(["title"])

    copy = fresh_db.table("docs_fts").duplicate("docs_fts_copy")

    assert copy.virtual_table_using is None
    assert list(copy.rows) == [{"title": "hello"}]


def test_duplicate_self_referencing_foreign_key_points_at_copy(fresh_db):
    fresh_db.execute("""
        CREATE TABLE nodes (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER REFERENCES nodes(id)
        )
    """)
    fresh_db.table("nodes").insert_all([{"id": 1}, {"id": 2, "parent_id": 1}])

    copy = fresh_db.table("nodes").duplicate("nodes_copy")

    assert [(fk.column, fk.other_table) for fk in copy.foreign_keys] == [
        ("parent_id", "nodes_copy")
    ]
    assert list(copy.rows) == [
        {"id": 1, "parent_id": None},
        {"id": 2, "parent_id": 1},
    ]


def test_duplicate_preserves_autoincrement_sequence(fresh_db):
    fresh_db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
    table = fresh_db.table("t")
    table.insert_all([{"v": "a"}, {"v": "b"}, {"v": "c"}])
    fresh_db.execute("DELETE FROM t WHERE id = 3")

    copy = table.duplicate("t_copy")

    assert "AUTOINCREMENT" in copy.schema
    assert copy.insert({"v": "d"}).last_rowid == 4


def test_duplicate_rewrites_table_qualified_columns(fresh_db):
    fresh_db.execute("""
        CREATE TABLE pets (
            age INTEGER CHECK (pets.age >= 0),
            note TEXT DEFAULT 'pets.age'
        )
    """)
    fresh_db.execute('CREATE INDEX young ON pets (age) WHERE "pets".age < 2')
    fresh_db.table("pets").insert({"age": 1})

    copy = fresh_db.table("pets").duplicate("pets_copy")

    assert "CHECK (pets_copy.age >= 0)" in copy.schema
    assert "DEFAULT 'pets.age'" in copy.schema
    assert fresh_db.execute(
        "SELECT sql FROM sqlite_master WHERE name = 'pets_copy_young'"
    ).fetchone() == (
        'CREATE INDEX pets_copy_young ON pets_copy (age) WHERE "pets_copy".age < 2',
    )
    with pytest.raises(sqlite3.IntegrityError):
        copy.insert({"age": -1})


def test_duplicate_names_that_need_quoting(fresh_db):
    table = fresh_db.table("my pets")
    table.insert({"id": 1, "name": "Cleo"}, pk="id")
    table.create_index(["name"])

    copy = table.duplicate('copy "2"')

    assert copy.schema.startswith('CREATE TABLE "copy ""2"""')
    assert copy.pks == ["id"]
    assert [index.name for index in copy.indexes] == ['copy "2"_idx_my_pets_name']
    assert list(copy.rows) == [{"id": 1, "name": "Cleo"}]
