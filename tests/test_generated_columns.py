import pytest

from sqlite_utils import Database
from sqlite_utils.create_table_parser import (
    GeneratedColumn,
    parse_generated_columns,
)
from sqlite_utils.db import AlterError, TransformError
from sqlite_utils.utils import sqlite3

pytestmark = pytest.mark.skipif(
    sqlite3.sqlite_version_info < (3, 31, 0),
    reason="Generated columns need SQLite 3.31+",
)

SCHEMA = """
create table items (
    id integer primary key,
    price real,
    qty integer,
    total real generated always as (price * qty) virtual,
    total_s real as (price * qty * 2) stored,
    label text as (name || '!') stored not null,
    name text
)
"""


@pytest.fixture
def db():
    db = Database(memory=True)
    db.execute(SCHEMA)
    db.execute("create index idx_total on items(total)")
    db.execute("insert into items (price, qty, name) values (2.5, 4, 'a'), (1, 3, 'b')")
    return db


def rows(db):
    return list(db["items"].rows)


@pytest.mark.parametrize(
    "sql,expected",
    (
        (
            SCHEMA,
            {
                "total": GeneratedColumn("price * qty"),
                "total_s": GeneratedColumn("price * qty * 2", stored=True),
                "label": GeneratedColumn("name || '!'", stored=True),
            },
        ),
        # No VIRTUAL/STORED keyword means virtual
        ("create table t (a, b as (a + 1))", {"b": GeneratedColumn("a + 1")}),
        # AS inside CAST(), type arguments, DEFAULT and CHECK are not generated
        (
            "create table t (a varchar(10) default (cast(1 as text)) "
            "check (cast(a as integer) > 0), "
            "\"b c\" text generated always as (cast(a as text) || ')') stored, "
            "check (a as_of))",
            {"b c": GeneratedColumn("cast(a as text) || ')'", stored=True)},
        ),
        (
            "create table t (a, -- as (nope)\n b /* as (x) */ as (a) virtual)",
            {"b": GeneratedColumn("a")},
        ),
        ("create table t (a, b)", {}),
    ),
)
def test_parse_generated_columns(sql, expected):
    assert parse_generated_columns(sql) == expected


def test_introspection(db):
    table = db["items"]
    assert [c.name for c in table.columns] == [
        "id",
        "price",
        "qty",
        "total",
        "total_s",
        "label",
        "name",
    ]
    assert table.columns_dict["total"] is float
    assert table.generated_columns == {
        "total": GeneratedColumn("price * qty", stored=False),
        "total_s": GeneratedColumn("price * qty * 2", stored=True),
        "label": GeneratedColumn("name || '!'", stored=True),
    }
    assert db["nope"].generated_columns == {}


def test_transform_preserves_generated_columns(db):
    before = rows(db)
    db["items"].transform()
    assert rows(db) == before
    assert db["items"].generated_columns == {
        "total": GeneratedColumn("price * qty", stored=False),
        "total_s": GeneratedColumn("price * qty * 2", stored=True),
        "label": GeneratedColumn("name || '!'", stored=True),
    }
    assert [c.name for c in db["items"].columns if c.notnull] == ["label"]
    # The index on the generated column survives
    assert [i.name for i in db["items"].indexes] == ["idx_total"]
    # ... and the columns are still computed
    db["items"].insert({"price": 2, "qty": 5, "name": "c"})
    assert db["items"].get(3) == {
        "id": 3,
        "price": 2.0,
        "qty": 5,
        "total": 10.0,
        "total_s": 20.0,
        "label": "c!",
        "name": "c",
    }


def test_transform_column_order(db):
    db["items"].transform(column_order=["name", "total", "id"])
    assert [c.name for c in db["items"].columns] == [
        "name",
        "total",
        "id",
        "price",
        "qty",
        "total_s",
        "label",
    ]
    assert rows(db)[0] == {
        "name": "a",
        "total": 10.0,
        "id": 1,
        "price": 2.5,
        "qty": 4,
        "total_s": 20.0,
        "label": "a!",
    }


def test_transform_rename_rewrites_expressions(db):
    db["items"].transform(rename={"price": "unit price", "total": "grand"})
    assert db["items"].generated_columns == {
        "grand": GeneratedColumn('"unit price" * qty', stored=False),
        "total_s": GeneratedColumn('"unit price" * qty * 2', stored=True),
        "label": GeneratedColumn("name || '!'", stored=True),
    }
    assert rows(db)[0]["grand"] == 10.0
    assert db["items"].indexes[0].columns == ["grand"]


def test_transform_drop_generated_column(db):
    db.execute("drop index idx_total")
    db["items"].transform(drop={"total"})
    assert set(db["items"].generated_columns) == {"total_s", "label"}
    assert "total" not in rows(db)[0]


def test_transform_drop_column_used_by_generated_column(db):
    with pytest.raises(TransformError) as ex:
        db["items"].transform(drop={"qty"})
    assert "used by generated column 'total'" in str(ex.value)
    # Dropping the dependent generated columns at the same time is fine
    db.execute("drop index idx_total")
    db["items"].transform(drop={"qty", "total", "total_s"})
    assert [c.name for c in db["items"].columns] == ["id", "price", "label", "name"]


def test_transform_change_type_of_generated_column(db):
    db["items"].transform(types={"total": int, "qty": str})
    assert db["items"].columns_dict["total"] is int
    assert db["items"].generated_columns["total"] == GeneratedColumn("price * qty")
    assert rows(db)[0]["qty"] == "4"


def test_generated_column_cannot_be_primary_key(db):
    with pytest.raises(AlterError):
        db["items"].transform(pk="total_s")
    assert db["items"].pks == ["id"]


def test_add_foreign_key_and_extract_preserve_generated_columns(db):
    db["cats"].insert_all([{"id": 4, "name": "x"}, {"id": 3, "name": "y"}], pk="id")
    db["items"].add_column("kind", str)
    db["items"].update(1, {"kind": "k"})
    db["items"].add_foreign_key("qty", "cats", "id")
    db["items"].extract("kind")
    assert set(db["items"].generated_columns) == {"total", "total_s", "label"}
    assert rows(db)[0]["total"] == 10.0
    assert rows(db)[0]["kind_id"] == 1
    # A column that a generated column depends on cannot be extracted
    with pytest.raises(TransformError):
        db["items"].extract("name")


def test_strict_table(db):
    if not db.supports_strict:
        pytest.skip("No STRICT support")
    db.execute(
        "create table s (id integer primary key, a integer, "
        "b integer as (a * 2) stored) strict"
    )
    db["s"].insert({"a": 2})
    db["s"].transform(rename={"a": "c"})
    assert db["s"].strict
    assert list(db["s"].rows) == [{"id": 1, "c": 2, "b": 4}]


def test_alter_with_generated_column_in_record(db):
    # Used to fail with "duplicate column name" after trying to add the column
    with pytest.raises(sqlite3.OperationalError) as ex:
        db["items"].insert({"price": 1, "qty": 1, "total": 5}, alter=True)
    assert "cannot INSERT into generated column" in str(ex.value)


def test_lookup_on_generated_column(db):
    assert db["items"].lookup({"label": "b!"}) == 2


def test_create_table_transform_keeps_generated_columns(db):
    db["items"].create(
        {"id": int, "price": float, "qty": int, "name": str, "note": str},
        pk="id",
        transform=True,
    )
    assert set(db["items"].generated_columns) == {"total", "total_s", "label"}
    assert "note" in db["items"].columns_dict
