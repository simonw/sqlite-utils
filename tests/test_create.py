from sqlite_utils import db
import sqlite3
import pytest


@pytest.fixture
def fresh_db():
    return db.Database(sqlite3.connect(":memory:"))


def test_create_table(fresh_db):
    assert [] == fresh_db.tables
    table = fresh_db.create_table(
        "test_table",
        {"text_col": str, "float_col": float, "int_col": int, "bool_col": bool},
    )
    assert ["test_table"] == fresh_db.tables
    assert [
        {"name": "text_col", "type": "TEXT"},
        {"name": "float_col", "type": "FLOAT"},
        {"name": "int_col", "type": "INTEGER"},
        {"name": "bool_col", "type": "INTEGER"},
    ] == [{"name": col.name, "type": col.type} for col in table.columns]


@pytest.mark.parametrize(
    "example,expected_columns",
    (
        (
            {"name": "Ravi", "age": 63},
            [{"name": "name", "type": "TEXT"}, {"name": "age", "type": "INTEGER"}],
        ),
    ),
)
def test_create_table_from_example(fresh_db, example, expected_columns):
    fresh_db["people"].insert(example)
    assert ["people"] == fresh_db.tables
    assert expected_columns == [
        {"name": col.name, "type": col.type} for col in fresh_db["people"].columns
    ]
