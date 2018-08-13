from sqlite_utils.db import Index
import collections
import pytest
import json


def test_create_table(fresh_db):
    assert [] == fresh_db.table_names
    table = fresh_db.create_table(
        "test_table",
        {"text_col": str, "float_col": float, "int_col": int, "bool_col": bool},
    )
    assert ["test_table"] == fresh_db.table_names
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
    assert ["people"] == fresh_db.table_names
    assert expected_columns == [
        {"name": col.name, "type": col.type} for col in fresh_db["people"].columns
    ]


def test_create_table_column_order(fresh_db):
    fresh_db["table"].insert(
        collections.OrderedDict(
            (
                ("zzz", "third"),
                ("abc", "first"),
                ("ccc", "second"),
                ("bbb", "second-to-last"),
                ("aaa", "last"),
            )
        ),
        column_order=("abc", "ccc", "zzz"),
    )
    assert [
        {"name": "abc", "type": "TEXT"},
        {"name": "ccc", "type": "TEXT"},
        {"name": "zzz", "type": "TEXT"},
        {"name": "bbb", "type": "TEXT"},
        {"name": "aaa", "type": "TEXT"},
    ] == [{"name": col.name, "type": col.type} for col in fresh_db["table"].columns]


def test_create_table_works_for_m2m_with_only_foreign_keys(fresh_db):
    fresh_db["one"].insert({"id": 1}, pk="id")
    fresh_db["two"].insert({"id": 1}, pk="id")
    fresh_db["m2m"].insert(
        {"one_id": 1, "two_id": 1},
        foreign_keys=(
            ("one_id", "INTEGER", "one", "id"),
            ("two_id", "INTEGER", "two", "id"),
        ),
    )
    assert [
        {"name": "one_id", "type": "INTEGER"},
        {"name": "two_id", "type": "INTEGER"},
    ] == [{"name": col.name, "type": col.type} for col in fresh_db["m2m"].columns]
    assert sorted(
        [
            {"column": "one_id", "other_table": "one", "other_column": "id"},
            {"column": "two_id", "other_table": "two", "other_column": "id"},
        ],
        key=lambda s: repr(s),
    ) == sorted(
        [
            {
                "column": fk.column,
                "other_table": fk.other_table,
                "other_column": fk.other_column,
            }
            for fk in fresh_db["m2m"].foreign_keys
        ],
        key=lambda s: repr(s),
    )


@pytest.mark.parametrize(
    "columns,index_name,expected_index",
    (
        (
            ["is_good_dog"],
            None,
            Index(
                seq=0,
                name="idx_dogs_is_good_dog",
                unique=0,
                origin="c",
                partial=0,
                columns=["is_good_dog"],
            ),
        ),
        (
            ["is_good_dog", "age"],
            None,
            Index(
                seq=0,
                name="idx_dogs_is_good_dog_age",
                unique=0,
                origin="c",
                partial=0,
                columns=["is_good_dog", "age"],
            ),
        ),
        (
            ["age"],
            "age_index",
            Index(
                seq=0,
                name="age_index",
                unique=0,
                origin="c",
                partial=0,
                columns=["age"],
            ),
        ),
    ),
)
def test_create_index(fresh_db, columns, index_name, expected_index):
    dogs = fresh_db["dogs"]
    dogs.insert({"name": "Cleo", "twitter": "cleopaws", "age": 3, "is_good_dog": True})
    assert [] == dogs.indexes
    dogs.create_index(columns, index_name)
    assert expected_index == dogs.indexes[0]


@pytest.mark.parametrize(
    "data_structure",
    (
        ["list with one item"],
        ["list with", "two items"],
        {"dictionary": "simple"},
        {"dictionary": {"nested": "complex"}},
        [{"list": "of"}, {"two": "dicts"}],
    ),
)
def test_insert_dictionaries_and_lists_as_json(fresh_db, data_structure):
    fresh_db["test"].insert({"id": 1, "data": data_structure}, pk="id")
    row = fresh_db.conn.execute("select id, data from test").fetchone()
    assert row[0] == 1
    assert data_structure == json.loads(row[1])


def test_create_view(fresh_db):
    fresh_db["data"].insert({"foo": "foo", "bar": "bar"})
    fresh_db.create_view("bar", "select bar from data")
    rows = fresh_db.conn.execute("select * from bar").fetchall()
    assert [("bar",)] == rows
