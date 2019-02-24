from sqlite_utils.db import Index, Database
import collections
import datetime
import json
import pathlib
import pytest
import sqlite3

try:
    import pandas as pd
except ImportError:
    pd = None


def test_create_table(fresh_db):
    assert [] == fresh_db.table_names()
    table = fresh_db.create_table(
        "test_table",
        {
            "text_col": str,
            "float_col": float,
            "int_col": int,
            "bool_col": bool,
            "bytes_col": bytes,
            "datetime_col": datetime.datetime,
        },
    )
    assert ["test_table"] == fresh_db.table_names()
    assert [
        {"name": "text_col", "type": "TEXT"},
        {"name": "float_col", "type": "FLOAT"},
        {"name": "int_col", "type": "INTEGER"},
        {"name": "bool_col", "type": "INTEGER"},
        {"name": "bytes_col", "type": "BLOB"},
        {"name": "datetime_col", "type": "TEXT"},
    ] == [{"name": col.name, "type": col.type} for col in table.columns]
    assert (
        "CREATE TABLE [test_table] (\n"
        "   [text_col] TEXT,\n"
        "   [float_col] FLOAT,\n"
        "   [int_col] INTEGER,\n"
        "   [bool_col] INTEGER,\n"
        "   [bytes_col] BLOB,\n"
        "   [datetime_col] TEXT\n"
        ")"
    ) == table.schema


@pytest.mark.parametrize(
    "example,expected_columns",
    (
        (
            {"name": "Ravi", "age": 63},
            [{"name": "name", "type": "TEXT"}, {"name": "age", "type": "INTEGER"}],
        ),
        (
            {"create": "Reserved word", "table": "Another"},
            [{"name": "create", "type": "TEXT"}, {"name": "table", "type": "TEXT"}],
        ),
    ),
)
def test_create_table_from_example(fresh_db, example, expected_columns):
    fresh_db["people"].insert(example)
    assert ["people"] == fresh_db.table_names()
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


def test_create_index_unique(fresh_db):
    dogs = fresh_db["dogs"]
    dogs.insert({"name": "Cleo", "twitter": "cleopaws", "age": 3, "is_good_dog": True})
    assert [] == dogs.indexes
    dogs.create_index(["name"], unique=True)
    assert (
        Index(
            seq=0,
            name="idx_dogs_name",
            unique=1,
            origin="c",
            partial=0,
            columns=["name"],
        )
        == dogs.indexes[0]
    )


def test_create_index_if_not_exists(fresh_db):
    dogs = fresh_db["dogs"]
    dogs.insert({"name": "Cleo", "twitter": "cleopaws", "age": 3, "is_good_dog": True})
    assert [] == dogs.indexes
    dogs.create_index(["name"])
    assert 1 == len(dogs.indexes)
    with pytest.raises(sqlite3.OperationalError):
        dogs.create_index(["name"])
    dogs.create_index(["name"], if_not_exists=True)


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


def test_insert_thousands_using_generator(fresh_db):
    fresh_db["test"].insert_all(
        {"i": i, "word": "word_{}".format(i)} for i in range(10000)
    )
    assert [{"name": "i", "type": "INTEGER"}, {"name": "word", "type": "TEXT"}] == [
        {"name": col.name, "type": col.type} for col in fresh_db["test"].columns
    ]
    assert 10000 == fresh_db["test"].count


def test_insert_thousands_ignores_extra_columns_after_first_100(fresh_db):
    fresh_db["test"].insert_all(
        [{"i": i, "word": "word_{}".format(i)} for i in range(100)]
        + [{"i": 101, "extra": "This extra column should cause an exception"}]
    )
    rows = fresh_db.execute_returning_dicts("select * from test where i = 101")
    assert [{"i": 101, "word": None}] == rows


def test_insert_hash_id(fresh_db):
    dogs = fresh_db["dogs"]
    id = dogs.upsert({"name": "Cleo", "twitter": "cleopaws"}, hash_id="id").last_pk
    assert "f501265970505d9825d8d9f590bfab3519fb20b1" == id
    assert 1 == dogs.count
    # Upserting a second time should not create a new row
    id2 = dogs.upsert({"name": "Cleo", "twitter": "cleopaws"}, hash_id="id").last_pk
    assert "f501265970505d9825d8d9f590bfab3519fb20b1" == id2
    assert 1 == dogs.count


def test_create_view(fresh_db):
    fresh_db["data"].insert({"foo": "foo", "bar": "bar"})
    fresh_db.create_view("bar", "select bar from data")
    rows = fresh_db.conn.execute("select * from bar").fetchall()
    assert [("bar",)] == rows


def test_vacuum(fresh_db):
    fresh_db["data"].insert({"foo": "foo", "bar": "bar"})
    fresh_db.vacuum()


def test_works_with_pathlib_path(tmpdir):
    path = pathlib.Path(tmpdir / "test.db")
    db = Database(path)
    db["demo"].insert_all([{"foo": 1}])
    assert 1 == db["demo"].count


@pytest.mark.skipif(pd is None, reason="pandas and numpy are not installed")
def test_create_table_numpy(fresh_db):
    import numpy as np

    df = pd.DataFrame({"col 1": range(3), "col 2": range(3)})
    fresh_db["pandas"].insert_all(df.to_dict(orient="records"))
    assert [
        {"col 1": 0, "col 2": 0},
        {"col 1": 1, "col 2": 1},
        {"col 1": 2, "col 2": 2},
    ] == list(fresh_db["pandas"].rows)
    # Now try all the different types
    df = pd.DataFrame(
        {
            "np.int8": [-8],
            "np.int16": [-16],
            "np.int32": [-32],
            "np.int64": [-64],
            "np.uint8": [8],
            "np.uint16": [16],
            "np.uint32": [32],
            "np.uint64": [64],
            "np.float16": [16.5],
            "np.float32": [32.5],
            "np.float64": [64.5],
        }
    )
    df = df.astype(
        {
            "np.int8": "int8",
            "np.int16": "int16",
            "np.int32": "int32",
            "np.int64": "int64",
            "np.uint8": "uint8",
            "np.uint16": "uint16",
            "np.uint32": "uint32",
            "np.uint64": "uint64",
            "np.float16": "float16",
            "np.float32": "float32",
            "np.float64": "float64",
        }
    )
    assert [
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float16",
        "float32",
        "float64",
    ] == [str(t) for t in df.dtypes]
    fresh_db["types"].insert_all(df.to_dict(orient="records"))
    assert [
        {
            "np.float16": 16.5,
            "np.float32": 32.5,
            "np.float64": 64.5,
            "np.int16": -16,
            "np.int32": -32,
            "np.int64": -64,
            "np.int8": -8,
            "np.uint16": 16,
            "np.uint32": 32,
            "np.uint64": 64,
            "np.uint8": 8,
        }
    ] == list(fresh_db["types"].rows)
