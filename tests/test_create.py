from sqlite_utils.db import (
    Index,
    Database,
    DescIndex,
    AlterError,
    NoObviousTable,
    OperationalError,
    ForeignKey,
    Table,
    View,
)
from sqlite_utils.utils import hash_record, sqlite3
import collections
import datetime
import decimal
import json
import pathlib
import pytest
import uuid

from .utils import collapse_whitespace

try:
    import pandas as pd  # type: ignore
except ImportError:
    pd = None  # type: ignore


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


def test_create_table_compound_primary_key(fresh_db):
    table = fresh_db.create_table(
        "test_table", {"id1": str, "id2": str, "value": int}, pk=("id1", "id2")
    )
    assert (
        "CREATE TABLE [test_table] (\n"
        "   [id1] TEXT,\n"
        "   [id2] TEXT,\n"
        "   [value] INTEGER,\n"
        "   PRIMARY KEY ([id1], [id2])\n"
        ")"
    ) == table.schema
    assert ["id1", "id2"] == table.pks


@pytest.mark.parametrize("pk", ("id", ["id"]))
def test_create_table_with_single_primary_key(fresh_db, pk):
    fresh_db["foo"].insert({"id": 1}, pk=pk)
    assert (
        fresh_db["foo"].schema == "CREATE TABLE [foo] (\n   [id] INTEGER PRIMARY KEY\n)"
    )


def test_create_table_with_invalid_column_characters(fresh_db):
    with pytest.raises(AssertionError):
        fresh_db.create_table("players", {"name[foo]": str})


def test_create_table_with_defaults(fresh_db):
    table = fresh_db.create_table(
        "players",
        {"name": str, "score": int},
        defaults={"score": 1, "name": "bob''bob"},
    )
    assert ["players"] == fresh_db.table_names()
    assert [{"name": "name", "type": "TEXT"}, {"name": "score", "type": "INTEGER"}] == [
        {"name": col.name, "type": col.type} for col in table.columns
    ]
    assert (
        "CREATE TABLE [players] (\n   [name] TEXT DEFAULT 'bob''''bob',\n   [score] INTEGER DEFAULT 1\n)"
    ) == table.schema


def test_create_table_with_bad_not_null(fresh_db):
    with pytest.raises(AssertionError):
        fresh_db.create_table(
            "players", {"name": str, "score": int}, not_null={"mouse"}
        )


def test_create_table_with_not_null(fresh_db):
    table = fresh_db.create_table(
        "players",
        {"name": str, "score": int},
        not_null={"name", "score"},
        defaults={"score": 3},
    )
    assert ["players"] == fresh_db.table_names()
    assert [{"name": "name", "type": "TEXT"}, {"name": "score", "type": "INTEGER"}] == [
        {"name": col.name, "type": col.type} for col in table.columns
    ]
    assert (
        "CREATE TABLE [players] (\n   [name] TEXT NOT NULL,\n   [score] INTEGER NOT NULL DEFAULT 3\n)"
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
        ({"day": datetime.time(11, 0)}, [{"name": "day", "type": "TEXT"}]),
        ({"decimal": decimal.Decimal("1.2")}, [{"name": "decimal", "type": "FLOAT"}]),
        (
            {"memoryview": memoryview(b"hello")},
            [{"name": "memoryview", "type": "BLOB"}],
        ),
        ({"uuid": uuid.uuid4()}, [{"name": "uuid", "type": "TEXT"}]),
        ({"foo[bar]": 1}, [{"name": "foo_bar_", "type": "INTEGER"}]),
    ),
)
def test_create_table_from_example(fresh_db, example, expected_columns):
    people_table = fresh_db["people"]
    assert people_table.last_rowid is None
    assert people_table.last_pk is None
    people_table.insert(example)
    assert 1 == people_table.last_rowid
    assert 1 == people_table.last_pk
    assert ["people"] == fresh_db.table_names()
    assert expected_columns == [
        {"name": col.name, "type": col.type} for col in fresh_db["people"].columns
    ]


def test_create_table_from_example_with_compound_primary_keys(fresh_db):
    record = {"name": "Zhang", "group": "staff", "employee_id": 2}
    table = fresh_db["people"].insert(record, pk=("group", "employee_id"))
    assert ["group", "employee_id"] == table.pks
    assert record == table.get(("staff", 2))


@pytest.mark.parametrize(
    "method_name", ("insert", "upsert", "insert_all", "upsert_all")
)
def test_create_table_with_custom_columns(fresh_db, method_name):
    table = fresh_db["dogs"]
    method = getattr(table, method_name)
    record = {"id": 1, "name": "Cleo", "age": "5"}
    if method_name.endswith("_all"):
        record = [record]
    method(record, pk="id", columns={"age": int, "weight": float})
    assert ["dogs"] == fresh_db.table_names()
    expected_columns = [
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "TEXT"},
        {"name": "age", "type": "INTEGER"},
        {"name": "weight", "type": "FLOAT"},
    ]
    assert expected_columns == [
        {"name": col.name, "type": col.type} for col in table.columns
    ]
    assert [{"id": 1, "name": "Cleo", "age": 5, "weight": None}] == list(table.rows)


@pytest.mark.parametrize("use_table_factory", [True, False])
def test_create_table_column_order(fresh_db, use_table_factory):
    row = collections.OrderedDict(
        (
            ("zzz", "third"),
            ("abc", "first"),
            ("ccc", "second"),
            ("bbb", "second-to-last"),
            ("aaa", "last"),
        )
    )
    column_order = ("abc", "ccc", "zzz")
    if use_table_factory:
        fresh_db.table("table", column_order=column_order).insert(row)
    else:
        fresh_db["table"].insert(row, column_order=column_order)
    assert [
        {"name": "abc", "type": "TEXT"},
        {"name": "ccc", "type": "TEXT"},
        {"name": "zzz", "type": "TEXT"},
        {"name": "bbb", "type": "TEXT"},
        {"name": "aaa", "type": "TEXT"},
    ] == [{"name": col.name, "type": col.type} for col in fresh_db["table"].columns]


@pytest.mark.parametrize(
    "foreign_key_specification,expected_exception",
    (
        # You can specify triples, pairs, or a list of columns
        ((("one_id", "one", "id"), ("two_id", "two", "id")), False),
        ((("one_id", "one"), ("two_id", "two")), False),
        (("one_id", "two_id"), False),
        # You can also specify ForeignKey tuples:
        (
            (
                ForeignKey("m2m", "one_id", "one", "id"),
                ForeignKey("m2m", "two_id", "two", "id"),
            ),
            False,
        ),
        # If you specify a column that doesn't point to a table, you  get an error:
        (("one_id", "two_id", "three_id"), NoObviousTable),
        # Tuples of the wrong length get an error:
        ((("one_id", "one", "id", "five"), ("two_id", "two", "id")), AssertionError),
        # Likewise a bad column:
        ((("one_id", "one", "id2"),), AlterError),
        # Or a list of dicts
        (({"one_id": "one"},), AssertionError),
    ),
)
@pytest.mark.parametrize("use_table_factory", [True, False])
def test_create_table_works_for_m2m_with_only_foreign_keys(
    fresh_db, foreign_key_specification, expected_exception, use_table_factory
):
    if use_table_factory:
        fresh_db.table("one", pk="id").insert({"id": 1})
        fresh_db.table("two", pk="id").insert({"id": 1})
    else:
        fresh_db["one"].insert({"id": 1}, pk="id")
        fresh_db["two"].insert({"id": 1}, pk="id")

    row = {"one_id": 1, "two_id": 1}

    def do_it():
        if use_table_factory:
            fresh_db.table("m2m", foreign_keys=foreign_key_specification).insert(row)
        else:
            fresh_db["m2m"].insert(row, foreign_keys=foreign_key_specification)

    if expected_exception:
        with pytest.raises(expected_exception):
            do_it()
        return
    else:
        do_it()
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


def test_self_referential_foreign_key(fresh_db):
    assert [] == fresh_db.table_names()
    table = fresh_db.create_table(
        "test_table",
        columns={
            "id": int,
            "ref": int,
        },
        pk="id",
        foreign_keys=(("ref", "test_table", "id"),),
    )
    assert (
        "CREATE TABLE [test_table] (\n"
        "   [id] INTEGER PRIMARY KEY,\n"
        "   [ref] INTEGER REFERENCES [test_table]([id])\n"
        ")"
    ) == table.schema


def test_create_error_if_invalid_foreign_keys(fresh_db):
    with pytest.raises(AlterError):
        fresh_db["one"].insert(
            {"id": 1, "ref_id": 3},
            pk="id",
            foreign_keys=(("ref_id", "bad_table", "bad_column"),),
        )


def test_create_error_if_invalid_self_referential_foreign_keys(fresh_db):
    with pytest.raises(AlterError) as ex:
        fresh_db["one"].insert(
            {"id": 1, "ref_id": 3},
            pk="id",
            foreign_keys=(("ref_id", "one", "bad_column"),),
        )
        assert ex.value.args == ("No such column: one.bad_column",)


@pytest.mark.parametrize(
    "col_name,col_type,not_null_default,expected_schema",
    (
        ("nickname", str, None, "CREATE TABLE [dogs] ( [name] TEXT , [nickname] TEXT)"),
        ("dob", datetime.date, None, "CREATE TABLE [dogs] ( [name] TEXT , [dob] TEXT)"),
        ("age", int, None, "CREATE TABLE [dogs] ( [name] TEXT , [age] INTEGER)"),
        ("weight", float, None, "CREATE TABLE [dogs] ( [name] TEXT , [weight] FLOAT)"),
        ("text", "TEXT", None, "CREATE TABLE [dogs] ( [name] TEXT , [text] TEXT)"),
        (
            "integer",
            "INTEGER",
            None,
            "CREATE TABLE [dogs] ( [name] TEXT , [integer] INTEGER)",
        ),
        ("float", "FLOAT", None, "CREATE TABLE [dogs] ( [name] TEXT , [float] FLOAT)"),
        ("blob", "blob", None, "CREATE TABLE [dogs] ( [name] TEXT , [blob] BLOB)"),
        (
            "default_str",
            None,
            None,
            "CREATE TABLE [dogs] ( [name] TEXT , [default_str] TEXT)",
        ),
        (
            "nickname",
            str,
            "",
            "CREATE TABLE [dogs] ( [name] TEXT , [nickname] TEXT NOT NULL DEFAULT '')",
        ),
        (
            "nickname",
            str,
            "dawg's dawg",
            "CREATE TABLE [dogs] ( [name] TEXT , [nickname] TEXT NOT NULL DEFAULT 'dawg''s dawg')",
        ),
    ),
)
def test_add_column(fresh_db, col_name, col_type, not_null_default, expected_schema):
    fresh_db.create_table("dogs", {"name": str})
    assert "CREATE TABLE [dogs] ( [name] TEXT )" == collapse_whitespace(
        fresh_db["dogs"].schema
    )
    fresh_db["dogs"].add_column(col_name, col_type, not_null_default=not_null_default)
    assert expected_schema == collapse_whitespace(fresh_db["dogs"].schema)


def test_add_foreign_key(fresh_db):
    fresh_db["authors"].insert_all(
        [{"id": 1, "name": "Sally"}, {"id": 2, "name": "Asheesh"}], pk="id"
    )
    fresh_db["books"].insert_all(
        [
            {"title": "Hedgehogs of the world", "author_id": 1},
            {"title": "How to train your wolf", "author_id": 2},
        ]
    )
    assert [] == fresh_db["books"].foreign_keys
    t = fresh_db["books"].add_foreign_key("author_id", "authors", "id")
    # Ensure it returned self:
    assert isinstance(t, Table) and t.name == "books"
    assert [
        ForeignKey(
            table="books", column="author_id", other_table="authors", other_column="id"
        )
    ] == fresh_db["books"].foreign_keys


def test_add_foreign_key_if_column_contains_space(fresh_db):
    fresh_db["authors"].insert_all([{"id": 1, "name": "Sally"}], pk="id")
    fresh_db["books"].insert_all(
        [
            {"title": "Hedgehogs of the world", "author id": 1},
        ]
    )
    fresh_db["books"].add_foreign_key("author id", "authors", "id")
    assert fresh_db["books"].foreign_keys == [
        ForeignKey(
            table="books", column="author id", other_table="authors", other_column="id"
        )
    ]


def test_add_foreign_key_error_if_column_does_not_exist(fresh_db):
    fresh_db["books"].insert(
        {"id": 1, "title": "Hedgehogs of the world", "author_id": 1}
    )
    with pytest.raises(AlterError):
        fresh_db["books"].add_foreign_key("author2_id", "books", "id")


def test_add_foreign_key_error_if_other_table_does_not_exist(fresh_db):
    fresh_db["books"].insert({"title": "Hedgehogs of the world", "author_id": 1})
    with pytest.raises(AlterError):
        fresh_db["books"].add_foreign_key("author_id", "authors", "id")


def test_add_foreign_key_error_if_already_exists(fresh_db):
    fresh_db["books"].insert({"title": "Hedgehogs of the world", "author_id": 1})
    fresh_db["authors"].insert({"id": 1, "name": "Sally"}, pk="id")
    fresh_db["books"].add_foreign_key("author_id", "authors", "id")
    with pytest.raises(AlterError) as ex:
        fresh_db["books"].add_foreign_key("author_id", "authors", "id")
    assert "Foreign key already exists for author_id => authors.id" == ex.value.args[0]


def test_add_foreign_key_no_error_if_exists_and_ignore_true(fresh_db):
    fresh_db["books"].insert({"title": "Hedgehogs of the world", "author_id": 1})
    fresh_db["authors"].insert({"id": 1, "name": "Sally"}, pk="id")
    fresh_db["books"].add_foreign_key("author_id", "authors", "id")
    fresh_db["books"].add_foreign_key("author_id", "authors", "id", ignore=True)


def test_add_foreign_keys(fresh_db):
    fresh_db["authors"].insert_all(
        [{"id": 1, "name": "Sally"}, {"id": 2, "name": "Asheesh"}], pk="id"
    )
    fresh_db["categories"].insert_all([{"id": 1, "name": "Wildlife"}], pk="id")
    fresh_db["books"].insert_all(
        [{"title": "Hedgehogs of the world", "author_id": 1, "category_id": 1}]
    )
    assert [] == fresh_db["books"].foreign_keys
    fresh_db.add_foreign_keys(
        [
            ("books", "author_id", "authors", "id"),
            ("books", "category_id", "categories", "id"),
        ]
    )
    assert [
        ForeignKey(
            table="books", column="author_id", other_table="authors", other_column="id"
        ),
        ForeignKey(
            table="books",
            column="category_id",
            other_table="categories",
            other_column="id",
        ),
    ] == sorted(fresh_db["books"].foreign_keys)


def test_add_column_foreign_key(fresh_db):
    fresh_db.create_table("dogs", {"name": str})
    fresh_db.create_table("breeds", {"name": str})
    fresh_db["dogs"].add_column("breed_id", fk="breeds")
    assert (
        "CREATE TABLE [dogs] ( [name] TEXT , [breed_id] INTEGER, FOREIGN KEY([breed_id]) REFERENCES [breeds]([rowid]) )"
        == collapse_whitespace(fresh_db["dogs"].schema)
    )
    # And again with an explicit primary key column
    fresh_db.create_table("subbreeds", {"name": str, "primkey": str}, pk="primkey")
    fresh_db["dogs"].add_column("subbreed_id", fk="subbreeds")
    assert (
        "CREATE TABLE [dogs] ( [name] TEXT , [breed_id] INTEGER, [subbreed_id] TEXT, "
        "FOREIGN KEY([breed_id]) REFERENCES [breeds]([rowid]), "
        "FOREIGN KEY([subbreed_id]) REFERENCES [subbreeds]([primkey]) )"
        == collapse_whitespace(fresh_db["dogs"].schema)
    )


def test_add_foreign_key_guess_table(fresh_db):
    fresh_db.create_table("dogs", {"name": str})
    fresh_db.create_table("breeds", {"name": str, "id": int}, pk="id")
    fresh_db["dogs"].add_column("breed_id", int)
    fresh_db["dogs"].add_foreign_key("breed_id")
    assert (
        "CREATE TABLE [dogs] ( [name] TEXT , [breed_id] INTEGER, FOREIGN KEY([breed_id]) REFERENCES [breeds]([id]) )"
        == collapse_whitespace(fresh_db["dogs"].schema)
    )


def test_index_foreign_keys(fresh_db):
    test_add_foreign_key_guess_table(fresh_db)
    assert [] == fresh_db["dogs"].indexes
    fresh_db.index_foreign_keys()
    assert [["breed_id"]] == [i.columns for i in fresh_db["dogs"].indexes]
    # Calling it a second time should do nothing
    fresh_db.index_foreign_keys()
    assert [["breed_id"]] == [i.columns for i in fresh_db["dogs"].indexes]


def test_index_foreign_keys_if_index_name_is_already_used(fresh_db):
    # https://github.com/simonw/sqlite-utils/issues/335
    test_add_foreign_key_guess_table(fresh_db)
    # Add index with a name that will conflict with index_foreign_keys()
    fresh_db["dogs"].create_index(["name"], index_name="idx_dogs_breed_id")
    fresh_db.index_foreign_keys()
    assert {(idx.name, tuple(idx.columns)) for idx in fresh_db["dogs"].indexes} == {
        ("idx_dogs_breed_id_2", ("breed_id",)),
        ("idx_dogs_breed_id", ("name",)),
    }


@pytest.mark.parametrize(
    "extra_data,expected_new_columns",
    [
        ({"species": "squirrels"}, [{"name": "species", "type": "TEXT"}]),
        (
            {"species": "squirrels", "hats": 5},
            [{"name": "species", "type": "TEXT"}, {"name": "hats", "type": "INTEGER"}],
        ),
        (
            {"hats": 5, "rating": 3.5},
            [{"name": "hats", "type": "INTEGER"}, {"name": "rating", "type": "FLOAT"}],
        ),
    ],
)
@pytest.mark.parametrize("use_table_factory", [True, False])
def test_insert_row_alter_table(
    fresh_db, extra_data, expected_new_columns, use_table_factory
):
    table = fresh_db["books"]
    table.insert({"title": "Hedgehogs of the world", "author_id": 1})
    assert [
        {"name": "title", "type": "TEXT"},
        {"name": "author_id", "type": "INTEGER"},
    ] == [{"name": col.name, "type": col.type} for col in table.columns]
    record = {"title": "Squirrels of the world", "author_id": 2}
    record.update(extra_data)
    if use_table_factory:
        fresh_db.table("books", alter=True).insert(record)
    else:
        fresh_db["books"].insert(record, alter=True)
    assert [
        {"name": "title", "type": "TEXT"},
        {"name": "author_id", "type": "INTEGER"},
    ] + expected_new_columns == [
        {"name": col.name, "type": col.type} for col in table.columns
    ]


def test_add_missing_columns_case_insensitive(fresh_db):
    table = fresh_db["foo"]
    table.insert({"id": 1, "name": "Cleo"}, pk="id")
    table.add_missing_columns([{"Name": ".", "age": 4}])
    assert (
        table.schema
        == "CREATE TABLE [foo] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT\n, [age] INTEGER)"
    )


@pytest.mark.parametrize("use_table_factory", [True, False])
def test_insert_replace_rows_alter_table(fresh_db, use_table_factory):
    first_row = {"id": 1, "title": "Hedgehogs of the world", "author_id": 1}
    next_rows = [
        {"id": 1, "title": "Hedgehogs of the World", "species": "hedgehogs"},
        {"id": 2, "title": "Squirrels of the World", "num_species": 200},
        {
            "id": 3,
            "title": "Badgers of the World",
            "significant_continents": ["Europe", "North America"],
        },
    ]
    if use_table_factory:
        table = fresh_db.table("books", pk="id", alter=True)
        table.insert(first_row)
        table.insert_all(next_rows, replace=True)
    else:
        table = fresh_db["books"]
        table.insert(first_row, pk="id")
        table.insert_all(next_rows, alter=True, replace=True)
    assert {
        "author_id": int,
        "id": int,
        "num_species": int,
        "significant_continents": str,
        "species": str,
        "title": str,
    } == table.columns_dict
    assert [
        {
            "author_id": None,
            "id": 1,
            "num_species": None,
            "significant_continents": None,
            "species": "hedgehogs",
            "title": "Hedgehogs of the World",
        },
        {
            "author_id": None,
            "id": 2,
            "num_species": 200,
            "significant_continents": None,
            "species": None,
            "title": "Squirrels of the World",
        },
        {
            "author_id": None,
            "id": 3,
            "num_species": None,
            "significant_continents": '["Europe", "North America"]',
            "species": None,
            "title": "Badgers of the World",
        },
    ] == list(table.rows)


def test_insert_all_with_extra_columns_in_later_chunks(fresh_db):
    chunk = [
        {"record": "Record 1"},
        {"record": "Record 2"},
        {"record": "Record 3"},
        {"record": "Record 4", "extra": 1},
    ]
    fresh_db["t"].insert_all(chunk, batch_size=2, alter=True)
    assert list(fresh_db["t"].rows) == [
        {"record": "Record 1", "extra": None},
        {"record": "Record 2", "extra": None},
        {"record": "Record 3", "extra": None},
        {"record": "Record 4", "extra": 1},
    ]


def test_bulk_insert_more_than_999_values(fresh_db):
    "Inserting 100 items with 11 columns should work"
    fresh_db["big"].insert_all(
        (
            {
                "id": i + 1,
                "c2": 2,
                "c3": 3,
                "c4": 4,
                "c5": 5,
                "c6": 6,
                "c7": 7,
                "c8": 8,
                "c9": 9,
                "c10": 10,
                "c11": 11,
            }
            for i in range(100)
        ),
        pk="id",
    )
    assert 100 == fresh_db["big"].count


@pytest.mark.parametrize(
    "num_columns,should_error", ((900, False), (999, False), (1000, True))
)
def test_error_if_more_than_999_columns(fresh_db, num_columns, should_error):
    record = dict([("c{}".format(i), i) for i in range(num_columns)])
    if should_error:
        with pytest.raises(AssertionError):
            fresh_db["big"].insert(record)
    else:
        fresh_db["big"].insert(record)


def test_columns_not_in_first_record_should_not_cause_batch_to_be_too_large(fresh_db):
    # https://github.com/simonw/sqlite-utils/issues/145
    # sqlite on homebrew and Debian/Ubuntu etc. is typically compiled with
    #  SQLITE_MAX_VARIABLE_NUMBER set to 250,000, so we need to exceed this value to
    #  trigger the error on these systems.
    THRESHOLD = 250000
    batch_size = 999
    extra_columns = 1 + (THRESHOLD - 1) // (batch_size - 1)
    records = [
        {"c0": "first record"},  # one column in first record -> batch size = 999
        # fill out the batch with 99 records with enough columns to exceed THRESHOLD
        *[
            dict([("c{}".format(i), j) for i in range(extra_columns)])
            for j in range(batch_size - 1)
        ],
    ]
    try:
        fresh_db["too_many_columns"].insert_all(
            records, alter=True, batch_size=batch_size
        )
    except sqlite3.OperationalError:
        raise


@pytest.mark.parametrize(
    "columns,index_name,expected_index",
    (
        (
            ["is good dog"],
            None,
            Index(
                seq=0,
                name="idx_dogs_is good dog",
                unique=0,
                origin="c",
                partial=0,
                columns=["is good dog"],
            ),
        ),
        (
            ["is good dog", "age"],
            None,
            Index(
                seq=0,
                name="idx_dogs_is good dog_age",
                unique=0,
                origin="c",
                partial=0,
                columns=["is good dog", "age"],
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
    dogs.insert({"name": "Cleo", "twitter": "cleopaws", "age": 3, "is good dog": True})
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
    with pytest.raises(Exception, match="index idx_dogs_name already exists"):
        dogs.create_index(["name"])
    dogs.create_index(["name"], if_not_exists=True)


def test_create_index_desc(fresh_db):
    dogs = fresh_db["dogs"]
    dogs.insert({"name": "Cleo", "twitter": "cleopaws", "age": 3, "is good dog": True})
    assert [] == dogs.indexes
    dogs.create_index([DescIndex("age"), "name"])
    sql = fresh_db.execute(
        "select sql from sqlite_master where name='idx_dogs_age_name'"
    ).fetchone()[0]
    assert sql == (
        "CREATE INDEX [idx_dogs_age_name]\n" "    ON [dogs] ([age] desc, [name])"
    )


def test_create_index_find_unique_name(fresh_db):
    table = fresh_db["t"]
    table.insert({"id": 1})
    table.create_index(["id"])
    # Without find_unique_name should error
    with pytest.raises(OperationalError, match="index idx_t_id already exists"):
        table.create_index(["id"])
    # With find_unique_name=True it should work
    table.create_index(["id"], find_unique_name=True)
    table.create_index(["id"], find_unique_name=True)
    # Should have three now
    index_names = {idx.name for idx in table.indexes}
    assert index_names == {"idx_t_id", "idx_t_id_2", "idx_t_id_3"}


def test_create_index_analyze(fresh_db):
    dogs = fresh_db["dogs"]
    assert "sqlite_stat1" not in fresh_db.table_names()
    dogs.insert({"name": "Cleo", "twitter": "cleopaws"})
    dogs.create_index(["name"], analyze=True)
    assert "sqlite_stat1" in fresh_db.table_names()
    assert list(fresh_db["sqlite_stat1"].rows) == [
        {"tbl": "dogs", "idx": "idx_dogs_name", "stat": "1 1"}
    ]


@pytest.mark.parametrize(
    "data_structure",
    (
        ["list with one item"],
        ["list with", "two items"],
        {"dictionary": "simple"},
        {"dictionary": {"nested": "complex"}},
        collections.OrderedDict(
            [
                ("key1", {"nested": ["cømplex"]}),
                ("key2", "foo"),
            ]
        ),
        [{"list": "of"}, {"two": "dicts"}],
    ),
)
def test_insert_dictionaries_and_lists_as_json(fresh_db, data_structure):
    fresh_db["test"].insert({"id": 1, "data": data_structure}, pk="id")
    row = fresh_db.execute("select id, data from test").fetchone()
    assert row[0] == 1
    assert data_structure == json.loads(row[1])


def test_insert_list_nested_unicode(fresh_db):
    fresh_db["test"].insert(
        {"id": 1, "data": {"key1": {"nested": ["cømplex"]}}}, pk="id"
    )
    row = fresh_db.execute("select id, data from test").fetchone()
    assert row[1] == '{"key1": {"nested": ["cømplex"]}}'


def test_insert_uuid(fresh_db):
    uuid4 = uuid.uuid4()
    fresh_db["test"].insert({"uuid": uuid4})
    row = list(fresh_db["test"].rows)[0]
    assert {"uuid"} == row.keys()
    assert isinstance(row["uuid"], str)
    assert row["uuid"] == str(uuid4)


def test_insert_memoryview(fresh_db):
    fresh_db["test"].insert({"data": memoryview(b"hello")})
    row = list(fresh_db["test"].rows)[0]
    assert {"data"} == row.keys()
    assert isinstance(row["data"], bytes)
    assert row["data"] == b"hello"


def test_insert_thousands_using_generator(fresh_db):
    fresh_db["test"].insert_all(
        {"i": i, "word": "word_{}".format(i)} for i in range(10000)
    )
    assert [{"name": "i", "type": "INTEGER"}, {"name": "word", "type": "TEXT"}] == [
        {"name": col.name, "type": col.type} for col in fresh_db["test"].columns
    ]
    assert 10000 == fresh_db["test"].count


def test_insert_thousands_raises_exception_with_extra_columns_after_first_100(fresh_db):
    # https://github.com/simonw/sqlite-utils/issues/139
    with pytest.raises(Exception, match="table test has no column named extra"):
        fresh_db["test"].insert_all(
            [{"i": i, "word": "word_{}".format(i)} for i in range(100)]
            + [{"i": 101, "extra": "This extra column should cause an exception"}],
        )


def test_insert_thousands_adds_extra_columns_after_first_100_with_alter(fresh_db):
    # https://github.com/simonw/sqlite-utils/issues/139
    fresh_db["test"].insert_all(
        [{"i": i, "word": "word_{}".format(i)} for i in range(100)]
        + [{"i": 101, "extra": "Should trigger ALTER"}],
        alter=True,
    )
    rows = list(fresh_db.query("select * from test where i = 101"))
    assert rows == [{"i": 101, "word": None, "extra": "Should trigger ALTER"}]


def test_insert_ignore(fresh_db):
    fresh_db["test"].insert({"id": 1, "bar": 2}, pk="id")
    # Should raise an error if we try this again
    with pytest.raises(Exception, match="UNIQUE constraint failed"):
        fresh_db["test"].insert({"id": 1, "bar": 2}, pk="id")
    # Using ignore=True should cause our insert to be silently ignored
    fresh_db["test"].insert({"id": 1, "bar": 3}, pk="id", ignore=True)
    # Only one row, and it should be bar=2, not bar=3
    rows = list(fresh_db.query("select * from test"))
    assert rows == [{"id": 1, "bar": 2}]


def test_insert_hash_id(fresh_db):
    dogs = fresh_db["dogs"]
    id = dogs.insert({"name": "Cleo", "twitter": "cleopaws"}, hash_id="id").last_pk
    assert "f501265970505d9825d8d9f590bfab3519fb20b1" == id
    assert 1 == dogs.count
    # Insert replacing a second time should not create a new row
    id2 = dogs.insert(
        {"name": "Cleo", "twitter": "cleopaws"}, hash_id="id", replace=True
    ).last_pk
    assert "f501265970505d9825d8d9f590bfab3519fb20b1" == id2
    assert 1 == dogs.count


@pytest.mark.parametrize("use_table_factory", [True, False])
def test_insert_hash_id_columns(fresh_db, use_table_factory):
    if use_table_factory:
        dogs = fresh_db.table("dogs", hash_id_columns=("name", "twitter"))
        insert_kwargs = {}
    else:
        dogs = fresh_db["dogs"]
        insert_kwargs = dict(hash_id_columns=("name", "twitter"))

    id = dogs.insert(
        {"name": "Cleo", "twitter": "cleopaws", "age": 5},
        **insert_kwargs,
    ).last_pk
    expected_hash = hash_record({"name": "Cleo", "twitter": "cleopaws"})
    assert id == expected_hash
    assert dogs.count == 1
    # Insert replacing a second time should not create a new row
    id2 = dogs.insert(
        {"name": "Cleo", "twitter": "cleopaws", "age": 6},
        **insert_kwargs,
        replace=True,
    ).last_pk
    assert id2 == expected_hash
    assert dogs.count == 1


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


def test_cannot_provide_both_filename_and_memory():
    with pytest.raises(
        AssertionError, match="Either specify a filename_or_conn or pass memory=True"
    ):
        Database("/tmp/foo.db", memory=True)


def test_creates_id_column(fresh_db):
    last_pk = fresh_db.table("cats", pk="id").insert({"name": "barry"}).last_pk
    assert [{"name": "barry", "id": last_pk}] == list(fresh_db["cats"].rows)


def test_drop(fresh_db):
    fresh_db["t"].insert({"foo": 1})
    assert ["t"] == fresh_db.table_names()
    assert None is fresh_db["t"].drop()
    assert [] == fresh_db.table_names()


def test_drop_view(fresh_db):
    fresh_db.create_view("foo_view", "select 1")
    assert ["foo_view"] == fresh_db.view_names()
    assert None is fresh_db["foo_view"].drop()
    assert [] == fresh_db.view_names()


def test_drop_ignore(fresh_db):
    with pytest.raises(sqlite3.OperationalError):
        fresh_db["does_not_exist"].drop()
    fresh_db["does_not_exist"].drop(ignore=True)
    # Testing view is harder, we need to create it in order
    # to get a View object, then drop it twice
    fresh_db.create_view("foo_view", "select 1")
    view = fresh_db["foo_view"]
    assert isinstance(view, View)
    view.drop()
    with pytest.raises(sqlite3.OperationalError):
        view.drop()
    view.drop(ignore=True)


def test_insert_all_empty_list(fresh_db):
    fresh_db["t"].insert({"foo": 1})
    assert 1 == fresh_db["t"].count
    fresh_db["t"].insert_all([])
    assert 1 == fresh_db["t"].count
    fresh_db["t"].insert_all([], replace=True)
    assert 1 == fresh_db["t"].count


def test_insert_all_single_column(fresh_db):
    table = fresh_db["table"]
    table.insert_all([{"name": "Cleo"}], pk="name")
    assert [{"name": "Cleo"}] == list(table.rows)
    assert table.pks == ["name"]


@pytest.mark.parametrize("method_name", ("insert_all", "upsert_all"))
def test_insert_all_analyze(fresh_db, method_name):
    table = fresh_db["table"]
    table.insert_all([{"id": 1, "name": "Cleo"}], pk="id")
    assert "sqlite_stat1" not in fresh_db.table_names()
    table.create_index(["name"], analyze=True)
    assert list(fresh_db["sqlite_stat1"].rows) == [
        {"tbl": "table", "idx": "idx_table_name", "stat": "1 1"}
    ]
    method = getattr(table, method_name)
    method([{"id": 2, "name": "Suna"}], pk="id", analyze=True)
    assert "sqlite_stat1" in fresh_db.table_names()
    assert list(fresh_db["sqlite_stat1"].rows) == [
        {"tbl": "table", "idx": "idx_table_name", "stat": "2 1"}
    ]


def test_create_with_a_null_column(fresh_db):
    record = {"name": "Name", "description": None}
    fresh_db["t"].insert(record)
    assert [record] == list(fresh_db["t"].rows)


def test_create_with_nested_bytes(fresh_db):
    record = {"id": 1, "data": {"foo": b"bytes"}}
    fresh_db["t"].insert(record)
    assert [{"id": 1, "data": '{"foo": "b\'bytes\'"}'}] == list(fresh_db["t"].rows)


@pytest.mark.parametrize(
    "input,expected", [("hello", "'hello'"), ("hello'there'", "'hello''there'''")]
)
def test_quote(fresh_db, input, expected):
    assert fresh_db.quote(input) == expected


@pytest.mark.parametrize(
    "columns,expected_sql_middle",
    (
        (
            {"id": int},
            "[id] INTEGER",
        ),
        (
            {"col": dict},
            "[col] TEXT",
        ),
        (
            {"col": tuple},
            "[col] TEXT",
        ),
        (
            {"col": list},
            "[col] TEXT",
        ),
    ),
)
def test_create_table_sql(fresh_db, columns, expected_sql_middle):
    sql = fresh_db.create_table_sql("t", columns)
    middle = sql.split("(")[1].split(")")[0].strip()
    assert middle == expected_sql_middle


def test_create(fresh_db):
    fresh_db["t"].create(
        {
            "id": int,
            "text": str,
            "float": float,
            "integer": int,
            "bytes": bytes,
        },
        pk="id",
        column_order=("id", "float"),
        not_null=("float", "integer"),
        defaults={"integer": 0},
    )
    assert fresh_db["t"].schema == (
        "CREATE TABLE [t] (\n"
        "   [id] INTEGER PRIMARY KEY,\n"
        "   [float] FLOAT NOT NULL,\n"
        "   [text] TEXT,\n"
        "   [integer] INTEGER NOT NULL DEFAULT 0,\n"
        "   [bytes] BLOB\n"
        ")"
    )


def test_create_if_not_exists(fresh_db):
    fresh_db["t"].create({"id": int})
    # This should error
    with pytest.raises(sqlite3.OperationalError):
        fresh_db["t"].create({"id": int})
    # This should not
    fresh_db["t"].create({"id": int}, if_not_exists=True)


def test_create_if_no_columns(fresh_db):
    with pytest.raises(AssertionError) as error:
        fresh_db["t"].create({})
    assert error.value.args[0] == "Tables must have at least one column"


def test_create_ignore(fresh_db):
    fresh_db["t"].create({"id": int})
    # This should error
    with pytest.raises(sqlite3.OperationalError):
        fresh_db["t"].create({"id": int})
    # This should not
    fresh_db["t"].create({"id": int}, ignore=True)


def test_create_replace(fresh_db):
    fresh_db["t"].create({"id": int})
    # This should error
    with pytest.raises(sqlite3.OperationalError):
        fresh_db["t"].create({"id": int})
    # This should not
    fresh_db["t"].create({"name": str}, replace=True)
    assert fresh_db["t"].schema == ("CREATE TABLE [t] (\n" "   [name] TEXT\n" ")")


@pytest.mark.parametrize(
    "cols,kwargs,expected_schema,should_transform",
    (
        # Change nothing
        (
            {"id": int, "name": str},
            {"pk": "id"},
            "CREATE TABLE [demo] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT\n)",
            False,
        ),
        # Drop name column, remove primary key
        ({"id": int}, {}, 'CREATE TABLE "demo" (\n   [id] INTEGER\n)', True),
        # Add a new column
        (
            {"id": int, "name": str, "age": int},
            {"pk": "id"},
            'CREATE TABLE "demo" (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT,\n   [age] INTEGER\n)',
            True,
        ),
        # Change a column type
        (
            {"id": int, "name": bytes},
            {"pk": "id"},
            'CREATE TABLE "demo" (\n   [id] INTEGER PRIMARY KEY,\n   [name] BLOB\n)',
            True,
        ),
        # Change the primary key
        (
            {"id": int, "name": str},
            {"pk": "name"},
            'CREATE TABLE "demo" (\n   [id] INTEGER,\n   [name] TEXT PRIMARY KEY\n)',
            True,
        ),
        # Change in column order
        (
            {"id": int, "name": str},
            {"pk": "id", "column_order": ["name"]},
            'CREATE TABLE "demo" (\n   [name] TEXT,\n   [id] INTEGER PRIMARY KEY\n)',
            True,
        ),
        # Same column order is ignored
        (
            {"id": int, "name": str},
            {"pk": "id", "column_order": ["id", "name"]},
            "CREATE TABLE [demo] (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT\n)",
            False,
        ),
        # Change not null
        (
            {"id": int, "name": str},
            {"pk": "id", "not_null": {"name"}},
            'CREATE TABLE "demo" (\n   [id] INTEGER PRIMARY KEY,\n   [name] TEXT NOT NULL\n)',
            True,
        ),
        # Change default values
        (
            {"id": int, "name": str},
            {"pk": "id", "defaults": {"id": 0, "name": "Bob"}},
            "CREATE TABLE \"demo\" (\n   [id] INTEGER PRIMARY KEY DEFAULT 0,\n   [name] TEXT DEFAULT 'Bob'\n)",
            True,
        ),
    ),
)
def test_create_transform(fresh_db, cols, kwargs, expected_schema, should_transform):
    fresh_db.create_table("demo", {"id": int, "name": str}, pk="id")
    fresh_db["demo"].insert({"id": 1, "name": "Cleo"})
    traces = []
    with fresh_db.tracer(lambda sql, parameters: traces.append((sql, parameters))):
        fresh_db["demo"].create(cols, **kwargs, transform=True)
    at_least_one_create_table = any(sql.startswith("CREATE TABLE") for sql, _ in traces)
    assert should_transform == at_least_one_create_table
    new_schema = fresh_db["demo"].schema
    assert new_schema == expected_schema, repr(new_schema)
    assert fresh_db["demo"].count == 1


def test_rename_table(fresh_db):
    fresh_db["t"].insert({"foo": "bar"})
    assert ["t"] == fresh_db.table_names()
    fresh_db.rename_table("t", "renamed")
    assert ["renamed"] == fresh_db.table_names()
    assert [{"foo": "bar"}] == list(fresh_db["renamed"].rows)
    # Should error if table does not exist:
    with pytest.raises(sqlite3.OperationalError):
        fresh_db.rename_table("does_not_exist", "renamed")
