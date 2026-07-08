"""
SQLite treats column names as case-insensitive. These tests exercise the
places where sqlite-utils performs Python-side lookups of column names
provided by the caller, which should match the schema case-insensitively.

https://github.com/simonw/sqlite-utils/issues/760
"""

import pytest

from sqlite_utils import Database
from sqlite_utils.db import ForeignKey


def test_insert_populates_last_pk_case_insensitively(fresh_db):
    books = fresh_db["books"]
    books.create({"Id": int, "Title": str}, pk="Id")
    books.insert({"Id": 1, "Title": "One"}, pk="id")
    assert books.last_pk == 1


def test_insert_populates_last_pk_compound_pk_case_insensitively(fresh_db):
    books = fresh_db["books"]
    books.create({"Author": str, "Position": int, "Title": str})
    books.insert(
        {"Author": "Sue", "Position": 1, "Title": "One"}, pk=("author", "position")
    )
    assert books.last_pk == ("Sue", 1)


@pytest.mark.parametrize("use_old_upsert", (False, True))
def test_upsert_pk_case_differs_from_schema(use_old_upsert):
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    books = db["books"]
    books.create({"Id": int, "Title": str}, pk="Id")
    books.insert({"Id": 1, "Title": "One"})
    books.upsert({"id": 1, "title": "Won"}, pk="id")
    assert list(books.rows) == [{"Id": 1, "Title": "Won"}]
    assert books.last_pk == 1


@pytest.mark.parametrize("use_old_upsert", (False, True))
def test_upsert_record_key_case_differs_from_pk(use_old_upsert):
    # all_columns comes from the record keys, pk= from the caller
    db = Database(memory=True, use_old_upsert=use_old_upsert)
    books = db["books"]
    books.create({"Id": int, "Title": str}, pk="Id")
    books.upsert({"ID": 1, "Title": "One"}, pk="id")
    assert list(books.rows) == [{"Id": 1, "Title": "One"}]
    assert books.last_pk == 1


def test_upsert_inferred_pk_case_differs_from_record_keys(fresh_db):
    # pk is inferred from the existing schema as "Id", records use "id"
    books = fresh_db["books"]
    books.create({"Id": int, "Title": str}, pk="Id")
    books.upsert({"id": 1, "title": "One"})
    assert list(books.rows) == [{"Id": 1, "Title": "One"}]
    assert books.last_pk == 1


def test_upsert_list_mode_pk_case_insensitive(fresh_db):
    books = fresh_db["books"]
    books.create({"Id": int, "Title": str}, pk="Id")
    books.upsert_all([["id", "title"], [1, "One"]], pk="Id")
    assert list(books.rows) == [{"Id": 1, "Title": "One"}]
    assert books.last_pk == 1


def test_lookup_pk_case_insensitive(fresh_db):
    fresh_db["species"].create({"ID": int, "Name": str}, pk="ID")
    fresh_db["species"].insert({"ID": 5, "Name": "Palm"})
    fresh_db["species"].create_index(["Name"], unique=True)
    assert fresh_db["species"].lookup({"Name": "Palm"}, pk="id") == 5


def test_lookup_does_not_create_redundant_index(fresh_db):
    fresh_db["species"].create({"id": int, "Name": str}, pk="id")
    fresh_db["species"].create_index(["Name"], unique=True)
    fresh_db["species"].lookup({"name": "Palm"})
    assert len(fresh_db["species"].indexes) == 1


def test_create_table_transform_same_columns_different_case(fresh_db):
    fresh_db["t"].create({"Name": str, "Age": int})
    fresh_db["t"].insert({"Name": "Cleo", "Age": 5})
    fresh_db.create_table("t", {"name": str, "age": int}, transform=True)
    # Schema casing is preserved - SQLite considers these the same columns
    assert fresh_db["t"].columns_dict == {"Name": str, "Age": int}
    assert list(fresh_db["t"].rows) == [{"Name": "Cleo", "Age": 5}]


def test_create_table_transform_case_insensitive_with_changes(fresh_db):
    fresh_db["t"].create({"Name": str, "Age": int})
    fresh_db.create_table("t", {"name": str, "age": str, "size": int}, transform=True)
    # age changed type, size added, Name untouched
    assert fresh_db["t"].columns_dict == {"Name": str, "Age": str, "size": int}


def test_transform_types_case_insensitive(fresh_db):
    fresh_db["t"].create({"Name": str, "Age": str})
    fresh_db["t"].transform(types={"age": int})
    assert fresh_db["t"].columns_dict == {"Name": str, "Age": int}


def test_transform_rename_case_insensitive(fresh_db):
    fresh_db["t"].create({"Name": str})
    fresh_db["t"].transform(rename={"name": "title"})
    assert fresh_db["t"].columns_dict == {"title": str}


def test_transform_drop_case_insensitive(fresh_db):
    fresh_db["t"].create({"Name": str, "Age": int})
    fresh_db["t"].transform(drop=["name"])
    assert fresh_db["t"].columns_dict == {"Age": int}


def test_transform_not_null_and_defaults_case_insensitive(fresh_db):
    fresh_db["t"].create({"Name": str, "Age": int})
    fresh_db["t"].transform(not_null={"name"}, defaults={"age": 3})
    columns = {c.name: c for c in fresh_db["t"].columns}
    assert columns["Name"].notnull
    assert fresh_db["t"].default_values == {"Age": 3}


def test_transform_pk_case_insensitive(fresh_db):
    fresh_db["t"].create({"Id": int, "Name": str})
    fresh_db["t"].transform(pk="id")
    assert fresh_db["t"].pks == ["Id"]
    assert fresh_db["t"].columns_dict == {"Id": int, "Name": str}


def test_transform_drop_foreign_keys_case_insensitive(fresh_db):
    fresh_db["parent"].create({"Id": int}, pk="Id")
    fresh_db["child"].create(
        {"id": int, "Parent_ID": int},
        pk="id",
        foreign_keys=[("Parent_ID", "parent", "Id")],
    )
    fresh_db["child"].transform(drop_foreign_keys=["parent_id"])
    assert fresh_db["child"].foreign_keys == []


def test_add_foreign_key_case_insensitive(fresh_db):
    fresh_db["parent"].create({"Id": int}, pk="Id")
    fresh_db["child"].create({"id": int, "Parent_ID": int}, pk="id")
    fresh_db["child"].add_foreign_key("parent_id", "parent", "id")
    fks = fresh_db["child"].foreign_keys
    assert len(fks) == 1
    # The foreign key should use the schema casing of the columns
    assert fks[0].column == "Parent_ID"
    assert fks[0].other_column == "Id"


def test_add_foreign_keys_case_insensitive(fresh_db):
    fresh_db["parent"].create({"Id": int}, pk="Id")
    fresh_db["child"].create({"id": int, "Parent_ID": int}, pk="id")
    fresh_db.add_foreign_keys([("child", "parent_id", "parent", "id")])
    fks = fresh_db["child"].foreign_keys
    assert len(fks) == 1
    assert fks[0].column == "Parent_ID"
    assert fks[0].other_column == "Id"


def test_add_foreign_key_detects_existing_case_insensitively(fresh_db):
    fresh_db["parent"].create({"Id": int}, pk="Id")
    fresh_db["child"].create(
        {"id": int, "Parent_ID": int},
        pk="id",
        foreign_keys=[("Parent_ID", "parent", "Id")],
    )
    # ignore=True should treat this as already existing, not add a duplicate
    fresh_db["child"].add_foreign_key("parent_id", "parent", "id", ignore=True)
    assert len(fresh_db["child"].foreign_keys) == 1


def test_add_column_fk_col_case_insensitive(fresh_db):
    fresh_db["parent"].create({"Id": int}, pk="Id")
    fresh_db["child"].create({"id": int}, pk="id")
    fresh_db["child"].add_column("parent_id", int, fk="parent", fk_col="id")
    fks = fresh_db["child"].foreign_keys
    assert len(fks) == 1
    assert fks[0].other_column == "Id"


def test_extract_case_insensitive(fresh_db):
    fresh_db["trees"].insert({"id": 1, "Species": "Palm"}, pk="id")
    fresh_db["trees"].extract("species")
    assert fresh_db["trees"].columns_dict == {"id": int, "Species_id": int}
    assert list(fresh_db["Species"].rows) == [{"id": 1, "Species": "Palm"}]


def test_convert_multi_case_insensitive(fresh_db):
    fresh_db["t"].insert({"id": 1, "Name": "Cleo"}, pk="id")
    fresh_db["t"].convert("name", lambda v: {"upper": v.upper()}, multi=True)
    assert list(fresh_db["t"].rows) == [{"id": 1, "Name": "Cleo", "upper": "CLEO"}]


def test_convert_output_case_insensitive(fresh_db):
    fresh_db["t"].insert({"id": 1, "Name": "Cleo", "Upper": None}, pk="id")
    fresh_db["t"].convert("name", lambda v: v.upper(), output="upper")
    assert list(fresh_db["t"].rows) == [{"id": 1, "Name": "Cleo", "Upper": "CLEO"}]


def test_create_table_sql_pk_case_insensitive(fresh_db):
    fresh_db["t"].create({"Id": int, "Name": str}, pk="id")
    # Should not have created an extra lowercase "id" column
    assert fresh_db["t"].columns_dict == {"Id": int, "Name": str}
    assert fresh_db["t"].pks == ["Id"]


def test_create_table_not_null_and_defaults_case_insensitive(fresh_db):
    fresh_db["t"].create(
        {"Name": str, "Age": int}, not_null={"name"}, defaults={"age": 1}
    )
    columns = {c.name: c for c in fresh_db["t"].columns}
    assert columns["Name"].notnull
    assert fresh_db["t"].default_values == {"Age": 1}


def test_create_table_foreign_keys_case_insensitive(fresh_db):
    fresh_db["parent"].create({"Id": int}, pk="Id")
    fresh_db["child"].create(
        {"id": int, "Parent_ID": int},
        pk="id",
        foreign_keys=[("parent_id", "parent", "id")],
    )
    fks = fresh_db["child"].foreign_keys
    assert fks == [
        ForeignKey(
            table="child", column="Parent_ID", other_table="parent", other_column="Id"
        )
    ]
