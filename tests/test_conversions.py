import pytest


def test_insert_conversion(fresh_db):
    table = fresh_db["table"]
    table.insert({"foo": "bar"}, conversions={"foo": "upper(?)"})
    assert [{"foo": "BAR"}] == list(table.rows)


def test_insert_all_conversion(fresh_db):
    table = fresh_db["table"]
    table.insert_all([{"foo": "bar"}], conversions={"foo": "upper(?)"})
    assert [{"foo": "BAR"}] == list(table.rows)


def test_upsert_conversion(fresh_db):
    table = fresh_db["table"]
    table.upsert({"id": 1, "foo": "bar"}, pk="id", conversions={"foo": "upper(?)"})
    assert [{"id": 1, "foo": "BAR"}] == list(table.rows)
    table.upsert(
        {"id": 1, "bar": "baz"}, pk="id", conversions={"bar": "upper(?)"}, alter=True
    )
    assert [{"id": 1, "foo": "BAR", "bar": "BAZ"}] == list(table.rows)


def test_upsert_all_conversion(fresh_db):
    table = fresh_db["table"]
    table.upsert_all(
        [{"id": 1, "foo": "bar"}], pk="id", conversions={"foo": "upper(?)"}
    )
    assert [{"id": 1, "foo": "BAR"}] == list(table.rows)


def test_update_conversion(fresh_db):
    table = fresh_db["table"]
    table.insert({"id": 5, "foo": "bar"}, pk="id")
    table.update(5, {"foo": "baz"}, conversions={"foo": "upper(?)"})
    assert [{"id": 5, "foo": "BAZ"}] == list(table.rows)


def test_table_constructor_conversion(fresh_db):
    table = fresh_db.table("table", conversions={"bar": "upper(?)"})
    table.insert({"bar": "baz"})
    assert [{"bar": "BAZ"}] == list(table.rows)
